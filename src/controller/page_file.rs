use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, io};

use foldhash::HashMapExt;
use parking_lot::RwLock;

use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::PageFileId;
use crate::page_data::{
    CreatePageFileError,
    MAX_NUM_PAGES,
    OpenPageFileError,
    PageFile,
};
use crate::page_file_allocator::{PageFileAllocator, WriteAllocTx};

const PREP_ALLOC_TIMEOUT: Duration = Duration::from_secs(5);

/// The page file controller manages creation and cleanup of
/// the page data files.
pub struct PageFileController {
    ctx: Arc<ctx::FileContext>,
    allocator: PageFileAllocator,
    page_files: RwLock<foldhash::HashMap<PageFileId, PageFile>>,
    next_page_file_id: tokio::sync::Mutex<PageFileId>,
}

impl PageFileController {
    /// Opens the [PageFileController] using the given context and metadata controller.
    ///
    /// This will open all existing page files and load their existing allocation state
    /// from the metadata controller into the disk allocator.
    pub async fn open(
        ctx: Arc<ctx::FileContext>,
        metadata_controller: &super::metadata::MetadataController,
    ) -> Result<Self, OpenPageFileError> {
        let directory = ctx.directory();
        let file_ids = directory.list_dir(FileGroup::Pages).await;

        let mut page_files = foldhash::HashMap::new();
        let mut max_page_file_id = PageFileId(0);
        for file_id in file_ids {
            let file = directory.get_rw_file(FileGroup::Pages, file_id).await?;
            let page_file = PageFile::open(ctx.clone(), file).await?;
            let page_file_id = page_file.id();
            max_page_file_id = cmp::max(max_page_file_id, page_file_id);
            page_files.insert(page_file_id, page_file);
        }

        let allocator = metadata_controller.create_page_file_allocator();

        Ok(Self {
            ctx,
            allocator,
            page_files: RwLock::new(page_files),
            next_page_file_id: tokio::sync::Mutex::new(PageFileId(
                max_page_file_id.0 + 1,
            )),
        })
    }

    /// Prepare an allocation returning the [WriteAllocTx] with the reserved pages.
    ///
    /// Creates a new page file if one does not have capacity.
    pub async fn prep_alloc(
        &self,
        num_pages: u32,
    ) -> Result<WriteAllocTx<'_>, CreatePageFileError> {
        assert!(
            num_pages <= MAX_NUM_PAGES as u32,
            "number of pages exceeds maximum page file size"
        );

        if let Some(alloc) = self.allocator.get_alloc_tx(num_pages) {
            return Ok(alloc);
        }

        let start = Instant::now();
        loop {
            if start.elapsed() >= PREP_ALLOC_TIMEOUT {
                return Err(CreatePageFileError::IO(io::Error::from(
                    io::ErrorKind::TimedOut,
                )));
            }

            self.create_new_page_file().await?;

            if let Some(alloc) = self.allocator.get_alloc_tx(num_pages) {
                return Ok(alloc);
            }
        }
    }

    /// Creates a new empty [PageFile] with a new [PageFileId] that should be monotonic
    /// to the previously generated ID.
    ///
    /// Only one page file is allowed to be created at a time, internally concurrent writers
    /// that are trying to create a file will wait for the inflight creation to finish and
    /// then return `Ok` without creating any additional file.
    async fn create_new_page_file(&self) -> Result<(), CreatePageFileError> {
        let mut creation_guard = match self.next_page_file_id.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // We wait on the lock still even though we know another writer is creating
                // a new file currently, we do this to prevent a thundering herd of retries.
                let _guard = self.next_page_file_id.lock().await;
                return Ok(());
            },
        };

        let next_page_file_id = *creation_guard;
        *creation_guard = PageFileId(next_page_file_id.0 + 1);

        let directory = self.ctx.directory();
        let file_id = directory.create_new_file(FileGroup::Pages).await?;

        let file = directory.get_rw_file(FileGroup::Pages, file_id).await?;

        let page_file =
            PageFile::create(self.ctx.clone(), file, next_page_file_id).await?;

        {
            let mut page_files = self.page_files.write();
            page_files.insert(next_page_file_id, page_file);
        }

        drop(creation_guard);

        Ok(())
    }
}
