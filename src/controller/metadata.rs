use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io, mem};

use parking_lot::{Mutex, RwLock};

use crate::checkpoint::{ReadCheckpointError, WriteCheckpointError};
use crate::directory::{FileGroup, FileId};
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::{
    DISK_PAGE_SIZE,
    MAX_NUM_PAGES,
    NUM_BLOCKS_PER_FILE,
    NUM_PAGES_PER_BLOCK,
};
use crate::{ctx, page_op_log};

type ConcurrentHashMap<K, V> = papaya::HashMap<K, V, foldhash::fast::RandomState>;

#[derive(Debug, thiserror::Error)]
/// An error preventing the [MetadataController] from opening
/// and recovering the previously persisted state.
pub enum OpenMetadataControllerError {
    #[error(transparent)]
    /// The controller could not recover page data from existing checkpoints.
    CheckpointError(#[from] ReadCheckpointError),
    #[error(transparent)]
    /// The controller could not recovery metadata updates from replaying
    /// the log.
    WalRecoveryError(#[from] page_op_log::LogOpenReadError),
}

/// The metadata controller handles the global page table state
/// and checkpointing the metadata tied to each page of data.
pub struct MetadataController {
    ctx: Arc<ctx::FileContext>,
    lookup_table: ConcurrentHashMap<PageGroupId, LookupEntry>,
    page_tables: ConcurrentHashMap<PageFileId, PageTable>,
    active_checkpoint_files: Mutex<BTreeMap<PageFileId, FileId>>,
    files_to_cleanup: Mutex<Vec<FileId>>,
}

impl MetadataController {
    /// Opens a new [MetadataController] and re-populates data using the page table
    /// checkpoints and replays any WAL file changes.
    pub async fn open(
        ctx: Arc<ctx::FileContext>,
    ) -> Result<Self, OpenMetadataControllerError> {
        let page_tables = super::checkpoint::read_checkpoints(ctx.clone()).await?;

        let controller = Self::empty(ctx.clone());
        for (page_file_id, page_table) in page_tables {
            controller.insert_page_table(page_file_id, page_table);
        }

        Ok(controller)
    }

    /// Create a new empty [MetadataController].
    pub fn empty(ctx: Arc<ctx::FileContext>) -> Self {
        Self {
            ctx,
            lookup_table: ConcurrentHashMap::with_hasher(
                foldhash::fast::RandomState::default(),
            ),
            page_tables: ConcurrentHashMap::with_hasher(
                foldhash::fast::RandomState::default(),
            ),
            active_checkpoint_files: Mutex::new(BTreeMap::new()),
            files_to_cleanup: Mutex::new(Vec::new()),
        }
    }

    /// Insert a new page table with some pre-existing page table.
    pub fn insert_page_table(&self, page_file_id: PageFileId, page_table: PageTable) {
        let tables = self.page_tables.pin();
        if tables.contains_key(&page_file_id) {
            panic!("page table already exists");
        }
        tables.insert(page_file_id, page_table);
    }

    /// Creates a new blank page table for the given page file.
    pub fn create_blank_page_table(&self, page_file_id: PageFileId) {
        self.insert_page_table(page_file_id, PageTable::default())
    }

    /// Returns whether a given page file & its page table
    /// exists within the controller.
    pub fn contains_page_table(&self, page_file_id: PageFileId) -> bool {
        let tables = self.page_tables.pin();
        tables.contains_key(&page_file_id)
    }

    /// Find the first page of a page group and return the page table
    /// it is stored at and the page ID of the first page.
    ///
    /// Returns `None` if the group does not exist.
    pub fn find_first_page(&self, group: PageGroupId) -> Option<LookupEntry> {
        self.lookup_table.pin().get(&group).copied()
    }

    /// Insert the given page group into the controller and associate it with
    /// the given page table (via the page file ID) and the first page containing
    /// the start of the group data.
    pub fn insert_page_group(&self, group: PageGroupId, entry: LookupEntry) {
        if !self.contains_page_table(entry.page_file_id) {
            panic!(
                "page table does not exist for page file: {:?}",
                entry.page_file_id
            );
        }

        let lookup = self.lookup_table.pin();
        lookup.insert(group, entry);
    }

    /// Collects the pages of a given page group within a given page file.
    ///
    /// The `start_page` is the start of the page group data and holds the link
    /// to the next allocated page.
    ///
    /// The `data_range` is the range of bytes that need to be read from the group,
    /// the controller put all pages required to complete the read in the `results`
    /// output vector.
    ///
    /// Panics if the provided [PageFileId] does not exist.
    pub fn collect_pages(
        &self,
        page_file_id: PageFileId,
        start_page: PageId,
        data_range: Range<usize>,
        results: &mut Vec<PageMetadata>,
    ) {
        let tables = self.page_tables.pin();
        let page_table = tables
            .get(&page_file_id)
            .expect("page file ID should exist as provided by user");

        page_table.collect_pages(start_page, data_range, results);
    }

    /// Write [PageMetadata] entries to a target page file.
    pub fn write_pages(&self, page_file_id: PageFileId, pages: &[PageMetadata]) {
        let tables = self.page_tables.pin();
        let page_table = tables
            .get(&page_file_id)
            .expect("page file ID should exist as provided by user");

        page_table.write_pages(pages);
    }

    #[tracing::instrument(skip(self))]
    /// Checkpoint the current memory state.
    ///
    /// Any page table that has changed since the last checkpoint and creates a new checkpoint file.
    ///
    /// This operation is technically incremental, if a page table has not changed from the last
    /// checkpoint then a new checkpoint file is not created.
    pub async fn checkpoint(&self) -> Result<usize, WriteCheckpointError> {
        let mut num_checkpointed_files = 0;
        for (page_file_id, page_table) in self.page_tables.pin().iter() {
            if !page_table.has_changed() {
                continue;
            }

            let file_id = super::checkpoint::checkpoint_page_table(
                self.ctx.clone(),
                *page_file_id,
                page_table,
            )
            .await?;
            num_checkpointed_files += 1;

            let maybe_old_file_id = {
                let mut checkpoints = self.active_checkpoint_files.lock();
                checkpoints.insert(*page_file_id, file_id)
            };

            if let Some(old_file_id) = maybe_old_file_id {
                self.files_to_cleanup.lock().push(old_file_id);
            }
        }

        self.garbage_collect_checkpoints().await?;

        Ok(num_checkpointed_files)
    }

    /// Returns the number of files to be cleaned up.
    pub fn num_files_to_cleanup(&self) -> usize {
        self.files_to_cleanup.lock().len()
    }

    #[tracing::instrument(skip(self))]
    /// Cleanup any checkpoint files that are not outdated and not needed.
    pub async fn garbage_collect_checkpoints(&self) -> io::Result<()> {
        tracing::info!("cleaning up unused checkpoint files");

        #[cfg(test)]
        fail::fail_point!("metadata::garbage_collect_checkpoints", |_| Err(
            io::Error::other("garbage_collect_checkpoints fail point error")
        ));

        while let Some(file_id) = { self.files_to_cleanup.lock().pop() } {
            let result = self
                .ctx
                .directory()
                .remove_file(FileGroup::Metadata, file_id)
                .await;

            if result.is_err() {
                self.files_to_cleanup.lock().push(file_id);
            }
            result?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
/// A lookup entry contains the location of the first page that a page group
/// contains and the revision of the page group.
pub struct LookupEntry {
    pub page_file_id: PageFileId,
    pub first_page_id: PageId,
    pub revision: u32,
}

type GuardedPages = RwLock<Box<[PageMetadata; NUM_PAGES_PER_BLOCK]>>;

/// A [PageTable] holds the individual [PageMetadata] entries for each page file,
/// tying the data stored in the data file with its metadata.
pub struct PageTable {
    /// A set of shards containing the page metadata.
    ///
    /// This is done in order to reduce lock contention under load.
    page_shards: [GuardedPages; NUM_BLOCKS_PER_FILE],
    change_op_stamp: AtomicU64,
    last_checkpoint: AtomicU64,
}

impl Default for PageTable {
    fn default() -> Self {
        let mut uninit_shards: [MaybeUninit<GuardedPages>; NUM_BLOCKS_PER_FILE] =
            [const { MaybeUninit::uninit() }; NUM_BLOCKS_PER_FILE];

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_BLOCKS_PER_FILE {
            let mut uninit_pages = Box::new_uninit_slice(NUM_PAGES_PER_BLOCK);
            for page_id in 0..NUM_PAGES_PER_BLOCK {
                uninit_pages[page_id].write(PageMetadata::empty());
            }

            // SAFETY: We have initialised each element in the array and the size of the array
            //         is aligned with the size of the array we're casting to.
            let pages = unsafe {
                let raw = Box::into_raw(uninit_pages);
                Box::from_raw(raw as *mut [PageMetadata; NUM_PAGES_PER_BLOCK])
            };

            uninit_shards[idx].write(RwLock::new(pages));
        }

        let init_shards = unsafe {
            mem::transmute::<
                [MaybeUninit<GuardedPages>; NUM_BLOCKS_PER_FILE],
                [GuardedPages; NUM_BLOCKS_PER_FILE],
            >(uninit_shards)
        };

        Self {
            page_shards: init_shards,
            change_op_stamp: AtomicU64::new(0),
            last_checkpoint: AtomicU64::new(0),
        }
    }
}

impl PageTable {
    /// Create a new [PageTable] using some existing set of pages.
    pub fn from_existing_state(pages: &[PageMetadata]) -> Self {
        let table = Self::default();
        table.write_pages(pages);
        table.checkpoint(table.get_current_op_stamp());
        table
    }

    pub(super) fn write_pages(&self, to_update: &[PageMetadata]) {
        let first_page = match to_update.first() {
            None => return,
            Some(first_page) => first_page,
        };

        let mut block_idx = (first_page.id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let mut page_idx = (first_page.id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        let mut block_shard = self.page_shards[block_idx].write();
        block_shard[page_idx] = *first_page;

        for metadata in to_update.iter().skip(1) {
            let old_block_idx = block_idx;
            block_idx = (metadata.id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
            page_idx = (metadata.id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

            if old_block_idx != block_idx {
                block_shard = self.page_shards[block_idx].write();
            }

            block_shard[page_idx] = *metadata;
        }

        self.change_op_stamp.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn collect_pages(
        &self,
        start_page: PageId,
        data_range: Range<usize>,
        results: &mut Vec<PageMetadata>,
    ) {
        assert!(
            start_page.0 < MAX_NUM_PAGES as u32,
            "page ID is beyond the bounds of the page table"
        );

        let n_pages_start = data_range.start / DISK_PAGE_SIZE;
        let n_pages_end = data_range.end.div_ceil(DISK_PAGE_SIZE);

        let n_pages_skip = n_pages_start;
        let n_pages_take = n_pages_end - n_pages_start;

        let mut block_idx = (start_page.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let mut page_idx = (start_page.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        // We do a little bit of weirdness here, particularly to avoid requesting a lock
        // on every block in the page table if we already have it.
        // The high level view is we're just getting each page in the page group
        // until we've selected the range of pages we need.

        let mut num_pages_taken = 0;
        let mut num_pages_skipped = 0;
        let mut block_shard = self.page_shards[block_idx].read();
        while num_pages_taken < n_pages_take {
            let page_metadata = &block_shard[page_idx];
            if num_pages_skipped < n_pages_skip {
                num_pages_skipped += 1;
            } else {
                assert!(
                    !page_metadata.is_empty(),
                    "BUG: page being referenced is empty"
                );
                results.push(*page_metadata);
                num_pages_taken += 1;
            }

            let next_page_id = page_metadata.next_page_id;
            if next_page_id.is_terminator() {
                break;
            }

            let old_block_idx = block_idx;
            block_idx = (next_page_id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
            page_idx = (next_page_id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

            if old_block_idx != block_idx {
                block_shard = self.page_shards[block_idx].read();
            }
        }
        drop(block_shard);
    }

    /// Fills the vector with any pages that are not empty within the page table.
    pub(super) fn collect_non_empty_pages(&self, pages: &mut Vec<PageMetadata>) {
        for shard in self.page_shards.iter() {
            for metadata in shard.read().iter() {
                if !metadata.is_empty() {
                    pages.push(*metadata);
                }
            }
        }
    }

    /// Returns if the page table has been modified since the last checkpoint.
    pub(super) fn has_changed(&self) -> bool {
        // TODO: Too strong?
        let current_op_stamp = self.change_op_stamp.load(Ordering::Acquire);
        current_op_stamp > self.last_checkpoint.load(Ordering::Acquire)
    }

    /// Returns the current op stamp of the page table.
    pub(super) fn get_current_op_stamp(&self) -> u64 {
        self.change_op_stamp.load(Ordering::Acquire)
    }

    /// Updates the last checkpointed op stamp to the new value.
    pub(super) fn checkpoint(&self, op_stamp: u64) {
        self.last_checkpoint.store(op_stamp, Ordering::Release);
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[test]
    fn test_page_table_from_existing_state() {
        let table = PageTable::from_existing_state(&[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                id: PageId(4),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                id: PageId((NUM_PAGES_PER_BLOCK + 4) as u32),
                data_len: 0,
                context: [0; 40],
            },
        ]);
        assert!(!table.has_changed());

        let pages = table.page_shards[0].read();
        assert_eq!(pages[4].id, PageId(4));
        let pages = table.page_shards[1].read();
        assert_eq!(pages[4].id, PageId((NUM_PAGES_PER_BLOCK + 4) as u32));
    }

    #[test]
    fn test_page_table_set_page() {
        let table = PageTable::default();

        {
            let pages = table.page_shards[0].read();
            assert!(pages[4].is_empty());
        }

        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };

        table.write_pages(&[metadata]);

        let pages = table.page_shards[0].read();
        assert_eq!(pages[4].id, PageId(4));
    }

    #[test]
    fn test_page_table_same_lock_update() {
        let table = PageTable::default();

        {
            let pages = table.page_shards[0].read();
            assert!(pages[5].is_empty());
            assert!(pages[6].is_empty());
        }

        // Test looping of metadata updates within same lock.
        let metadata1 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        };
        let metadata2 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(2),
            id: PageId(6),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata1, metadata2]);

        let pages = table.page_shards[0].read();
        assert_eq!(pages[5].id, PageId(5));
        assert_eq!(pages[6].id, PageId(6));
    }

    #[test]
    fn test_page_table_diff_lock_update() {
        let table = PageTable::default();
        {
            let pages = table.page_shards[0].read();
            assert!(pages[5].is_empty());
        }

        {
            let pages = table.page_shards[1].read();
            assert!(pages[4].is_empty());
        }

        // Test looping of metadata updates within same lock.
        let metadata1 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        };
        let metadata2 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(2),
            id: PageId((NUM_PAGES_PER_BLOCK + 4) as u32),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata1, metadata2]);

        let pages = table.page_shards[0].read();
        assert_eq!(pages[5].id, PageId(5));

        let pages = table.page_shards[1].read();
        assert_eq!(pages[4].id, PageId((NUM_PAGES_PER_BLOCK + 4) as u32));
    }

    #[test]
    fn test_page_table_collect_non_empty_pages() {
        let table = PageTable::default();
        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata]);

        let mut pages = Vec::new();
        table.collect_non_empty_pages(&mut pages);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id.0, 4);
    }

    #[test]
    fn test_page_table_change_detection() {
        let table = PageTable::default();

        assert!(!table.has_changed());

        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata]);

        assert!(table.has_changed());

        let op_stamp = table.get_current_op_stamp();
        assert_eq!(op_stamp, 1);

        table.checkpoint(1);
        assert!(!table.has_changed());
    }
}
