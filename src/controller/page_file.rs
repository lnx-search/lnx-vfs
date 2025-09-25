use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, io};

use foldhash::HashMapExt;
use parking_lot::RwLock;

use crate::buffer::DmaBuffer;
use crate::directory::FileGroup;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageId};
use crate::page_data::{
    CreatePageFileError,
    DISK_PAGE_SIZE,
    MAX_NUM_PAGES,
    OpenPageFileError,
    PageFile,
    SubmitWriterError,
};
use crate::page_file_allocator::{PageFileAllocator, WriteAllocTx};
use crate::{ctx, file, layout, page_data, utils};

const PREP_ALLOC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_WRITE_IOP_SIZE: u32 = 8;

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
    async fn prep_alloc(
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

/// Creates a writer that automatically buffers and submits writes to a page file
/// asynchronously.
///
/// The caller must call `finish()` in order to complete the write and get the resulting
/// page metadata entries, otherwise, the pending write will be aborted and the allocated pages
/// put back into the allocation pool.
pub struct PageDataWriter<'controller> {
    ctx: &'controller ctx::FileContext,
    page_file: PageFile,
    alloc_tx: WriteAllocTx<'controller>,
    write_iops: smallvec::IntoIter<[Range<u32>; 8]>,

    expected_len: u64,
    bytes_written: u64,

    metadata_pages: Vec<PageMetadata>,

    inflight_iops: VecDeque<InflightIop>,
    buffer_writer: Option<DmaBufWriter>,
}

impl<'controller> PageDataWriter<'controller> {
    fn new(
        ctx: &'controller ctx::FileContext,
        page_file: PageFile,
        alloc_tx: WriteAllocTx<'controller>,
        expected_len: u64,
    ) -> Self {
        let page_iops = alloc_tx.spans().iter().map(|span| {
            let start = span.start_page;
            let end = start + span.span_len as u32;
            start..end
        });

        let write_iops = crate::coalesce::coalesce_write(page_iops, MAX_WRITE_IOP_SIZE);

        Self {
            ctx,
            page_file,
            alloc_tx,
            write_iops: write_iops.into_iter(),

            expected_len,
            bytes_written: 0,

            metadata_pages: Vec::new(),
            inflight_iops: VecDeque::new(),
            buffer_writer: None,
        }
    }

    /// Write a buffer to the writer.
    pub async fn write(&mut self, mut buf: &[u8]) -> Result<(), SubmitWriterError> {
        if self.bytes_written + buf.len() as u64 > self.expected_len {
            return Err(io::Error::new(
                io::ErrorKind::QuotaExceeded,
                "write could not be completed as it would go beyond the \
                bounds of the defined page size",
            )
            .into());
        }

        while !buf.is_empty() && self.buffer_writer.is_some() {
            if let Some(buffer_writer) = self.buffer_writer.as_mut() {
                let n = buffer_writer.write(&mut buf);
                self.bytes_written += n as u64;

                if n == buf.len() {
                    break;
                }
            }

            self.flush_buffer().await?;
        }

        if !buf.is_empty() {
            panic!(
                "BUG: writer should have capacity yet it did not consume the full buffer"
            );
        }

        Ok(())
    }

    /// Complete the pending write operation.
    ///
    /// The operation will error if there is still data expected to be written
    /// or if a remaining IOP errors.
    pub async fn finish(
        mut self,
    ) -> Result<(WriteAllocTx<'controller>, Vec<PageMetadata>), SubmitWriterError> {
        if self.bytes_written < self.expected_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "not all expected data has be submitted to the writer",
            )
            .into());
        }

        self.flush_buffer().await?;

        while let Some(iop) = self.inflight_iops.pop_front() {
            let result = file::wait_for_reply(iop.reply).await?;
            if result < iop.expected_len {
                return Err(short_write_err().into());
            }
        }

        Ok((self.alloc_tx, self.metadata_pages))
    }

    /// Flush the full memory buffer and submit the buffer to be written to the page file.
    async fn flush_buffer(&mut self) -> Result<(), SubmitWriterError> {
        let maybe_old_writer = self.replace_writer_with_next_iop();

        let Some(old_writer) = maybe_old_writer else {
            return Ok(());
        };

        let DmaBufWriter {
            start_page,
            num_pages,
            buffer,
            offset,
        } = old_writer;

        assert!(
            offset == buffer.len() || self.bytes_written == self.expected_len,
            "BUG: system failed sanity check ensuring partial page write is only possible \
            at the end of the group."
        );

        let expected_len = buffer.len();
        let start = self.metadata_pages.len();
        for page_offset in 0..num_pages {
            let page_id = PageId(start_page.0 + page_offset);
            let mut metadata = PageMetadata::unassigned(page_id);
            let unaligned_bytes = offset % DISK_PAGE_SIZE;
            if unaligned_bytes != 0 && page_offset == num_pages - 1 {
                metadata.data_len = unaligned_bytes as u32;
            } else {
                metadata.data_len = DISK_PAGE_SIZE as u32;
            }
            self.metadata_pages.push(metadata);
        }

        let reply = self
            .page_file
            .submit_write_at(&mut self.metadata_pages[start..], buffer)
            .await?;

        self.inflight_iops.push_back(InflightIop {
            reply,
            expected_len,
        });

        Ok(())
    }

    fn process_completed_iops(&mut self) -> io::Result<()> {
        while let Some(iop) = self.inflight_iops.pop_front() {
            match file::try_get_reply(&iop.reply) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.inflight_iops.push_front(iop);
                },
                Err(e) => return Err(e),
                Ok(result) if result < iop.expected_len => {
                    return Err(short_write_err());
                },
                Ok(_) => {},
            }
        }
        Ok(())
    }

    fn replace_writer_with_next_iop(&mut self) -> Option<DmaBufWriter> {
        if let Some(next_iop) = self.write_iops.next() {
            let dma_buffer = self
                .ctx
                .alloc_pages(utils::disk_to_alloc_pages(next_iop.len()));
            let writer = DmaBufWriter::new(
                PageId(next_iop.start),
                next_iop.len() as u32,
                dma_buffer,
            );
            self.buffer_writer.replace(writer)
        } else {
            self.buffer_writer.take()
        }
    }
}

struct DmaBufWriter {
    start_page: PageId,
    num_pages: u32,
    buffer: DmaBuffer,
    offset: usize,
}

impl DmaBufWriter {
    fn new(start_page: PageId, num_pages: u32, buffer: DmaBuffer) -> Self {
        Self {
            start_page,
            num_pages,
            buffer,
            offset: 0,
        }
    }

    fn write(&mut self, buffer: &mut &[u8]) -> usize {
        let take_n = cmp::min(buffer.len(), self.remaining_capacity());
        self.buffer[self.offset..][..take_n].copy_from_slice(&buffer[..take_n]);
        self.offset += take_n;
        *buffer = &buffer[take_n..];
        take_n
    }

    fn remaining_capacity(&self) -> usize {
        self.buffer.len() - self.offset
    }
}

struct InflightIop {
    reply: i2o2::ReplyReceiver,
    expected_len: usize,
}

fn short_write_err() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "short write occurred")
}
