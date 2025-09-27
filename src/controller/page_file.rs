use std::collections::VecDeque;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, io};

use foldhash::HashMapExt;
use parking_lot::RwLock;

use crate::buffer::DmaBuffer;
use crate::directory::FileGroup;
use crate::disk_allocator::InitState;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageId};
use crate::page_data::{
    CreatePageFileError,
    DISK_PAGE_SIZE,
    MAX_NUM_PAGES,
    MAX_SINGLE_IOP_NUM_PAGES,
    OpenPageFileError,
    PageFile,
    ReadPageError,
    SubmitWriterError,
};
use crate::page_file_allocator::{PageFileAllocator, WriteAllocTx};
use crate::{ctx, disk_allocator, file, utils};

const PREP_ALLOC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_READ_AMPLIFICATION: f32 = 1.3;
const MAX_READ_CONCURRENCY: usize = 128;

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
        let allocator = metadata_controller.create_page_file_allocator();

        let mut page_files = Vec::with_capacity(file_ids.len());
        let mut max_page_file_id = PageFileId(0);
        for file_id in file_ids {
            let file = directory.get_rw_file(FileGroup::Pages, file_id).await?;
            let page_file = PageFile::open(ctx.clone(), file).await?;
            let page_file_id = page_file.id();
            max_page_file_id = cmp::max(max_page_file_id, page_file_id);
            page_files.push(page_file);
        }

        let slf = Self {
            ctx,
            allocator,
            page_files: RwLock::new(foldhash::HashMap::with_capacity(page_files.len())),
            next_page_file_id: tokio::sync::Mutex::new(PageFileId(
                max_page_file_id.0 + 1,
            )),
        };

        for page_file in page_files {
            slf.add_new_page_file(page_file)
        }

        Ok(slf)
    }

    /// Returns the number of page files managed by the controller.
    pub fn num_page_files(&self) -> usize {
        self.page_files.read().len()
    }

    /// Creates a new [PageDataWriter] for writing a new group of pages of a given length.
    pub async fn create_writer(
        &self,
        len: u64,
    ) -> Result<PageDataWriter<'_>, CreatePageFileError> {
        let num_pages = len.div_ceil(DISK_PAGE_SIZE as u64) as u32;
        let alloc_txn = self.prep_alloc(num_pages).await?;
        let page_file = self
            .page_files
            .read()
            .get(&alloc_txn.page_file_id())
            .expect("BUG: page file does not exist after allocator selected pages")
            .clone();

        Ok(PageDataWriter::new(&self.ctx, page_file, alloc_txn, len))
    }

    #[tracing::instrument(skip(self, page_metadata), fields(num_pages = page_metadata.len()))]
    /// Read multiple pages of data from a given page file returning a
    /// channel receiver to collect the results.
    ///
    /// The provided set of pages to read must be sorted from the smallest page ID to largest.
    pub async fn read_many(
        &self,
        page_file_id: PageFileId,
        page_metadata: &[PageMetadata],
    ) -> io::Result<tokio::task::JoinSet<Result<ReadIop, ReadPageError>>> {
        tracing::debug!("reading many pages");

        let page_file = self
            .page_files
            .read()
            .get(&page_file_id)
            .cloned()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("page file not found: {page_file_id:?}"),
                )
            })?;

        if page_metadata.is_empty() {
            return Ok(tokio::task::JoinSet::new());
        } else if !page_metadata.is_sorted_by_key(|page| page.id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "provided pages to read are not sorted",
            ));
        }

        tracing::info!("got page file");
        let iops = page_metadata.iter().map(|page| {
            let start = page.id.0;
            let end = page.id.0 + 1;
            start..end
        });

        let iops = iops.collect::<Vec<_>>();
        tracing::info!(iops = ?iops, "iopp[s");
        let coalesced_iops = crate::coalesce::coalesce_read(
            iops,
            MAX_SINGLE_IOP_NUM_PAGES as u32,
            Some(MAX_READ_AMPLIFICATION),
        );
        tracing::info!(?coalesced_iops, "coalesced");

        let limiter = Arc::new(tokio::sync::Semaphore::new(MAX_READ_CONCURRENCY));
        let mut join_set = tokio::task::JoinSet::new();
        for iop in coalesced_iops {
            tracing::debug!(iop = ?iop, "dispatching");
            let limiter = limiter.clone();
            let page_file = page_file.clone();

            let buffer = self.ctx.alloc_pages(utils::disk_to_alloc_pages(iop.len()));
            let metadata_slice = get_page_range_to_page_indices(page_metadata, iop);
            let pages_slice = page_metadata[metadata_slice].to_vec();

            join_set.spawn(async move {
                let _permit = limiter.acquire().await;
                tracing::debug!("waiting!");
                let (mask, buffer) = page_file.read_at(&pages_slice, buffer).await?;
                tracing::debug!("done!");
                Ok(ReadIop {
                    pages: pages_slice.into_iter(),
                    mask,
                    offset: 0,
                    buffer,
                })
            });
        }

        Ok(join_set)
    }

    /// Attempts to get a [WriteAllocTx] from an existing page file allocator
    /// otherwise a new page file is created and added to the allocator pool.
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

        #[cfg(test)]
        fail::fail_point!("page_file_controller::create_new_page_file", |_| Ok(()));

        let directory = self.ctx.directory();
        let file_id = directory.create_new_file(FileGroup::Pages).await?;

        let file = directory.get_rw_file(FileGroup::Pages, file_id).await?;

        let page_file =
            PageFile::create(self.ctx.clone(), file, next_page_file_id).await?;
        self.add_new_page_file(page_file);

        drop(creation_guard);

        Ok(())
    }

    fn add_new_page_file(&self, page_file: PageFile) {
        let page_file_id = page_file.id();

        {
            tracing::debug!(page_file_id = ?page_file_id, "adding new page file to allocator");
            let mut page_files = self.page_files.write();
            let maybe_existing = page_files.insert(page_file_id, page_file);
            assert!(
                maybe_existing.is_none(),
                "BUG: Page file was overwritten which should never happen"
            );
        }

        self.allocator.insert_page_file(
            page_file_id,
            disk_allocator::PageAllocator::new(InitState::Free),
        );
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

        let write_iops =
            crate::coalesce::coalesce_write(page_iops, MAX_SINGLE_IOP_NUM_PAGES as u32);

        let mut slf = Self {
            ctx,
            page_file,
            alloc_tx,
            write_iops: write_iops.into_iter(),

            expected_len,
            bytes_written: 0,

            metadata_pages: Vec::new(),
            inflight_iops: VecDeque::new(),
            buffer_writer: None,
        };
        slf.replace_writer_with_next_iop();
        slf
    }

    /// The total number of pages allocated for this write.
    pub fn num_pages_allocated(&self) -> usize {
        self.alloc_tx
            .spans()
            .iter()
            .map(|span| span.span_len as usize)
            .sum()
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

        self.process_completed_iops()?;

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

/// A completed read IOP spanning a range of pages.
pub struct ReadIop {
    pages: std::vec::IntoIter<PageMetadata>,
    mask: u8,
    offset: usize,
    buffer: DmaBuffer,
}

impl std::fmt::Debug for ReadIop {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadIop(mask=0b{:<08b}, num_pages={})",
            self.mask,
            self.mask.count_ones()
        )
    }
}

impl ReadIop {
    /// Gets the next page in the IOP and returns the slice of data for the page
    /// along with the metadata of the page.
    pub fn next_page(&mut self) -> Option<(PageMetadata, &[u8])> {
        let page = self.pages.next()?;
        let offset = self.advance_to_next_set_bit();
        Some((page, &self.buffer[offset..][..page.data_len as usize]))
    }

    fn advance_to_next_set_bit(&mut self) -> usize {
        while self.mask & (1 << self.offset) == 0 {
            self.offset += 1;
        }
        self.offset
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

fn get_page_range_to_page_indices(
    metadata: &[PageMetadata],
    page_range: Range<u32>,
) -> Range<usize> {
    let mut start = None;
    let mut end = None;

    for (idx, page) in metadata.iter().enumerate() {
        if start.is_none() && page_range.contains(&page.id.0) {
            start = Some(idx);
        }

        if start.is_some() && !page_range.contains(&page.id.0) {
            end = Some(idx);
            break;
        }
    }

    if start.is_some() && end.is_none() {
        end = Some(metadata.len());
    }

    let start = start.unwrap();
    let end = end.unwrap();

    start..end
}

struct InflightIop {
    reply: i2o2::ReplyReceiver,
    expected_len: usize,
}

fn short_write_err() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "short write occurred")
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::buffer::ALLOC_PAGE_SIZE;

    #[test]
    fn test_read_iop_debug() {
        let iop = ReadIop {
            pages: vec![].into_iter(),
            mask: 0b0100_0000,
            offset: 0,
            buffer: DmaBuffer::alloc_empty(),
        };
        assert_eq!(format!("{iop:?}"), "ReadIop(mask=0b01000000, num_pages=1)");
    }

    #[test]
    fn test_read_iop_advance_next_bit() {
        let mut iop = ReadIop {
            pages: vec![].into_iter(),
            mask: 0b0010_0000,
            offset: 0,
            buffer: DmaBuffer::alloc_empty(),
        };

        let offset = iop.advance_to_next_set_bit();
        assert_eq!(offset, 5);
    }

    #[rstest::rstest]
    #[case::empty1(0, &[], 0, &[])]
    #[case::empty2(1, &[], ALLOC_PAGE_SIZE, &[])]
    #[case::write_single_page1(1, &[(b"Hello, world!".as_slice(), 13)], ALLOC_PAGE_SIZE - 13, b"Hello, world!")]
    #[case::write_single_page2(
        1,
        &[
            (b"Hello, world 1!".as_slice(), 15),
            (b"Hello, world 2!".as_slice(), 15),
            (b"Hello, world 3!".as_slice(), 15),
        ],
        ALLOC_PAGE_SIZE - 15 * 3,
        b"Hello, world 1!Hello, world 2!Hello, world 3!",
    )]
    #[case::write_to_eof(
        1,
        &[
            ([4u8; ALLOC_PAGE_SIZE].as_ref(), ALLOC_PAGE_SIZE),
            (b"Hello, world!".as_slice(), 0),
        ],
        0,
        &[4u8; ALLOC_PAGE_SIZE],
    )]
    #[case::many_pages(
        3,
        &[
            ([4u8; ALLOC_PAGE_SIZE].as_ref(), ALLOC_PAGE_SIZE),
            ([4u8; 4].as_ref(), 4),
        ],
        ALLOC_PAGE_SIZE * 2 - 4,
        &[4u8; ALLOC_PAGE_SIZE + 4],
    )]
    fn test_dma_buf_writer(
        #[case] num_pages: u32,
        #[case] writes: &[(&[u8], usize)],
        #[case] expected_remaining_capacity: usize,
        #[case] expected_buffer: &[u8],
    ) {
        let buffer = DmaBuffer::alloc_sys(num_pages as usize);
        let mut writer = DmaBufWriter::new(PageId(0), num_pages, buffer);
        for &(mut write, expect_n) in writes {
            let n = writer.write(&mut write);
            assert_eq!(n, expect_n);
        }
        assert_eq!(writer.remaining_capacity(), expected_remaining_capacity);
        assert_eq!(&writer.buffer[..writer.offset], expected_buffer);
    }

    #[rstest::rstest]
    #[case::single_page(
        &[
            PageMetadata::unassigned(PageId(4)),
        ],
        4..5,  // Page IDs
        0..1
    )]
    #[case::many_dense_pages(
        &[
            PageMetadata::unassigned(PageId(10)),
            PageMetadata::unassigned(PageId(11)),
            PageMetadata::unassigned(PageId(12)),
            PageMetadata::unassigned(PageId(13)),
        ],
        10..13,  // Page IDs
        0..3
    )]
    #[case::many_sparse_pages1(
        &[
            PageMetadata::unassigned(PageId(10)),
            PageMetadata::unassigned(PageId(11)),
            PageMetadata::unassigned(PageId(15)),
            PageMetadata::unassigned(PageId(17)),
            PageMetadata::unassigned(PageId(32)),
        ],
        10..16,  // Page IDs
        0..3
    )]
    #[case::many_sparse_pages2(
        &[
            PageMetadata::unassigned(PageId(9)),
            PageMetadata::unassigned(PageId(11)),
            PageMetadata::unassigned(PageId(15)),
            PageMetadata::unassigned(PageId(17)),
            PageMetadata::unassigned(PageId(32)),
        ],
        11..18,  // Page IDs
        1..4,
    )]
    #[case::many_sparse_pages_end_read(
        &[
            PageMetadata::unassigned(PageId(9)),
            PageMetadata::unassigned(PageId(11)),
            PageMetadata::unassigned(PageId(15)),
            PageMetadata::unassigned(PageId(17)),
            PageMetadata::unassigned(PageId(32)),
        ],
        32..33,  // Page IDs
        4..5,
    )]
    #[should_panic(expected = "")]
    #[case::empty_metadata_and_range(&[], 0..0, 0..0)]
    #[trace]
    fn test_get_page_range_to_page_indices(
        #[case] metadata: &[PageMetadata],
        #[case] input_range: Range<u32>,
        #[case] expected_range: Range<usize>,
    ) {
        let actual_range = get_page_range_to_page_indices(metadata, input_range);
        assert_eq!(actual_range, expected_range);
    }
}
