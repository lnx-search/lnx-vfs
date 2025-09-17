use std::io::ErrorKind;
use std::sync::Arc;
use std::{io, mem};

use crate::buffer::DmaBuffer;
use crate::directory::{FileGroup, FileId};
use crate::file::DISK_ALIGN;
use crate::layout::log::LogEntry;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{file_metadata, log};
use crate::page_op_log::MetadataHeader;
use crate::utils::{align_down, align_up};
use crate::{ctx, file};

const BUFFER_SIZE: usize = 128 << 10;
const SEQUENCE_ID_START: u32 = 1;

#[derive(Debug, thiserror::Error)]
/// An error that prevent the writer from opening the log.
pub enum LogOpenWriteError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The file metadata encoder could not encode and write the header to the buffer.
    HeaderEncode(#[from] file_metadata::EncodeError),
}

/// The [LogFileWriter] acts like a WAL for operations occurring on the page store,
/// it only logs the metadata operations however, so any data writes should be safely
/// persisted before writing to this log.
///
/// The writer will internally buffer logs into blocks forming 512b chunks, which are then
/// buffered in memory before being flushed to disk.  The data is written in a way that
/// prevents torn-writes.
///
/// The file has a close-on-error semantic, meaning when an error occurs the writer
/// will be closed and no new operations will be available.
/// This is done in order to prevent accidental corruption of phantom data.
pub struct LogFileWriter {
    ctx: Arc<ctx::FileContext>,
    file: file::RWFile,
    log_file_id: u64,
    locked_out: bool,

    log_offset: u64,
    current_pos: u64,
    block_absolute_pos: u64,

    /// The log block that is currently being filled with [LogEntry]s.
    /// When this is filled it produces a 512 byte block which is then
    /// written to the `block_buffer`.
    wip_block: log::LogBlock,
    /// The in-memory buffer of log blocks before they are written to disk.
    /// This is used to optimise the number of IOPs submitted to the IO scheduler.
    block_buffer: DmaBuffer,
    /// The offset that points to the end of the end of the initialised buffer,
    /// aka the end of where log blocks have been written to.
    block_offset: usize,
    /// The position in the buffer that has been submitted for writing to disk.
    block_buffer_write_pos: usize,

    /// The unique monotonic ID assigned to each log entry.
    next_sequence_id: u32,

    inflight_iop: Option<InflightIop>,
}

impl LogFileWriter {
    /// Open an existing log file.
    ///
    /// This will read and validate the header of the file and perform all the
    /// necessary integrity checks.
    pub async fn create(
        ctx: Arc<ctx::FileContext>,
        file: file::RWFile,
    ) -> Result<Self, LogOpenWriteError> {
        let header = MetadataHeader {
            log_file_id: super::generate_random_log_id(),
            encryption: ctx.get_encryption_status(),
        };

        let associated_data =
            file_metadata::header_associated_data(file.id(), FileGroup::Wal);

        let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
        file_metadata::encode_metadata(
            ctx.cipher(),
            &associated_data,
            &header,
            &mut header_buffer[..file_metadata::HEADER_SIZE],
        )?;
        file.write_buffer(&mut header_buffer, 0).await?;

        Ok(Self::new(
            ctx,
            file,
            header.log_file_id,
            file_metadata::HEADER_SIZE as u64,
        ))
    }

    /// Create a new [LogFileWriter] using the provided file context, file and offset.
    pub(super) fn new(
        ctx: Arc<ctx::FileContext>,
        file: file::RWFile,
        log_file_id: u64,
        log_offset: u64,
    ) -> Self {
        assert_eq!(
            log_offset as usize % DISK_ALIGN,
            0,
            "log offset must be a multiple of the disk alignment"
        );

        let buffer = ctx.alloc::<BUFFER_SIZE>();

        Self {
            ctx,
            file,
            log_file_id,
            locked_out: false,

            log_offset,
            current_pos: 0,
            block_absolute_pos: log::LOG_BLOCK_SIZE as u64,

            wip_block: log::LogBlock::default(),
            block_buffer: buffer,
            block_offset: log::LOG_BLOCK_SIZE,
            block_buffer_write_pos: 0,

            next_sequence_id: SEQUENCE_ID_START,

            inflight_iop: None,
        }
    }

    #[inline]
    /// Returns whether the file is locked out due to a prior error.
    pub fn is_locked_out(&self) -> bool {
        self.locked_out
    }

    #[inline]
    /// Returns the sequence ID the writer is sitting at.
    pub fn current_sequence_id(&self) -> u32 {
        self.next_sequence_id - 1
    }

    #[inline]
    /// Returns the position of the writer cursor.
    ///
    /// NOTE: This is not strictly tied to the file cursor, instead it is
    ///       the absolute position of the next block as if it is about to be written.
    pub fn position(&self) -> u64 {
        self.log_offset + self.block_absolute_pos
    }

    /// Returns ID of the file being written to by the writer.
    pub fn file_id(&self) -> FileId {
        self.file.id()
    }

    /// Consume the writer and return the inner ring file.
    pub fn into_file(self) -> file::RWFile {
        self.file
    }

    #[tracing::instrument("wal::write_entry", skip_all)]
    /// Write a set of blocks to the log file at the current position.
    ///
    /// The `sequence_id` and `last_flush_sequence_id` fields will be overwritten.
    ///
    /// WARNING: This does not strictly flush data to disk! You must call `sync()` separately
    /// to persist the data safely.
    pub async fn write_log(
        &mut self,
        entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> io::Result<()> {
        #[cfg(test)]
        fail::fail_point!("wal::write_log", |_| Err(io::Error::other(
            "WAL write_log fail point error"
        )));

        self.ensure_file_writeable()?;
        let result = self.write_log_inner(entry, metadata).await;
        if result.is_err() {
            tracing::error!(result = ?result, "write call failed, locking out log writer");
            self.locked_out = true;
        }
        result
    }

    #[tracing::instrument("wal::sync", skip_all)]
    /// Flush the buffered log data to disk and ensure it is safely persisted.
    ///
    /// Returns the position the file is flushed up to.
    pub async fn sync(&mut self) -> io::Result<()> {
        #[cfg(test)]
        fail::fail_point!("wal::sync", |_| Err(io::Error::other(
            "WAL sync fail point error"
        )));

        self.ensure_file_writeable()?;
        let result = self.sync_inner().await;
        if result.is_err() {
            tracing::error!(result = ?result, "sync call failed, locking out log writer");
            self.locked_out = true;
        }
        result
    }

    pub(self) async fn write_log_inner(
        &mut self,
        mut entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> io::Result<()> {
        self.assign_writer_context(&mut entry);

        // Used as a sanity check that the behaviour is consistent with what other parts
        // other the system expects.
        #[cfg(debug_assertions)]
        sanity_check_log_values(&entry, metadata.as_ref());

        let (entry, metadata) = match self.wip_block.push_entry(entry, metadata) {
            Ok(()) => return Ok(()),
            Err(pair) => pair,
        };

        self.flush_log_block_to_mem()?;

        // When the block is full, we can reset it as the alignment will be maintained.
        self.wip_block.reset();
        self.block_offset += log::LOG_BLOCK_SIZE;
        self.block_absolute_pos += log::LOG_BLOCK_SIZE as u64;

        let result = self.wip_block.push_entry(entry, metadata);
        assert!(result.is_ok(), "block should never be full after reset");

        if self.block_offset >= self.block_buffer.len() {
            tracing::debug!("memory buffer capacity reached, flushing...");
            self.write_buffer().await?;
        }

        Ok(())
    }

    pub(self) async fn sync_inner(&mut self) -> io::Result<()> {
        // Flush any intermediate buffers.
        self.flush_log_block_to_mem()?;
        self.write_buffer().await?;

        if let Some(iop) = self.inflight_iop.take() {
            tracing::trace!("waiting for inflight IOP to complete");
            complete_iop(iop).await?;
        }

        Ok(())
    }

    fn flush_log_block_to_mem(&mut self) -> io::Result<()> {
        let start_position = self.position() - log::LOG_BLOCK_SIZE as u64;
        let buffer_start = self.block_offset - log::LOG_BLOCK_SIZE;
        let buffer = &mut self.block_buffer[buffer_start..][..log::LOG_BLOCK_SIZE];
        let associated_data = super::op_log_associated_data(
            self.file.id(),
            self.log_file_id,
            start_position,
        );
        log::encode_log_block(
            self.ctx.cipher(),
            &associated_data,
            &self.wip_block,
            buffer,
        )
        .map_err(io::Error::other)?;

        Ok(())
    }

    /// Submit the current memory buffer to the IO scheduler for writing
    /// and wait on the last submitted iop if applicable.
    async fn write_buffer(&mut self) -> io::Result<()> {
        let delta_len = self.block_offset - self.block_buffer_write_pos;
        let aligned_len = align_up(delta_len, DISK_ALIGN);
        let buffer = &self.block_buffer[self.block_buffer_write_pos..][..aligned_len];
        let write_offset = self.log_offset + self.current_pos;

        let buffer_ptr = buffer.as_ptr();
        let buffer_len = buffer.len();

        tracing::debug!(
            offset = write_offset,
            len = buffer_len,
            "flushing memory buffer to disk"
        );

        // We advance the write pos cursor while still maintaining alignment.
        // We can do this because future writes will replay the unaligned chunk
        // of the buffer until it is long enough to be aligned.
        self.block_buffer_write_pos += align_down(delta_len, DISK_ALIGN);

        // Advance the file cursor, for the same reason as the block buffer pos
        // we only advance the cursor by aligned steps.
        self.current_pos += align_down(delta_len, DISK_ALIGN) as u64;

        // Note on write safety with these shared buffers.
        // Technically, the buffer could be modified while the request is still in flight,
        // however, this does not happen in practice as the only time we modify the buffer
        // just after calling this method is when the buffer is full, in which case we create
        // a new buffer from the arena anyway. When dealing with a partial buffer it is
        // during a fsync which will immediately wait for the IOP to complete.
        let guard = if self.block_offset >= self.block_buffer.len() {
            let buffer = self.take_memory_buffer();
            Arc::new(buffer) as file::DynamicGuard
        } else {
            self.block_buffer.share_guard() as file::DynamicGuard
        };

        // SAFETY: our op is safe to send across the thread boundaries and the buffer
        //         is guaranteed to live at least as long as the ring requires as it
        //         is passed to our ring guard.
        let reply = unsafe {
            self.file
                .submit_write(buffer_ptr, buffer_len, write_offset, Some(guard))
                .await?
        };

        let iop = InflightIop {
            reply,
            expected_write_size: buffer_len,
        };

        // We don't immediately wait for the reply as we don't actually care if it completes
        // until we flush. However, if reply is already set, we will attempt to get
        // the result.
        if let Some(iop) = self.inflight_iop.replace(iop) {
            complete_iop(iop).await?;
        }

        Ok(())
    }

    fn assign_writer_context(&mut self, entry: &mut LogEntry) {
        entry.sequence_id = self.next_sequence_id;
        self.next_sequence_id += 1;
    }

    fn ensure_file_writeable(&mut self) -> io::Result<()> {
        if self.is_locked_out() {
            Err(io::Error::new(
                ErrorKind::ReadOnlyFilesystem,
                "writer is locked due to prior error",
            ))
        } else {
            Ok(())
        }
    }

    fn take_memory_buffer(&mut self) -> DmaBuffer {
        let new_buffer = self.ctx.alloc::<BUFFER_SIZE>();
        let block_buffer = mem::replace(&mut self.block_buffer, new_buffer);
        self.block_buffer_write_pos = 0;
        self.block_offset = log::LOG_BLOCK_SIZE;

        // An absolute hack that we should destroy. This "fixes" the fact that the buffer
        // leaves a 512B empty tail... Which should really be fixed on the buffer side
        // but writing the memory correctly and then doing all the writing is... painful.
        self.block_absolute_pos += log::LOG_BLOCK_SIZE as u64;

        block_buffer
    }
}

async fn complete_iop(iop: InflightIop) -> io::Result<()> {
    let InflightIop {
        reply,
        expected_write_size,
    } = iop;

    let result = file::wait_for_reply(reply).await?;
    if result != expected_write_size {
        Err(io::Error::new(
            ErrorKind::StorageFull,
            "storage failed to allocate",
        ))
    } else {
        Ok(())
    }
}

struct InflightIop {
    reply: i2o2::ReplyReceiver,
    expected_write_size: usize,
}

#[cfg(debug_assertions)]
fn sanity_check_log_values(entry: &LogEntry, metadata: Option<&PageMetadata>) {
    assert_ne!(entry.transaction_n_entries, 0);
    assert_ne!(entry.transaction_id, u64::MAX);

    if let Some(metadata) = metadata {
        assert_eq!(entry.page_id, metadata.id);
        assert!(metadata.next_page_id > metadata.id);
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::directory::FileGroup;
    use crate::layout::log::LogOp;
    use crate::layout::{PageFileId, PageId};

    #[tokio::test]
    async fn test_writer_sequence_id() {
        let ctx = ctx::FileContext::for_test(false).await;
        let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

        let mut writer = LogFileWriter::new(ctx, file, 1, 0);

        let entry = LogEntry {
            sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 1,
            page_id: PageId(5),
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };

        writer.write_log(entry, None).await.expect("write log");
        assert_eq!(writer.next_sequence_id, 2);

        let entries = writer.wip_block.entries();
        assert_eq!(entries.len(), 1);
        let entry = entries[0].log;
        assert_eq!(entry.sequence_id, 1);

        writer.sync().await.expect("sync log");

        writer.write_log(entry, None).await.expect("write log");
        assert_eq!(writer.next_sequence_id, 3);

        let entries = writer.wip_block.entries();
        assert_eq!(entries.len(), 2);
        let entry = entries[1].log;
        assert_eq!(entry.sequence_id, 2);
    }
}
