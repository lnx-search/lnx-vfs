use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cmp, io, mem};

use crate::buffer::DmaBuffer;
use crate::directory::{FileGroup, FileId};
use crate::file::DISK_ALIGN;
use crate::layout::log::{LogEntryHeader, LogOp};
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{file_metadata, log};
use crate::page_op_log::MetadataHeader;
use crate::utils::{align_down, align_up};
use crate::{ctx, file};

const BUFFER_SIZE: usize = 512 << 10;
const SEQUENCE_ID_START: u32 = 1;
static ORDER_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);
const MAX_TEMP_BUFFER_SIZE: usize = 512 << 10;

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
    buffered_pos: u64,

    temp_buffer: Vec<u8>,
    temp_buffer_offset: usize,

    /// The in-memory buffer of log blocks before they are written to disk.
    /// This is used to optimise the number of IOPs submitted to the IO scheduler.
    buffer: DmaBuffer,
    /// The offset that points to the end of the end of the initialised buffer,
    /// aka the end of where log blocks have been written to.
    buffer_offset: usize,
    /// The position in the buffer that has been submitted for writing to disk.
    buffer_write_pos: usize,

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
        let order_key = ORDER_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);

        let header = MetadataHeader {
            log_file_id: super::generate_random_log_id(),
            order_key,
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
            buffered_pos: 0,

            temp_buffer: Vec::new(),
            temp_buffer_offset: 0,

            buffer,
            buffer_offset: 0,
            buffer_write_pos: 0,

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
    pub fn position(&self) -> u64 {
        self.buffered_pos
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
        transaction_id: u64,
        ops: &Vec<LogOp>,
    ) -> io::Result<()> {
        #[cfg(test)]
        fail::fail_point!("wal::write_log", |_| Err(io::Error::other(
            "WAL write_log fail point error"
        )));

        self.ensure_file_writeable()?;
        let result = self.write_log_inner(transaction_id, ops).await;
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
        transaction_id: u64,
        ops: &Vec<LogOp>,
    ) -> io::Result<()> {
        let sequence_id = self.next_sequence_id();

        // Used as a sanity check that the behaviour is consistent with what other parts
        // other the system expects.
        #[cfg(debug_assertions)]
        sanity_check_log_values(transaction_id, ops);

        let associated_data = super::op_log_associated_data(
            self.file.id(),
            self.log_file_id,
            sequence_id,
            self.position(),
        );

        log::encode_log_block(
            self.ctx.cipher(),
            &associated_data,
            transaction_id,
            ops,
            &mut self.temp_buffer,
        ).map_err(|err| io::Error::new(ErrorKind::Other, err))?;

        let mut bytes_copied = self.copy_tmp_buffer_into_write_buffer();
        while bytes_copied < self.temp_buffer.len() {
            self.write_buffer().await?;
            bytes_copied += self.copy_tmp_buffer_into_write_buffer();
        }

        if self.buffer_offset >= self.buffer.len() {
            self.write_buffer().await?;
        }

        self.temp_buffer.clear();
        self.temp_buffer_offset = 0;
        if self.temp_buffer.capacity() >= MAX_TEMP_BUFFER_SIZE {
            self.temp_buffer.shrink_to(MAX_TEMP_BUFFER_SIZE);
        }

        Ok(())
    }

    pub(self) async fn sync_inner(&mut self) -> io::Result<()> {
        // Flush any intermediate buffers.
        self.write_buffer().await?;

        if let Some(iop) = self.inflight_iop.take() {
            tracing::trace!("waiting for inflight IOP to complete");
            complete_iop(iop).await?;
        }

        // TODO: self.file.fdatasync().await?;

        Ok(())
    }

    /// Submit the current memory buffer to the IO scheduler for writing
    /// and wait on the last submitted iop if applicable.
    async fn write_buffer(&mut self) -> io::Result<()> {
        let delta_len = self.buffer_offset - self.buffer_write_pos;
        let aligned_len = align_up(delta_len, DISK_ALIGN);
        let buffer = &self.buffer[self.buffer_write_pos..][..aligned_len];
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
        self.buffer_write_pos += align_down(delta_len, DISK_ALIGN);

        // Advance the file cursor, for the same reason as the block buffer pos
        // we only advance the cursor by aligned steps.
        self.current_pos += align_down(delta_len, DISK_ALIGN) as u64;

        // Note on write safety with these shared buffers.
        // Technically, the buffer could be modified while the request is still in flight,
        // however, this does not happen in practice as the only time we modify the buffer
        // just after calling this method is when the buffer is full, in which case we create
        // a new buffer from the arena anyway. When dealing with a partial buffer it is
        // during a fsync which will immediately wait for the IOP to complete.
        let guard = if self.buffer_offset >= self.buffer.len() {
            let buffer = self.take_memory_buffer();
            Arc::new(buffer) as file::DynamicGuard
        } else {
            self.buffer.share_guard() as file::DynamicGuard
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

    fn next_sequence_id(&mut self) -> u32 {
        let sequence_id = self.next_sequence_id;
        self.next_sequence_id += 1;
        sequence_id
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

    fn copy_tmp_buffer_into_write_buffer(&mut self) -> usize {
        let capacity = self.buffer.len() - self.buffer_offset;
        let remaining_temp = &self.temp_buffer[self.temp_buffer_offset..];
        let take_n = cmp::min(capacity, remaining_temp.len());
        self.buffer[self.buffer_offset..][..take_n]
            .copy_from_slice(&remaining_temp[..take_n]);
        self.buffer_offset += take_n;
        self.temp_buffer_offset += take_n;
        take_n
    }

    fn take_memory_buffer(&mut self) -> DmaBuffer {
        let new_buffer = self.ctx.alloc::<BUFFER_SIZE>();
        let block_buffer = mem::replace(&mut self.buffer, new_buffer);
        self.buffer_write_pos = 0;
        self.buffer_offset = 0;
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
fn sanity_check_log_values(transaction_id: u64, ops: &[LogOp]) {
    assert_ne!(transaction_id, u64::MAX);

    for op in ops {
        match op {
            LogOp::Write(op) => {
                for metadata in op.altered_pages.iter() {
                    assert!(metadata.next_page_id > metadata.id);
                }
            },
            LogOp::Free(_) => {},
            LogOp::Reassign(_) => {},
        }
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

        let entry = LogEntryHeader {
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
