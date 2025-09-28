use std::io;
use std::sync::Arc;

use super::MetadataHeader;
use crate::directory::{FileGroup, FileId};
use crate::layout::file_metadata::Encryption;
use crate::layout::log::LogOp;
use crate::layout::{file_metadata, log};
use crate::stream_reader::{StreamReader, StreamReaderBuilder};
use crate::{ctx, file};

const BUFFER_SIZE: usize = 128 << 10;

#[derive(Debug, thiserror::Error)]
/// An error that prevented the reader from decoding a log block.
pub enum LogDecodeError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The decoder could not process the buffer.
    Decode(log::DecodeLogBlockError),
}

#[derive(Debug, thiserror::Error)]
/// An error that prevent the reader from opening the log.
pub enum LogOpenReadError {
    #[error("missing metadata header")]
    /// The file is missing the required metadata header.
    MissingHeader,
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The decoder could not process the buffer and decode the header.
    InvalidHeader(#[from] file_metadata::DecodeError),
    #[error("file is not encrypted but system has encryption enabled")]
    /// The encryption of the file does not align with the current context.
    ///
    /// This means the file is marked as not being encrypted but the system
    /// is configured for encryption.
    EncryptionStatusMismatch,
}

macro_rules! handle_io_result {
    ($res:expr, $expect_n:expr) => {{
        match $res {
            Ok(n) if n < $expect_n => {
                tracing::debug!("wal log reader has reached eof");
                return Ok(None);
            },
            Ok(_) => {},
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                tracing::debug!("wal log reader has reached eof");
                return Ok(None);
            },
            Err(err) => return Err(err.into()),
        }
    }};
}

/// The [LogFileReader] decodes
pub struct LogFileReader {
    ctx: Arc<ctx::FileContext>,
    log_file_id: u64,
    order_key: u64,
    expected_sequence_id: u32,
    reader: StreamReader,
    temp_buffer: Vec<u8>,
}

impl std::fmt::Debug for LogFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LogFileReader(file_id={:?}, log_file_id={}, order={})",
            self.reader.file_id(),
            self.log_file_id,
            self.order_key,
        )
    }
}

impl LogFileReader {
    /// Open an existing log file.
    ///
    /// This will read and validate the header of the file and perform all the
    /// necessary integrity checks.
    pub async fn open(
        ctx: Arc<ctx::FileContext>,
        file: file::ROFile,
    ) -> Result<Self, LogOpenReadError> {
        let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
        let n = file.read_buffer(&mut header_buffer, 0).await?;
        if n == 0 {
            return Err(LogOpenReadError::MissingHeader);
        }

        let associated_data =
            file_metadata::header_associated_data(file.id(), FileGroup::Wal);

        let header: MetadataHeader = file_metadata::decode_metadata(
            ctx.cipher(),
            &associated_data,
            &mut header_buffer[..file_metadata::HEADER_SIZE],
        )?;

        // The system will not even open if encryption is enabled and the system
        // is not setup for encryption.
        if header.encryption == Encryption::Disabled && ctx.cipher().is_some() {
            return Err(LogOpenReadError::EncryptionStatusMismatch);
        }

        Ok(Self::new(
            ctx,
            file,
            header.log_file_id,
            header.order_key,
            file_metadata::HEADER_SIZE as u64,
        ))
    }

    /// Create a new [LogFileReader] using the provided file context, file and offset.
    ///
    /// NOTE: This does _not_ validate the header of the file, it assumes the file
    /// is already valid
    pub(super) fn new(
        ctx: Arc<ctx::FileContext>,
        file: file::ROFile,
        log_file_id: u64,
        order_key: u64,
        log_offset: u64,
    ) -> Self {
        let reader = StreamReaderBuilder::new(ctx.clone(), file)
            .with_offset(log_offset)
            .with_buffer_size(BUFFER_SIZE)
            .build();
        Self {
            ctx,
            log_file_id,
            order_key,
            reader,
            expected_sequence_id: 0,
            temp_buffer: Vec::new(),
        }
    }

    #[inline]
    /// Returns the [FileId] of the underlying file used by the reader.
    pub fn file_id(&self) -> FileId {
        self.reader.file_id()
    }

    #[inline]
    /// Returns the order key of the WAL which can be used to
    /// ensure events are recovered and applied in the right order.
    pub fn order_key(&self) -> u64 {
        self.order_key
    }

    #[tracing::instrument("wal::read_block", skip(self))]
    /// Retrieve the next log block in the file.
    ///
    /// It is possible for the reader to return empty [log::LogBlock] due to padding
    /// of writes near the end of the file.
    ///
    /// Returns `Ok(None)` once at EOF.
    pub async fn next_transaction(
        &mut self,
        ops: &mut Vec<LogOp>,
    ) -> Result<Option<u64>, LogDecodeError> {
        let position = self.reader.position();

        self.temp_buffer.clear();
        let result = self
            .reader
            .read_n(&mut self.temp_buffer, log::HEADER_SIZE)
            .await;
        handle_io_result!(result, log::HEADER_SIZE);

        self.expected_sequence_id += 1;
        let associated_data = super::op_log_associated_data(
            self.reader.file_id(),
            self.log_file_id,
            self.expected_sequence_id,
            position,
        );

        let (transaction_id, buffer_len) = log::decode_log_header(
            self.ctx.cipher(),
            &associated_data,
            &mut self.temp_buffer,
        )
        .map_err(LogDecodeError::Decode)?;

        self.temp_buffer.clear();

        let result = self.reader.read_n(&mut self.temp_buffer, buffer_len).await;
        handle_io_result!(result, buffer_len);

        let ops_start_len = ops.len();
        log::decode_log_block(
            self.ctx.cipher(),
            &associated_data,
            &mut self.temp_buffer,
            ops,
        )
        .map_err(LogDecodeError::Decode)?;
        let num_ops = ops.len() - ops_start_len;

        tracing::trace!(num_ops = num_ops, "wal log recovered transaction");

        Ok(Some(transaction_id))
    }
}
