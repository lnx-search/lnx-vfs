use std::io;
use std::sync::Arc;

use super::MetadataHeader;
use crate::directory::FileGroup;
use crate::layout::file_metadata::Encryption;
use crate::layout::{PageId, file_metadata, log};
use crate::stream_reader::{StreamReader, StreamReaderBuilder};
use crate::{buffer, ctx, file};

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
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The decoder could not process the buffer.
    InvalidHeader(#[from] file_metadata::DecodeError),
    #[error("file is not encrypted but system has encryption enabled")]
    /// The encryption of the file does not align with the current context.
    ///
    /// This means the file is marked as not being encrypted but the system
    /// is configured for encryption.
    EncryptionStatusMismatch,
}

/// The [LogFileReader] decodes
pub struct LogFileReader {
    ctx: Arc<ctx::FileContext>,
    log_file_id: u64,
    reader: StreamReader,
    scratch_space: Box<[u8; log::LOG_BLOCK_SIZE]>,
    next_page_id_to_decrypt: PageId,
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
        const NUM_PAGES_FOR_HEADER: usize =
            file_metadata::HEADER_SIZE.div_ceil(buffer::ALLOC_PAGE_SIZE);

        let mut header_buffer = ctx.alloc::<NUM_PAGES_FOR_HEADER>();
        file.read_buffer(&mut header_buffer, 0).await?;

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
        log_offset: u64,
    ) -> Self {
        let reader = StreamReaderBuilder::new(ctx.clone(), file)
            .with_offset(log_offset)
            .with_buffer_size(BUFFER_SIZE)
            .build();
        Self {
            ctx,
            log_file_id,
            reader,
            scratch_space: Box::new([0; log::LOG_BLOCK_SIZE]),
            next_page_id_to_decrypt: PageId(0),
        }
    }

    /// Retrieve the next log block in the file.
    ///
    /// Returns `Ok(None)` once at EOF.
    pub async fn next_block(&mut self) -> Result<Option<log::LogBlock>, LogDecodeError> {
        let position = self.reader.position();
        let result = self.reader.read_exact(&mut self.scratch_space[..]).await;

        match result {
            Ok(()) => {},
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            },
            Err(err) => return Err(err.into()),
        }

        let associated_data = super::op_log_associated_data(
            self.reader.file_id(),
            self.log_file_id,
            self.next_page_id_to_decrypt,
            position,
        );

        let block = log::decode_log_block(
            self.ctx.cipher(),
            &associated_data,
            &mut self.scratch_space[..],
        )
        .map_err(LogDecodeError::Decode)?;

        if let Some(page_id) = block.last_page_id() {
            self.next_page_id_to_decrypt = page_id;
        }

        Ok(Some(block))
    }
}
