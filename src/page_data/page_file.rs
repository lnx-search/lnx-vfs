use std::io;
use std::sync::Arc;

use crate::buffer::DmaBuffer;
use crate::directory::{FileGroup, FileId};
use crate::file::DynamicGuard;
use crate::layout::file_metadata::Encryption;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageId, encrypt, file_metadata, integrity};
use crate::page_data::{
    CONTEXT_BUFFER_SIZE,
    DISK_PAGE_SIZE,
    MAX_NUM_PAGES,
    MAX_SINGLE_IOP_NUM_PAGES,
    page_associated_data,
};
use crate::{ctx, file};

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from opening a page file.
pub enum OpenPageFileError {
    #[error("missing metadata header")]
    /// The file is missing the required metadata header.
    MissingHeader,
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The file metadata decoder could not deserialize the header.
    HeaderDecode(#[from] file_metadata::DecodeError),
    #[error("file is not encrypted but system has encryption enabled")]
    /// The encryption of the file does not align with the current context.
    ///
    /// This means the file is marked as not being encrypted but the system
    /// is configured for encryption.
    EncryptionStatusMismatch,
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from creating a new page file.
pub enum CreatePageFileError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The file metadata encoder could not serialize the header.
    HeaderEncode(#[from] file_metadata::EncodeError),
}

#[derive(Debug, thiserror::Error)]
/// An error preventing the page file from writing the page data.
pub enum SubmitWriterError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The system was unable to encrypt the page data.
    EncryptionError(#[from] encrypt::EncryptError),
}

#[derive(Debug, thiserror::Error)]
/// An error preventing the page file from reading the page data.
pub enum ReadPageError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The system was unable to decode the page data.
    DecodeError(#[from] super::encode::DecodePageError),
    #[error("short read")]
    /// A short read occurred and should be retried.
    ShortRead,
}

#[derive(Clone)]
/// The page file contains blocks of data called "pages".
pub struct PageFile {
    id: PageFileId,
    ctx: Arc<ctx::FileContext>,
    file: file::RWFile,
    data_offset: u64,
}

impl PageFile {
    /// Returns the ID of the page file.
    pub fn id(&self) -> PageFileId {
        self.id
    }
    /// Returns the file ID of the page file.
    pub fn file_id(&self) -> FileId {
        self.file.id()
    }

    /// Open an existing page file.
    pub async fn open(
        ctx: Arc<ctx::FileContext>,
        file: file::RWFile,
    ) -> Result<Self, OpenPageFileError> {
        let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
        let n = file.read_buffer(&mut header_buffer, 0).await?;
        if n == 0 {
            return Err(OpenPageFileError::MissingHeader);
        }

        let header_associated_data =
            file_metadata::header_associated_data(file.id(), FileGroup::Pages);

        let header: super::MetadataHeader = file_metadata::decode_metadata(
            ctx.cipher(),
            &header_associated_data,
            &mut header_buffer[..file_metadata::HEADER_SIZE],
        )?;
        drop(header_buffer);

        // The system will not even open if encryption is enabled and the system
        // is not setup for encryption.
        if header.encryption == Encryption::Disabled && ctx.cipher().is_some() {
            return Err(OpenPageFileError::EncryptionStatusMismatch);
        }

        Ok(Self::new(
            ctx,
            file,
            header.page_file_id,
            file_metadata::HEADER_SIZE as u64,
        ))
    }

    /// Create a new page file.
    pub async fn create(
        ctx: Arc<ctx::FileContext>,
        file: file::RWFile,
        id: PageFileId,
    ) -> Result<Self, CreatePageFileError> {
        let mut buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();

        let header_associated_data =
            file_metadata::header_associated_data(file.id(), FileGroup::Pages);

        let header = super::MetadataHeader {
            file_id: file.id(),
            page_file_id: id,
            encryption: ctx.get_encryption_status(),
            max_num_pages: MAX_NUM_PAGES,
        };
        file_metadata::encode_metadata(
            ctx.cipher(),
            &header_associated_data,
            &header,
            &mut buffer[..file_metadata::HEADER_SIZE],
        )?;

        file.write_buffer(&mut buffer, 0).await?;

        Ok(Self::new(ctx, file, id, file_metadata::HEADER_SIZE as u64))
    }

    fn new(
        ctx: Arc<ctx::FileContext>,
        file: file::RWFile,
        id: PageFileId,
        data_offset: u64,
    ) -> Self {
        Self {
            id,
            ctx,
            file,
            data_offset,
        }
    }

    /// Read a set of pages from the page file.
    ///
    /// Reads _may_ be sparse providing the buffer is the long enough to read
    /// all the data (including the missed pages.)
    ///
    /// This will avoid needlessly decrypting and verifying unused pages.
    ///
    /// Unlike `submit_write_at`, `read_at` will block (asynchronously) until teh read is complete.
    pub async fn read_at(
        &self,
        page_metadata_entries: &[PageMetadata],
        mut buffer: DmaBuffer,
    ) -> Result<DmaBuffer, ReadPageError> {
        let num_contiguous_pages =
            validate_read_metadata_entries(page_metadata_entries, buffer.len())?;
        let buffer_len = num_contiguous_pages as usize * DISK_PAGE_SIZE;

        // We know the metadata entries are sorted because of the loop check we just did
        // and, we know there are at least 1 metadata entries in the set from our first check.
        let first_page_id = page_metadata_entries.first().unwrap().id;
        assert!(
            first_page_id.0 < MAX_NUM_PAGES as u32,
            "BUG: page ID is beyond the bounds of the page file",
        );

        let offset = self.resolve_pos(first_page_id);
        let buffer_ptr = buffer.as_mut_ptr();
        let guard = buffer.share_guard();
        dbg!(buffer_len, offset);

        let reply = unsafe {
            self.file
                .submit_read(buffer_ptr, buffer_len, offset, Some(guard as DynamicGuard))
                .await?
        };

        let result = file::wait_for_reply(reply).await?;
        if result < buffer_len {
            return Err(ReadPageError::ShortRead);
        }

        let context = super::utils::copy_sparse_metadata_context(page_metadata_entries);
        // Used to avoid needless work verifying pages
        let mask = super::utils::make_metadata_density_mask(page_metadata_entries);

        let buffer = self
            .decode_page_data(first_page_id, num_contiguous_pages, context, mask, buffer)
            .await
            .expect("spawn blocking task failed")?;

        Ok(buffer)
    }

    /// Write the buffer (potentially containing multiple pages) to the page file
    /// and update the page metadata context.
    ///
    /// NOTE: All pages specified must be contiguous to one another, no sparse write
    /// is allowed.
    pub async fn submit_write_at(
        &self,
        page_metadata_entries: &mut [PageMetadata],
        buffer: DmaBuffer,
    ) -> Result<i2o2::ReplyReceiver, SubmitWriterError> {
        validate_write_metadata_entries(page_metadata_entries, buffer.len())?;

        let read_len = page_metadata_entries.len() * DISK_PAGE_SIZE;

        // We know the metadata entries are sorted because of the loop check we just did
        // and, we know there are at least 1 metadata entries in the set from our first check.
        let first_page_id = page_metadata_entries.first().unwrap().id;

        let (mut buffer, context_block) = self
            .encode_page_data(first_page_id, page_metadata_entries.len(), buffer)
            .await
            .expect("spawn blocking task failed")?;

        for (page_offset, metadata) in page_metadata_entries.iter_mut().enumerate() {
            let ctx_start = page_offset * 40;
            let ctx_end = ctx_start + 40;

            let context = &context_block[ctx_start..ctx_end];
            metadata.context.copy_from_slice(context);
        }

        let offset = self.resolve_pos(first_page_id);
        let buffer_ptr = buffer.as_mut_ptr();

        let reply = unsafe {
            self.file
                .submit_write(
                    buffer_ptr,
                    read_len,
                    offset,
                    Some(Arc::new(buffer) as DynamicGuard),
                )
                .await?
        };

        Ok(reply)
    }

    /// Encodes the provided page buffer data and either checksums or encrypts the buffers
    /// returning the updates context data.
    ///
    /// This runs the encode task in a worker thread to prevent blocking as the
    /// encode routine is slight too slow to be able to use without causing some issues
    /// with the tokio scheduler.
    fn encode_page_data(
        &self,
        first_page_id: PageId,
        num_metadata_entries: usize,
        mut buffer: DmaBuffer,
    ) -> tokio::task::JoinHandle<
        Result<(DmaBuffer, [u8; CONTEXT_BUFFER_SIZE]), encrypt::EncryptError>,
    > {
        let page_file_id = self.id();
        let file_id = self.file_id();
        let ctx = self.ctx.clone();

        tokio::task::spawn_blocking(move || {
            let mut context = [0; CONTEXT_BUFFER_SIZE];

            for page_offset in 0..num_metadata_entries {
                let page_id = PageId(first_page_id.0 + page_offset as u32);

                let ctx_start = page_offset * 40;
                let ctx_end = ctx_start + 40;

                let data_start = page_offset * DISK_PAGE_SIZE;
                let data_end = data_start + DISK_PAGE_SIZE;

                let associated_data =
                    page_associated_data(file_id, page_file_id, page_id);

                super::encode::encode_page_data(
                    ctx.cipher(),
                    &associated_data,
                    &mut buffer[data_start..data_end],
                    &mut context[ctx_start..ctx_end],
                )?;
            }

            Ok((buffer, context))
        })
    }

    /// Decodes the provided page buffer data and validates the integrity of the buffer
    /// or decrypted the data (depending on the configured context.)
    ///
    /// This runs the decode task in a worker thread to prevent blocking as the
    /// decode routine is slight too slow to be able to use without causing some issues
    /// with the tokio scheduler.
    fn decode_page_data(
        &self,
        first_page_id: PageId,
        num_metadata_entries: u8,
        context: [u8; CONTEXT_BUFFER_SIZE],
        decode_bitmask: u8,
        mut buffer: DmaBuffer,
    ) -> tokio::task::JoinHandle<Result<DmaBuffer, ReadPageError>> {
        let file_id = self.file_id();
        let page_file_id = self.id();
        let ctx = self.ctx.clone();

        tokio::task::spawn_blocking(move || {
            for page_offset in 0..num_metadata_entries {
                if decode_bitmask & (1 << page_offset) == 0 {
                    continue;
                }

                let page_id = PageId(first_page_id.0 + page_offset as u32);

                let ctx_start = page_offset as usize * 40;
                let ctx_end = ctx_start + 40;

                let data_start = page_offset as usize * DISK_PAGE_SIZE;
                let data_end = data_start + DISK_PAGE_SIZE;

                let associated_data =
                    page_associated_data(file_id, page_file_id, page_id);

                super::encode::decode_page_data(
                    ctx.cipher(),
                    &associated_data,
                    &mut buffer[data_start..data_end],
                    &context[ctx_start..ctx_end],
                )?;
            }

            Ok(buffer)
        })
    }

    fn resolve_pos(&self, page_id: PageId) -> u64 {
        let relative_position = page_id.0 as u64 * DISK_PAGE_SIZE as u64;
        dbg!(relative_position);
        relative_position + self.data_offset
    }
}

fn validate_write_metadata_entries(
    metadata: &[PageMetadata],
    buffer_len: usize,
) -> io::Result<()> {
    if metadata.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty metadata entries",
        ));
    } else if metadata.len() > MAX_SINGLE_IOP_NUM_PAGES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "iop size too large",
        ));
    }

    let first_page_id = metadata.first().unwrap().id;
    let last_page_id = metadata.last().unwrap().id;
    assert!(
        first_page_id.0 < MAX_NUM_PAGES as u32 && last_page_id.0 < MAX_NUM_PAGES as u32,
        "BUG: page ID is beyond the bounds of the page file",
    );

    let expected_len = metadata.len() * DISK_PAGE_SIZE;
    if buffer_len < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provided buffer too small",
        ));
    }

    if !super::utils::metadata_is_contiguous(metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provided page range is not contiguous",
        ));
    }

    Ok(())
}

fn validate_read_metadata_entries(
    metadata: &[PageMetadata],
    buffer_len: usize,
) -> io::Result<u8> {
    if metadata.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty metadata entries",
        ));
    } else if !metadata.is_sorted_by_key(|p| p.id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "metadata entries not sorted by page ID",
        ));
    }

    let first_page_id = metadata.first().unwrap().id;
    let last_page_id = metadata.last().unwrap().id;
    let num_pages = (last_page_id.0 - first_page_id.0) as usize + 1;

    assert!(
        first_page_id.0 < MAX_NUM_PAGES as u32 && last_page_id.0 < MAX_NUM_PAGES as u32,
        "BUG: page ID is beyond the bounds of the page file",
    );

    if num_pages > MAX_SINGLE_IOP_NUM_PAGES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "iop size too large",
        ));
    }

    let expected_len = num_pages * DISK_PAGE_SIZE;
    if buffer_len < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provided buffer too small",
        ));
    }

    assert!(num_pages <= 8);
    Ok(num_pages as u8)
}
