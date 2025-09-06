use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::buffer::DmaBuffer;
use crate::directory::FileGroup;
use crate::file::DynamicGuard;
use crate::layout::file_metadata::Encryption;
use crate::layout::{PageFileId, PageId, file_metadata};
use crate::page_data::{DEFAULT_PAGE_SIZE, MAX_NUM_PAGES};
use crate::{ctx, file};

const SYNC_COALESCE_DURATION: Duration = Duration::from_millis(1);

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

/// The page file contains blocks of data called "pages".
pub struct PageFile {
    id: PageFileId,
    ctx: Arc<ctx::FileContext>,
    file: file::RWFile,
    data_offset: u64,

    /// A counter that is incremented for every sync operation.
    ///
    /// This is used to coalesce syncs of the file.
    sync_counter: Arc<AtomicU64>,
    /// A mutex that much be acquired before a sync can take place.
    ///
    /// The inner value contains the counter value that has been flushed.
    sync_guard: tokio::sync::Mutex<u64>,
}

impl PageFile {
    /// Returns the ID of the page file.
    pub fn id(&self) -> PageFileId {
        self.id
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
        file.fdatasync().await?;

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
            sync_counter: Arc::new(AtomicU64::new(1)),
            sync_guard: tokio::sync::Mutex::new(0),
        }
    }

    /// Read a set of pages from the page file.
    ///
    /// The read will begin at `page_id` and continue on for upto the length of the
    /// provided buffer or until EOF, which ever comes first.
    ///
    /// NOTE: This does _NOT_ decrypt the data of the pages if encryption at rest is enabled.
    pub async fn submit_read_at(
        &self,
        page_id: PageId,
        buffer: &mut DmaBuffer,
    ) -> io::Result<i2o2::ReplyReceiver> {
        let offset = self.resolve_pos(page_id);

        let buffer_ptr = buffer.as_mut_ptr();
        let buffer_len = buffer.len();
        let guard = buffer.share_guard();

        let reply = unsafe {
            self.file
                .submit_read(buffer_ptr, buffer_len, offset, Some(guard as DynamicGuard))
                .await?
        };

        Ok(reply)
    }

    /// Write the buffer starting at the given page ID.
    ///
    /// WARNING: This can overwrite other pages.
    ///
    /// NOTE: This does _NOT_ encrypt the data of the pages if encryption at rest is enabled.
    pub async fn submit_write_at(
        &self,
        page_id: PageId,
        buffer: &mut DmaBuffer,
        write_len: usize,
    ) -> io::Result<i2o2::ReplyReceiver> {
        assert!(
            page_id.0 < MAX_NUM_PAGES as u32,
            "page ID is beyond the bounds of the page file",
        );
        assert!(
            write_len <= buffer.len(),
            "write len must be less than or equal to buffer len"
        );

        let offset = self.resolve_pos(page_id);

        let buffer_ptr = buffer.as_mut_ptr();
        let guard = buffer.share_guard();

        let reply = unsafe {
            self.file
                .submit_write(buffer_ptr, write_len, offset, Some(guard as DynamicGuard))
                .await?
        };

        Ok(reply)
    }

    fn resolve_pos(&self, page_id: PageId) -> u64 {
        let relative_position = page_id.0 as u64 * DEFAULT_PAGE_SIZE as u64;
        relative_position + self.data_offset
    }

    /// Wait for all changes to be persisted safely on the underlying storage.
    pub async fn sync(&self) -> io::Result<u64> {
        // We get increment and get the sync counter, this is the oldest stamp that we will
        // accept for a flush operation.
        //
        // This works because our `sync_counter` is incremented just before
        // we issue the fdatasync request, meaning we can tell if our current
        // operations were part of the most recent flush or not.
        let oldest_stamp = self.sync_counter.fetch_add(1, Ordering::Relaxed);

        let mut lock = self.sync_guard.lock().await;
        if *lock >= oldest_stamp {
            return Ok(*lock);
        }

        // We sacrifice some latency in order to improve the overall efficiency of the system.
        tokio::time::sleep(SYNC_COALESCE_DURATION).await;

        let op_stamp = self.sync_counter.fetch_add(1, Ordering::Relaxed);
        let result = self.file.fdatasync().await;
        *lock = op_stamp;
        result.map(|_| op_stamp)
    }
}
