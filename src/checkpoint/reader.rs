use std::io;
use std::sync::Arc;

use crate::buffer::ALLOC_PAGE_SIZE;
use crate::directory::FileGroup;
use crate::layout::file_metadata::Encryption;
use crate::layout::{file_metadata, page_metadata};
use crate::{ctx, file};

#[derive(Debug, thiserror::Error)]
/// An error that prevent the reader from decoding the checkpoint.
pub enum ReadCheckpointError {
    #[error("missing metadata header")]
    /// The file is missing the required metadata header.
    MissingHeader,
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The file metadata decoder could not deserialize the header.
    HeaderDecode(#[from] file_metadata::DecodeError),
    #[error(transparent)]
    /// The checkpoint decoder could not deserialize the checkpoint.
    CheckpointDecode(#[from] page_metadata::DecodeError),
    #[error("file is not encrypted but system has encryption enabled")]
    /// The encryption of the file does not align with the current context.
    ///
    /// This means the file is marked as not being encrypted but the system
    /// is configured for encryption.
    EncryptionStatusMismatch,
}

/// Read a persisted metadata checkpoint.
pub async fn read_checkpoint(
    ctx: &Arc<ctx::FileContext>,
    file: &file::ROFile,
) -> Result<page_metadata::PageChangeCheckpoint, ReadCheckpointError> {
    let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
    let n = file.read_buffer(&mut header_buffer, 0).await?;
    if n == 0 {
        return Err(ReadCheckpointError::MissingHeader);
    }

    let header_associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Metadata);

    let header: super::MetadataHeader = file_metadata::decode_metadata(
        ctx.cipher(),
        &header_associated_data,
        &mut header_buffer[..file_metadata::HEADER_SIZE],
    )?;
    drop(header_buffer);

    // The system will not even open if encryption is enabled and the system
    // is not setup for encryption.
    if header.encryption == Encryption::Disabled && ctx.cipher().is_some() {
        return Err(ReadCheckpointError::EncryptionStatusMismatch);
    }

    let ckpt_associated_data = super::ckpt_associated_data(
        file.id(),
        header.parent_page_file_id,
        header.checkpoint_num_changes,
        file_metadata::HEADER_SIZE as u64,
    );

    let num_pages = header.checkpoint_buffer_size.div_ceil(ALLOC_PAGE_SIZE);
    let mut checkpoint_buffer = ctx.alloc_pages(num_pages);
    file.read_buffer(&mut checkpoint_buffer, file_metadata::HEADER_SIZE as u64)
        .await?;

    let checkpoint = tokio::task::spawn_blocking({
        let ctx = ctx.clone();
        move || {
            page_metadata::decode_page_metadata_changes(
                ctx.cipher(),
                &ckpt_associated_data,
                &mut checkpoint_buffer[..header.checkpoint_buffer_size],
            )
        }
    })
    .await
    .expect("spawn worker thread")?;

    Ok(checkpoint)
}
