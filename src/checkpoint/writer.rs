use std::io;

use crate::buffer::ALLOC_PAGE_SIZE;
use crate::directory::FileGroup;
use crate::layout::{PageFileId, file_metadata, page_metadata};
use crate::{ctx, file};

#[derive(Debug, thiserror::Error)]
/// An error that prevent the writer from persisting the checkpoint.
pub enum WriteCheckpointError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The file metadata encoder could not serialize the header.
    HeaderEncode(#[from] file_metadata::EncodeError),
    #[error(transparent)]
    /// The checkpoint encoder could not serialize the checkpoint.
    CheckpointEncode(#[from] page_metadata::EncodeError),
}

/// Writes a new [page_metadata::PageChangeCheckpoint] to the target file.
///
/// The file will automatically be synced to ensure data is persisted.
pub async fn write_checkpoint(
    ctx: &ctx::FileContext,
    file: &file::RWFile,
    page_file_id: PageFileId,
    changes: &page_metadata::PageChangeCheckpoint,
) -> Result<(), WriteCheckpointError> {
    let ckpt_associated_data = super::ckpt_associated_data(
        file.id(),
        page_file_id,
        file_metadata::HEADER_SIZE as u64,
    );

    let changes_buffer = page_metadata::encode_page_metadata_changes(
        ctx.cipher(),
        &ckpt_associated_data,
        changes,
    )?;

    let num_pages =
        (file_metadata::HEADER_SIZE + changes_buffer.len()).div_ceil(ALLOC_PAGE_SIZE);
    let mut buffer = ctx.alloc_pages(num_pages);
    buffer[file_metadata::HEADER_SIZE..][..changes_buffer.len()]
        .copy_from_slice(&changes_buffer);

    let header_associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Metadata);

    let header = super::MetadataHeader {
        file_id: file.id(),
        parent_page_file_id: page_file_id,
        encryption: ctx.get_encryption_status(),
        checkpoint_buffer_size: changes_buffer.len(),
    };
    file_metadata::encode_metadata(
        ctx.cipher(),
        &header_associated_data,
        &header,
        &mut buffer[..file_metadata::HEADER_SIZE],
    )?;

    file.write_buffer(&mut buffer, 0).await?;
    file.fdatasync().await?;

    Ok(())
}
