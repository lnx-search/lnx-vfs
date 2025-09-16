use std::sync::Arc;

use super::metadata::PageTable;
use crate::checkpoint::{WriteCheckpointError, write_checkpoint};
use crate::ctx;
use crate::directory::{FileGroup, FileId};
use crate::layout::PageFileId;
use crate::layout::page_metadata::PageChangeCheckpoint;
use crate::page_data::NUM_PAGES_PER_BLOCK;

/// Checkpoint the target page table if any in-memory state has changed.
pub async fn checkpoint_page_table(
    ctx: Arc<ctx::FileContext>,
    page_file_id: PageFileId,
    page_table: &PageTable,
) -> Result<FileId, WriteCheckpointError> {
    #[cfg(test)]
    fail::fail_point!("checkpoint::checkpoint_page_table", |_| Err(
        WriteCheckpointError::IO(std::io::Error::other(
            "checkpoint_page_table fail point error"
        ))
    ));

    let op_stamp = page_table.get_current_op_stamp();

    let mut non_empty_pages = PageChangeCheckpoint::with_capacity(NUM_PAGES_PER_BLOCK);
    page_table.collect_non_empty_pages(&mut non_empty_pages);

    let directory = ctx.directory();
    let file_id = directory
        .create_new_atomic_file(FileGroup::Metadata)
        .await?;
    let file = directory.get_rw_file(FileGroup::Metadata, file_id).await?;

    write_checkpoint(&ctx, &file, page_file_id, non_empty_pages).await?;

    directory
        .persist_atomic_file(FileGroup::Metadata, file_id)
        .await?;

    // Once it is safely persisted, we update the memory checkpoint.
    page_table.checkpoint(op_stamp);

    Ok(file_id)
}
