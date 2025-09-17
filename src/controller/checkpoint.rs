use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use super::metadata::PageTable;
use crate::checkpoint::{
    Checkpoint,
    ReadCheckpointError,
    WriteCheckpointError,
    read_checkpoint,
    write_checkpoint,
};
use crate::ctx;
use crate::directory::{FileGroup, FileId};
use crate::layout::PageFileId;
use crate::layout::page_metadata::PageChangeCheckpoint;
use crate::page_data::NUM_PAGES_PER_BLOCK;

#[tracing::instrument(skip(ctx, page_table))]
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

    tracing::info!(checkpoint_op_stamp = op_stamp, "checkpointed page table");

    Ok(file_id)
}

#[tracing::instrument(skip(ctx))]
/// Read all persisted page tables from their checkpoints.
///
/// This does NOT recover any additional state from the WAL.
pub async fn read_checkpoints(
    ctx: Arc<ctx::FileContext>,
) -> Result<BTreeMap<PageFileId, PageTable>, ReadCheckpointError> {
    let directory = ctx.directory();
    let file_ids = directory.list_dir(FileGroup::Metadata).await;

    let mut active_checkpoints: BTreeMap<PageFileId, (FileId, Checkpoint)> =
        BTreeMap::new();
    let mut files_to_cleanup = Vec::new();
    for file_id in file_ids {
        let file = directory.get_ro_file(FileGroup::Metadata, file_id).await?;

        let checkpoint = read_checkpoint(&ctx, &file).await?;

        match active_checkpoints.entry(checkpoint.page_file_id) {
            Entry::Vacant(entry) => {
                entry.insert((file_id, checkpoint));
            },
            Entry::Occupied(mut entry) => {
                let &(existing_file_id, _) = entry.get();
                if file_id > existing_file_id {
                    entry.insert((file_id, checkpoint));
                    files_to_cleanup.push(existing_file_id);
                }
            },
        }
    }

    tracing::info!(
        num_active_checkpoints = active_checkpoints.len(),
        files_to_cleanup = ?files_to_cleanup,
        "collected active checkpoints"
    );

    let mut page_tables = BTreeMap::new();
    for (page_file_id, (_, checkpoint)) in active_checkpoints {
        tracing::info!(
            page_file_id = ?page_file_id,
            num_allocated_pages = checkpoint.updates.len(),
            "loading page table",
        );

        let page_table = PageTable::from_existing_state(&checkpoint.updates);
        page_tables.insert(page_file_id, page_table);
    }

    for file_id in files_to_cleanup {
        if let Err(err) = directory.remove_file(FileGroup::Metadata, file_id).await {
            tracing::error!(error = %err, "failed to remove old checkpoint file");
        }
    }

    Ok(page_tables)
}
