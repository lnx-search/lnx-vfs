use std::collections::BTreeMap;
use std::sync::Arc;

use foldhash::HashMapExt;

use super::metadata::{LookupEntry, MetadataController, PageTable};
use crate::checkpoint::{
    ReadCheckpointError,
    WriteCheckpointError,
    read_checkpoint,
    write_checkpoint,
};
use crate::directory::{FileGroup, FileId};
use crate::layout::log::LogOp;
use crate::layout::page_metadata::{PageChangeCheckpoint, PageMetadata};
use crate::layout::{PageFileId, PageGroupId};
use crate::page_data::NUM_PAGES_PER_BLOCK;
use crate::{ctx, page_op_log};

#[tracing::instrument(skip(ctx, page_table))]
/// Checkpoint the target page table if any in-memory state has changed.
pub(super) async fn checkpoint_page_table(
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

    let mut assigned_pages = PageChangeCheckpoint::with_capacity(NUM_PAGES_PER_BLOCK);
    page_table.collect_assigned_pages(&mut assigned_pages);
    let num_allocated_pages = assigned_pages.len();

    let directory = ctx.directory();
    let file_id = directory
        .create_new_atomic_file(FileGroup::Metadata)
        .await?;
    let file = directory.get_rw_file(FileGroup::Metadata, file_id).await?;

    write_checkpoint(&ctx, &file, page_file_id, assigned_pages).await?;

    directory
        .persist_atomic_file(FileGroup::Metadata, file_id)
        .await?;

    // Once it is safely persisted, we update the memory checkpoint.
    page_table.checkpoint(op_stamp);

    tracing::info!(
        file_id = ?file_id,
        checkpoint_op_stamp = op_stamp,
        num_allocated_pages = num_allocated_pages,
        "checkpointed page table",
    );

    Ok(file_id)
}

/// The state of the metadata controller after the last successful checkpoint.
pub struct LastCheckpointedState {
    /// The page group lookup table.
    pub lookup_table: foldhash::HashMap<PageGroupId, LookupEntry>,
    /// The recovered page tables.
    pub page_tables: BTreeMap<PageFileId, PageTable>,
}

#[tracing::instrument(skip(ctx))]
/// Read all persisted page tables from their checkpoints.
///
/// This does NOT recover any additional state from the WAL.
pub(super) async fn read_checkpoints(
    ctx: Arc<ctx::FileContext>,
) -> Result<LastCheckpointedState, ReadCheckpointError> {
    use std::collections::btree_map::Entry;

    let directory = ctx.directory();
    let file_ids = directory.list_dir(FileGroup::Metadata).await;

    let mut active_checkpoints = BTreeMap::new();
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

                // Checkpoint files can be replayed in the order of their file ID assignment as
                // the next checkpoint will always have a file ID larger than the preceding one
                // it is taking over from.
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

    let mut lookup_table = foldhash::HashMap::with_capacity(1_000);
    let mut page_tables = BTreeMap::new();
    for (page_file_id, (_, checkpoint)) in active_checkpoints {
        tracing::info!(
            page_file_id = ?page_file_id,
            num_allocated_pages = checkpoint.updates.len(),
            "loading page table",
        );

        reconstruct_lookup_table_from_pages(
            &mut lookup_table,
            page_file_id,
            &checkpoint.updates,
        );

        let page_table = PageTable::from_existing_state(&checkpoint.updates);
        page_tables.insert(page_file_id, page_table);
    }

    for file_id in files_to_cleanup {
        if let Err(err) = directory.remove_file(FileGroup::Metadata, file_id).await {
            tracing::error!(error = %err, "failed to remove old checkpoint file");
        }
    }

    Ok(LastCheckpointedState {
        lookup_table,
        page_tables,
    })
}

#[derive(Debug, thiserror::Error)]
/// An error preventing the system from recovering the WAL.
pub enum RecoverWalError {
    #[error("IO error: {0}")]
    /// An IO error occurred.
    IO(#[from] std::io::Error),
    #[error(transparent)]
    /// The system could not open the WAL for reading.
    OpenReaderError(#[from] page_op_log::LogOpenReadError),
}

#[tracing::instrument(skip_all)]
/// Recover any additional updates to the metadata state which was not captured
/// in the last checkpoint.
///
/// This reads each existing WAL log file until an error is encountered.
/// We assume an error means the end of the log due to the layout of the WAL
/// ensuring torn writes should not be possible in any situation.
pub(super) async fn recover_wal_updates(
    ctx: Arc<ctx::FileContext>,
    controller: &MetadataController,
) -> Result<(), RecoverWalError> {
    let directory = ctx.directory();
    let file_ids = directory.list_dir(FileGroup::Wal).await;

    let mut readers = Vec::with_capacity(file_ids.len());
    for file_id in file_ids {
        let file = directory.get_ro_file(FileGroup::Wal, file_id).await?;
        let reader = page_op_log::LogFileReader::open(ctx.clone(), file).await?;
        readers.push(reader);
    }
    // Sort the WAL files in order of their creation/initialisation
    // to ensure events are replayed correctly.
    readers.sort_by_key(|reader| reader.order_key());

    let mut num_transactions_recovered = 0;
    for reader in readers {
        let num_transactions = recover_wal_file(reader, controller).await?;
        num_transactions_recovered += num_transactions;
    }

    tracing::info!(
        num_transactions_recovered = num_transactions_recovered,
        "recovered WAL file changes"
    );

    Ok(())
}

#[tracing::instrument(skip_all, fields(file_id = ?reader.file_id()))]
async fn recover_wal_file(
    mut reader: page_op_log::LogFileReader,
    controller: &MetadataController,
) -> Result<usize, RecoverWalError> {
    tracing::info!("reading WAL file");

    let mut transaction_ops = Vec::with_capacity(100);
    let mut num_transactions_recovered = 0;
    let mut last_transaction_id = 0;
    loop {
        transaction_ops.clear();

        let transaction_id = match reader.next_transaction(&mut transaction_ops).await {
            Ok(Some(txn_id)) => txn_id,
            Ok(None) => break,
            Err(err) => {
                // NOTE: This does not mean something is wrong, this is a normal branch to hit
                //       because old blocks will fail to decode when we recycle the log, but
                //       the system will attempt to decode it anyway.
                tracing::debug!(error = ?err, "encountered error decoding block, terminating");
                break;
            },
        };
        num_transactions_recovered += 1;

        assert!(
            last_transaction_id < transaction_id,
            "BUG: last transaction ID ({last_transaction_id}) was not smaller than the \
            next transaction ID ({transaction_id}), this means the writer has lost track \
            of the order of operations within the WAL."
        );
        last_transaction_id = transaction_id;

        for op in transaction_ops.drain(..) {
            match op {
                LogOp::Write(op) => {
                    // When recovering, we might have a brand-new page file that hasn't
                    // had any data written to it from a checkpoint yet, so we should
                    // recreate it.
                    if !controller.contains_page_table(op.page_file_id) {
                        controller.create_blank_page_table(op.page_file_id);
                    }

                    controller.assign_pages_to_group(
                        op.page_file_id,
                        op.page_group_id,
                        &op.altered_pages,
                    );
                },
                LogOp::Free(op) => {
                    controller.unassign_pages_in_group(op.page_group_id);
                },
                LogOp::Reassign(op) => {
                    controller
                        .reassign_pages(op.old_page_group_id, op.new_page_group_id);
                },
            }
        }
    }

    Ok(num_transactions_recovered)
}

/// Reconstructs the lookup table for page groups.
///
/// This assumes that page sequences are written from the smallest ID to the largest ID
/// in order to maintain ordering of data.
///
/// It _also_ assumes that pages assigned to groups are performed from the smallest allocated
/// ID to the largest ID.
///
/// Reconstruction of the lookup table is done by doing a first-write-wins strategy on the
/// hashmap.
fn reconstruct_lookup_table_from_pages(
    lookup_table: &mut foldhash::HashMap<PageGroupId, LookupEntry>,
    page_file_id: PageFileId,
    pages: &[PageMetadata],
) {
    for page in pages {
        // There should not be any empty pages, but just in case.
        if page.is_unassigned() {
            continue;
        }

        let lookup_entry = LookupEntry {
            page_file_id,
            first_page_id: page.id,
        };

        lookup_table.entry(page.group).or_insert(lookup_entry);
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::layout::PageId;

    #[test]
    fn test_reconstruct_lookup_table_from_pages() {
        let pages = &[
            PageMetadata::null(),
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(5),
                id: PageId(4),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(5),
                revision: 0,
                next_page_id: PageId(6),
                id: PageId(1),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(4),
                id: PageId(3),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(3),
                revision: 0,
                next_page_id: PageId(9),
                id: PageId(2),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(3),
                revision: 2,
                next_page_id: PageId(9),
                id: PageId(5),
                data_len: 0,
                context: [0; 40],
            },
        ];

        let mut lookup = foldhash::HashMap::new();
        reconstruct_lookup_table_from_pages(&mut lookup, PageFileId(1), pages);

        let mut lookup = lookup.into_iter().collect::<Vec<_>>();
        lookup.sort_by_key(|(id, _)| *id);

        assert_eq!(
            lookup,
            &[
                (
                    PageGroupId(1),
                    LookupEntry {
                        page_file_id: PageFileId(1),
                        first_page_id: PageId(4),
                    }
                ),
                (
                    PageGroupId(3),
                    LookupEntry {
                        page_file_id: PageFileId(1),
                        first_page_id: PageId(2),
                    }
                ),
                (
                    PageGroupId(5),
                    LookupEntry {
                        page_file_id: PageFileId(1),
                        first_page_id: PageId(1),
                    }
                ),
            ],
        );
    }
}

#[cfg(all(test, not(feature = "test-miri"), feature = "bench-lib-unstable"))]
mod benches {
    extern crate test;

    use std::hint::black_box;

    use super::*;
    use crate::layout::PageId;
    use crate::page_data::NUM_BLOCKS_PER_FILE;

    #[bench]
    fn reconstruct_lookup_table(bencher: &mut test::Bencher) -> anyhow::Result<()> {
        let gen_page_id = || {
            PageId(fastrand::u32(
                0..(NUM_BLOCKS_PER_FILE * NUM_PAGES_PER_BLOCK) as u32,
            ))
        };
        let mut entries = Vec::with_capacity(NUM_BLOCKS_PER_FILE * NUM_PAGES_PER_BLOCK);
        for _ in 0..NUM_BLOCKS_PER_FILE * NUM_PAGES_PER_BLOCK {
            entries.push(PageMetadata {
                group: PageGroupId(fastrand::u64(0..10_000)),
                revision: fastrand::u32(0..50),
                next_page_id: gen_page_id(),
                id: gen_page_id(),
                data_len: 0,
                context: [0; 40],
            });
        }

        bencher.iter(|| {
            let mut lookup_table = foldhash::HashMap::default();
            reconstruct_lookup_table_from_pages(
                black_box(&mut lookup_table),
                black_box(PageFileId(0)),
                black_box(&entries),
            )
        });

        Ok(())
    }
}
