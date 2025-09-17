use std::collections::BTreeMap;
use std::sync::Arc;

use foldhash::HashMapExt;

use super::metadata::{LookupEntry, PageTable};
use crate::checkpoint::{
    Checkpoint,
    ReadCheckpointError,
    WriteCheckpointError,
    read_checkpoint,
    write_checkpoint,
};
use crate::ctx;
use crate::directory::{FileGroup, FileId};
use crate::layout::page_metadata::{PageChangeCheckpoint, PageMetadata};
use crate::layout::{PageFileId, PageGroupId};
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
pub async fn read_checkpoints(
    ctx: Arc<ctx::FileContext>,
) -> Result<LastCheckpointedState, ReadCheckpointError> {
    use std::collections::btree_map::Entry;

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

/// Reconstructs the lookup table for page groups.
///
/// This assumes that page sequences are written from the smallest ID to the largest ID
/// in order to maintain ordering of data.
fn reconstruct_lookup_table_from_pages(
    lookup_table: &mut foldhash::HashMap<PageGroupId, LookupEntry>,
    page_file_id: PageFileId,
    pages: &[PageMetadata],
) {
    use std::collections::hash_map::Entry;

    for page in pages {
        // There should not be any empty pages, but just in case.
        if page.is_empty() {
            continue;
        }

        let lookup_entry = LookupEntry {
            page_file_id,
            first_page_id: page.id,
            revision: page.revision,
        };

        match lookup_table.entry(page.group) {
            Entry::Vacant(entry) => {
                entry.insert(lookup_entry);
            },
            Entry::Occupied(mut entry) => {
                let existing_page = entry.get();

                // - If the new page has a newer revision, use that.
                // - If the page revisions match, but we have a lower page ID we assign that.
                if page.revision > existing_page.revision
                    || (page.revision == existing_page.revision
                        && page.id < existing_page.first_page_id)
                {
                    entry.insert(lookup_entry);
                }
            },
        }
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::layout::PageId;

    #[test]
    fn test_reconstruct_lookup_table_from_pages() {
        let pages = &[
            PageMetadata::empty(),
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
                        first_page_id: PageId(3),
                        revision: 0
                    }
                ),
                (
                    PageGroupId(3),
                    LookupEntry {
                        page_file_id: PageFileId(1),
                        first_page_id: PageId(5),
                        revision: 2
                    }
                ),
                (
                    PageGroupId(5),
                    LookupEntry {
                        page_file_id: PageFileId(1),
                        first_page_id: PageId(1),
                        revision: 0
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
            reconstruct_lookup_table_from_pages(
                black_box(PageFileId(0)),
                black_box(&entries),
            )
        });

        Ok(())
    }
}
