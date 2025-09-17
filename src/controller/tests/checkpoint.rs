use crate::checkpoint::read_checkpoint;
use crate::controller::checkpoint::{checkpoint_page_table, read_checkpoints};
use crate::controller::metadata::PageTable;
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::NUM_PAGES_PER_BLOCK;

#[rstest::rstest]
#[case::empty_table(&[])]
#[case::single_entry(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[case::multiple_blocks(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId((NUM_PAGES_PER_BLOCK + 4) as u32),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[trace]
#[tokio::test]
async fn test_file_save(#[case] entries: &[PageMetadata]) {
    let ctx = ctx::FileContext::for_test(false).await;

    let page_table = PageTable::from_existing_state(entries);

    let file_id = checkpoint_page_table(ctx.clone(), PageFileId(0), &page_table)
        .await
        .expect("Checkpoint page table failed");

    let file = ctx
        .directory()
        .get_ro_file(FileGroup::Metadata, file_id)
        .await
        .unwrap();

    let checkpoint = read_checkpoint(&ctx, &file)
        .await
        .expect("read checkpoint file");
    assert_eq!(checkpoint.updates.as_slice(), entries);
}

#[tokio::test]
async fn test_page_table_checkpointed_post_write() {
    let ctx = ctx::FileContext::for_test(false).await;

    let page_table = PageTable::default();
    page_table.write_pages(&[PageMetadata {
        group: PageGroupId(1),
        revision: 0,
        next_page_id: PageId(1),
        id: PageId(4),
        data_len: 0,
        context: [0; 40],
    }]);
    assert!(page_table.has_changed());

    checkpoint_page_table(ctx.clone(), PageFileId(0), &page_table)
        .await
        .expect("Checkpoint page table failed");

    // checkpoint_page_table should update the memory checkpoint.
    assert!(!page_table.has_changed());
}

#[tokio::test]
async fn test_page_table_load_from_checkpoints() {
    let ctx = ctx::FileContext::for_test(false).await;

    let pages = &[PageMetadata {
        group: PageGroupId(1),
        revision: 0,
        next_page_id: PageId(1),
        id: PageId(4),
        data_len: 0,
        context: [0; 40],
    }];

    let page_table = PageTable::default();
    page_table.write_pages(pages);
    assert!(page_table.has_changed());

    checkpoint_page_table(ctx.clone(), PageFileId(0), &page_table)
        .await
        .expect("Checkpoint page table failed");

    let page_tables = read_checkpoints(ctx)
        .await
        .expect("all checkpoints should be loaded");
    assert_eq!(page_tables.len(), 1);

    let page_table = page_tables
        .get(&PageFileId(0))
        .expect("checkpoint page table should exist");
    assert!(!page_table.has_changed());

    let mut collected_pages = Vec::new();
    page_table.collect_non_empty_pages(&mut collected_pages);
    assert_eq!(collected_pages, pages);
}
