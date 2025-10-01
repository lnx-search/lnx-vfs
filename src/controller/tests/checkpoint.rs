use crate::checkpoint::read_checkpoint;
use crate::controller::checkpoint::{
    checkpoint_page_table,
    read_checkpoints,
    recover_wal_updates,
};
use crate::controller::metadata::{MetadataController, PageTable};
use crate::controller::tests::{create_wal_file, write_log_ops};
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::log::{FreeOp, LogOp, ReassignOp, WriteOp};
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

    let file_id = checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
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

    checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
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

    checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
        .await
        .expect("Checkpoint page table failed");

    let checkpointed_state = read_checkpoints(ctx)
        .await
        .expect("all checkpoints should be loaded");
    assert_eq!(checkpointed_state.page_tables.len(), 1);

    let page_table = checkpointed_state
        .page_tables
        .get(&PageFileId(0))
        .expect("checkpoint page table should exist");
    assert!(!page_table.has_changed());

    let mut collected_pages = Vec::new();
    page_table.collect_assigned_pages(&mut collected_pages);
    assert_eq!(collected_pages, pages);
}

#[tokio::test]
async fn test_page_table_load_from_checkpoints_cleanup_outdated_files() {
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
    checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
        .await
        .expect("Checkpoint page table failed");

    page_table.write_pages(&[PageMetadata {
        group: PageGroupId(1),
        revision: 0,
        next_page_id: PageId(1),
        id: PageId(5),
        data_len: 0,
        context: [0; 40],
    }]);
    assert!(page_table.has_changed());
    let new_file_id = checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
        .await
        .expect("Checkpoint page table failed");

    let checkpointed_state = read_checkpoints(ctx.clone())
        .await
        .expect("all checkpoints should be loaded");
    assert_eq!(checkpointed_state.page_tables.len(), 1);

    let file_ids = ctx.directory().list_dir(FileGroup::Metadata).await;
    assert_eq!(&file_ids, &[new_file_id]);
}

#[tokio::test]
async fn test_page_table_load_from_checkpoints_skips_cleanup_errors() {
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

    checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
        .await
        .expect("Checkpoint page table failed");

    page_table.write_pages(&[PageMetadata {
        group: PageGroupId(1),
        revision: 0,
        next_page_id: PageId(1),
        id: PageId(5),
        data_len: 0,
        context: [0; 40],
    }]);
    assert!(page_table.has_changed());
    checkpoint_page_table(ctx.clone(), PageFileId(0), 1, &page_table)
        .await
        .expect("Checkpoint page table failed");

    let scenario = fail::FailScenario::setup();
    fail::cfg("directory::remove_file", "return(-4)").unwrap();

    let checkpointed_state = read_checkpoints(ctx)
        .await
        .expect("all checkpoints should be loaded");
    assert_eq!(checkpointed_state.page_tables.len(), 1);

    scenario.teardown();
}

#[tokio::test]
async fn test_wal_replay_single_wal() {
    let ctx = ctx::FileContext::for_test(false).await;

    let wal_file = create_wal_file(&ctx).await;

    let entries = &[
        (
            1,
            vec![LogOp::Write(WriteOp {
                page_group_id: PageGroupId(1),
                page_file_id: PageFileId(1),
                altered_pages: vec![
                    PageMetadata {
                        group: PageGroupId(1),
                        revision: 0,
                        next_page_id: PageId(2),
                        id: PageId(1),
                        data_len: 0,
                        context: [0; 40],
                    },
                    PageMetadata {
                        group: PageGroupId(1),
                        revision: 0,
                        next_page_id: PageId::TERMINATOR,
                        id: PageId(2),
                        data_len: 0,
                        context: [0; 40],
                    },
                ],
            })],
        ),
        (
            2,
            vec![LogOp::Write(WriteOp {
                page_group_id: PageGroupId(2),
                page_file_id: PageFileId(1),
                altered_pages: vec![PageMetadata {
                    group: PageGroupId(2),
                    revision: 0,
                    next_page_id: PageId::TERMINATOR,
                    id: PageId(3),
                    data_len: 0,
                    context: [0; 40],
                }],
            })],
        ),
    ];

    write_log_ops(ctx.clone(), wal_file.clone(), entries).await;

    let controller = MetadataController::empty(ctx.clone());
    recover_wal_updates(ctx.clone(), &controller)
        .await
        .expect("wal file should be recovered");

    let mut pages = Vec::new();
    controller.collect_pages(PageGroupId(1), 0..60_000, &mut pages);
    assert_eq!(pages.len(), 2);

    let mut pages = Vec::new();
    controller.collect_pages(PageGroupId(2), 0..50, &mut pages);
    assert_eq!(pages.len(), 1);
}

#[tokio::test]
async fn test_wal_replay_multi_wal_ordering() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;

    let wal_file1 = create_wal_file(&ctx).await;
    let wal_file2 = create_wal_file(&ctx).await;

    let entries_wal2 = &[
        (
            1,
            vec![LogOp::Write(WriteOp {
                page_group_id: PageGroupId(1),
                page_file_id: PageFileId(1),
                altered_pages: vec![
                    PageMetadata {
                        group: PageGroupId(1),
                        revision: 0,
                        next_page_id: PageId(2),
                        id: PageId(1),
                        data_len: 0,
                        context: [0; 40],
                    },
                    PageMetadata {
                        group: PageGroupId(1),
                        revision: 0,
                        next_page_id: PageId::TERMINATOR,
                        id: PageId(2),
                        data_len: 0,
                        context: [0; 40],
                    },
                ],
            })],
        ),
        (
            2,
            vec![LogOp::Write(WriteOp {
                page_group_id: PageGroupId(2),
                page_file_id: PageFileId(1),
                altered_pages: vec![PageMetadata {
                    group: PageGroupId(2),
                    revision: 0,
                    next_page_id: PageId::TERMINATOR,
                    id: PageId(3),
                    data_len: 0,
                    context: [0; 40],
                }],
            })],
        ),
    ];

    let entries_wal1 = &[
        (
            3,
            vec![LogOp::Free(FreeOp {
                page_group_id: PageGroupId(2),
            })],
        ),
        (
            4,
            vec![LogOp::Reassign(ReassignOp {
                old_page_group_id: PageGroupId(1),
                new_page_group_id: PageGroupId(4),
            })],
        ),
    ];

    // We specifically write to wal2 first, so their timestamps have
    // wal2 being "older" than "wal1" when it comes to recovery.
    // This tests the behaviour as WAL entries are recycled.
    write_log_ops(ctx.clone(), wal_file2.clone(), entries_wal2).await;
    write_log_ops(ctx.clone(), wal_file1.clone(), entries_wal1).await;

    let controller = MetadataController::empty(ctx.clone());
    recover_wal_updates(ctx.clone(), &controller)
        .await
        .expect("wal file should be recovered");

    let mut pages = Vec::new();
    let exists = controller.collect_pages(PageGroupId(1), 0..60_000, &mut pages);
    assert!(!exists);

    let mut pages = Vec::new();
    let exists = controller.collect_pages(PageGroupId(2), 0..60_000, &mut pages);
    assert!(!exists);

    let mut pages = Vec::new();
    controller.collect_pages(PageGroupId(4), 0..60_000, &mut pages);
    assert_eq!(pages.len(), 2);
}
