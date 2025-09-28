use std::ops::Range;

use crate::controller::metadata::{LookupEntry, MetadataController, PageTable};
use crate::controller::tests::{create_wal_file, make_log_entry, write_log_entries};
use crate::ctx;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::{DISK_PAGE_SIZE, NUM_PAGES_PER_BLOCK};

#[tokio::test]
async fn test_controller_insert_page_table() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    assert!(!controller.contains_page_table(PageFileId(1)));
    controller.insert_page_table(PageFileId(1), PageTable::default());
    assert!(controller.contains_page_table(PageFileId(1)));
}

#[tokio::test]
#[should_panic(expected = "page table already exists")]
async fn test_controller_insert_page_table_panics_already_exists() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    assert!(!controller.contains_page_table(PageFileId(1)));
    controller.insert_page_table(PageFileId(1), PageTable::default());
    assert!(controller.contains_page_table(PageFileId(1)));
    controller.insert_page_table(PageFileId(1), PageTable::default());
}

#[tokio::test]
async fn test_controller_create_blank_page_table() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    assert!(!controller.contains_page_table(PageFileId(1)));
    controller.create_blank_page_table(PageFileId(1));
    assert!(controller.contains_page_table(PageFileId(1)));
}

#[tokio::test]
async fn test_controller_insert_page_group() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));

    assert_eq!(controller.find_first_page(PageGroupId(1)), None);
    controller.insert_page_group(
        PageGroupId(1),
        LookupEntry {
            page_file_id: PageFileId(1),
            first_page_id: PageId(0),
            revision: 0,
        },
    );
    assert_eq!(
        controller.find_first_page(PageGroupId(1)),
        Some(LookupEntry {
            page_file_id: PageFileId(1),
            first_page_id: PageId(0),
            revision: 0,
        }),
    );
}

#[tokio::test]
#[should_panic(expected = "page table does not exist for page file: PageFileId(1)")]
async fn test_controller_insert_page_group_panics_unknown_page_file() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.insert_page_group(
        PageGroupId(1),
        LookupEntry {
            page_file_id: PageFileId(1),
            first_page_id: PageId(0),
            revision: 0,
        },
    );
}

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
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(6),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(7),
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
async fn test_controller_write_pages(#[case] entries: &[PageMetadata]) {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));
    controller.assign_pages_to_group(PageFileId(1), entries);
    // NOTE: The page table has separate tests to check it was actually written.
    //       This is a sanity check.
}

#[should_panic(expected = "BUG: page being referenced is unassigned")]
#[tokio::test]
async fn test_controller_set_empty() {
    let ctx = ctx::FileContext::for_test(false).await;

    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));
    controller.assign_pages_to_group(
        PageFileId(1),
        &[PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        }],
    );

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(4), 0..50, &mut pages);
    assert_eq!(pages.len(), 1);

    controller
        .assign_pages_to_group(PageFileId(1), &[PageMetadata::unassigned(PageId(4))]);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(4), 0..50, &mut pages);
}

#[tokio::test]
#[should_panic(expected = "page file ID should exist as provided by user")]
async fn test_controller_write_pages_panics_unknown_page_file() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.assign_pages_to_group(PageFileId(1), &[]);
}

#[rstest::rstest]
#[case::empty_table(&[], PageId(0), 0..0, &[])]
#[case::single_entry_full_page(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    0..DISK_PAGE_SIZE,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[case::single_entry_partial_page(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    0..50,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[case::single_entry_partial_page_with_offset(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    50..DISK_PAGE_SIZE,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[case::multiple_entries_single_block(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(6),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(6),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    0..DISK_PAGE_SIZE * 3,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(6),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(6),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[case::multiple_entries_many_blocks(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(24_000),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    0..DISK_PAGE_SIZE * 3,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(24_000),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[case::multiple_entries_with_offset(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(24_000),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    DISK_PAGE_SIZE..DISK_PAGE_SIZE * 2 + 3,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(24_000),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[should_panic(expected = "BUG: page being referenced is unassigned")]
#[case::next_page_id_not_terminator_or_missing_panics(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(0),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    0..DISK_PAGE_SIZE + 1,
    &[]
)]
#[case::next_page_id_not_terminator_doesnt_panic_if_all_pages_requested_exist(
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(0),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
    ],
    PageId(4),
    0..DISK_PAGE_SIZE,
    &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(0),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
    ]
)]
#[trace]
#[tokio::test]
async fn test_controller_collect_pages(
    #[case] entries: &[PageMetadata],
    #[case] start_page_id: PageId,
    #[case] data_range: Range<usize>,
    #[case] expected_pages: &[PageMetadata],
) {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));
    controller.assign_pages_to_group(PageFileId(1), entries);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), start_page_id, data_range, &mut pages);
    assert_eq!(pages, expected_pages);
}

#[tokio::test]
#[should_panic(expected = "page file ID should exist as provided by user")]
async fn test_controller_collect_pages_panics_unknown_page_file() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(0), 0..0, &mut pages);
}

#[tokio::test]
#[should_panic(expected = "page ID is beyond the bounds of the page table")]
async fn test_controller_collect_pages_panics_out_of_bounds() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(u32::MAX - 1), 0..0, &mut pages);
}

#[tokio::test]
async fn test_controller_checkpoint() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));
    controller.create_blank_page_table(PageFileId(2));
    controller.create_blank_page_table(PageFileId(3));

    // The actual correctness of the files being written is tested in the checkpoint.rs file.
    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("files should be checkpointed and returned");

    // No tables should be checkpointed as they have not changed.
    assert_eq!(num_checkpointed_files, 0);
    assert_eq!(controller.num_files_to_cleanup(), 0);

    controller.assign_pages_to_group(
        PageFileId(1),
        &[PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        }],
    );
    controller.assign_pages_to_group(
        PageFileId(2),
        &[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(5),
                id: PageId(4),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(24_000),
                id: PageId(5),
                data_len: 0,
                context: [0; 40],
            },
        ],
    );

    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("files should be checkpointed and returned");
    assert_eq!(num_checkpointed_files, 2);
    // no files originally.
    assert_eq!(controller.num_files_to_cleanup(), 0);
}

#[tokio::test]
async fn test_controller_incremental_checkpoint() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(0));
    controller.create_blank_page_table(PageFileId(1));

    for id in 0..2 {
        controller.assign_pages_to_group(
            PageFileId(id),
            &[PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(5),
                id: PageId(4),
                data_len: 0,
                context: [0; 40],
            }],
        );
    }

    let scenario = fail::FailScenario::setup();
    // Ignore the first call on the fail point, then return pre-configured error.
    fail::cfg("checkpoint::checkpoint_page_table", "1*off->return").unwrap();

    let _err = controller
        .checkpoint()
        .await
        .expect_err("checkpoint error should occur");

    scenario.teardown();

    // The previously successfully tables have not changed, therefore they should be
    // appearing in the result of the next call.
    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("checkpoint should complete");
    assert_eq!(num_checkpointed_files, 1);
}

#[tokio::test]
async fn test_controller_gc_old_checkpoint_files() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(0));

    controller.assign_pages_to_group(
        PageFileId(0),
        &[PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        }],
    );
    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("checkpoint should complete");
    assert_eq!(num_checkpointed_files, 1);
    assert_eq!(controller.num_files_to_cleanup(), 0);

    controller.assign_pages_to_group(
        PageFileId(0),
        &[PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        }],
    );

    let scenario = fail::FailScenario::setup();
    fail::cfg("metadata::garbage_collect_checkpoints", "return").unwrap();

    let _err = controller
        .checkpoint()
        .await
        .expect_err("checkpoint error should occur when gc attempted");
    assert_eq!(controller.num_files_to_cleanup(), 1);

    scenario.teardown();

    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("checkpoint should complete");
    assert_eq!(num_checkpointed_files, 0);

    // Checkpoint should still cleanup files
    assert_eq!(controller.num_files_to_cleanup(), 0);
}

#[tokio::test]
async fn test_controller_recover_from_checkpoints() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx.clone());
    controller.create_blank_page_table(PageFileId(2));

    let all_pages = &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(5),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(2),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(5),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(7),
            data_len: 0,
            context: [0; 40],
        },
        // This page should overwrite the GroupId 5 entry.
        PageMetadata {
            group: PageGroupId(6),
            revision: 0,
            next_page_id: PageId(24_000),
            id: PageId(7),
            data_len: 0,
            context: [0; 40],
        },
    ];
    controller.assign_pages_to_group(PageFileId(2), all_pages);

    let num_checkpointed_files = controller.checkpoint().await.unwrap();
    assert_eq!(num_checkpointed_files, 1);
    drop(controller);

    let controller = MetadataController::open(ctx)
        .await
        .expect("controller should be opened without error");
    assert_eq!(controller.num_page_groups(), 3);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(2), PageId(4), 0..50, &mut pages);
    assert_eq!(pages, &[all_pages[0]]);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(2), PageId(5), 0..50, &mut pages);
    assert_eq!(pages, &[all_pages[1]]);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(2), PageId(7), 0..50, &mut pages);
    assert_eq!(pages, &[all_pages[3]]);
}

#[tokio::test]
async fn test_controller_recover_from_wal() {
    let ctx = ctx::FileContext::for_test(false).await;

    let wal_file = create_wal_file(&ctx).await;
    let entries = &[
        // Normal transaction that was completed.
        make_log_entry(PageGroupId(0), PageId(0), 0, 2, PageFileId(1), PageId(1)),
        make_log_entry(
            PageGroupId(0),
            PageId(1),
            0,
            2,
            PageFileId(1),
            PageId::TERMINATOR,
        ),
        // Aborted transaction
        make_log_entry(PageGroupId(1), PageId(1), 1, 2, PageFileId(1), PageId(4)),
        // Transaction that is applied to a different page file.
        make_log_entry(
            PageGroupId(2),
            PageId(5),
            2,
            1,
            PageFileId(3),
            PageId::TERMINATOR,
        ),
    ];

    write_log_entries(ctx.clone(), wal_file.clone(), entries, 0).await;

    let controller = MetadataController::open(ctx)
        .await
        .expect("controller should be opened without error");
    assert_eq!(controller.num_page_groups(), 2);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(0), 0..50, &mut pages);
    assert_eq!(
        pages,
        &[PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(0),
            data_len: 0,
            context: [0; 40],
        }]
    );

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(3), PageId(5), 0..50, &mut pages);
    assert_eq!(
        pages,
        &[PageMetadata {
            group: PageGroupId(2),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        }]
    );
}

// Debugging tip: Ignore this set of tests if the `test_controller_recover_from_checkpoints`
// test is also failing, as it is probably unrelated to the fuzzing.
#[rstest::rstest]
#[case::fail_read_checkpoint("checkpoint::read_checkpoint", "return", true)]
#[case::fail_remove_file("directory::remove_file", "return", false)]
#[case::blocking_read_checkpoint("checkpoint::read_checkpoint", "sleep(200)", false)]
#[case::blocking_remove_file("directory::remove_file", "sleep(200)", false)]
#[trace]
#[tokio::test]
async fn test_controller_recovery_checkpoint_fuzz(
    #[case] fail_point: &str,
    #[case] fail_action: &str,
    #[case] expect_error: bool,
    #[values(1, 5, 20)] num_page_tables: usize,
    #[values(1, 3, 9, 133, 373)] num_entries_per_table: usize,
) {
    fastrand::seed(643634634637843);

    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx.clone());

    let mut page_id = 0;
    for id in 0..num_page_tables {
        let page_file_id = PageFileId(id as u32);
        controller.create_blank_page_table(page_file_id);

        let mut pages = Vec::new();
        for _ in 0..num_entries_per_table {
            page_id += 1;

            let group_id = PageGroupId(fastrand::u64(0..500));
            let metadata = PageMetadata {
                group: group_id,
                revision: fastrand::u32(0..10),
                next_page_id: PageId(fastrand::u32(0..5_000)),
                id: PageId(page_id),
                data_len: 0,
                context: [0; 40],
            };

            // Lookup entry not valid, but we don't care for this test.
            controller.insert_page_group(
                group_id,
                LookupEntry {
                    page_file_id,
                    first_page_id: PageId::TERMINATOR,
                    revision: 0,
                },
            );

            pages.push(metadata);
        }

        controller.assign_pages_to_group(page_file_id, &pages);
    }

    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("checkpoint should complete");
    assert_eq!(num_checkpointed_files, num_page_tables);

    // Write one page and checkpoint again in order to check remove file fail point.
    controller.assign_pages_to_group(
        PageFileId(0),
        &[PageMetadata {
            group: PageGroupId(1),
            revision: fastrand::u32(0..10),
            next_page_id: PageId(fastrand::u32(0..5_000)),
            id: PageId(19999),
            data_len: 0,
            context: [0; 40],
        }],
    );
    // Lookup entry not valid, but we don't care for this test.
    controller.insert_page_group(
        PageGroupId(1),
        LookupEntry {
            page_file_id: PageFileId(0),
            first_page_id: PageId::TERMINATOR,
            revision: 0,
        },
    );
    let num_checkpointed_files = controller
        .checkpoint()
        .await
        .expect("checkpoint should complete");
    assert_eq!(num_checkpointed_files, 1);

    let num_expected_page_groups = controller.num_page_groups();
    drop(controller);

    let scenario = fail::FailScenario::setup();
    fail::cfg(fail_point, fail_action).unwrap();

    let result = MetadataController::open(ctx.clone()).await;
    if expect_error && result.is_ok() {
        panic!("controller should encounter an error");
    } else if let Err(err) = result
        && !expect_error
    {
        panic!("controller errored unexpectedly: {err:?}");
    }

    scenario.teardown();

    let controller = MetadataController::open(ctx)
        .await
        .expect("controller should be opened without error");
    for id in 0..num_page_tables {
        assert!(controller.contains_page_table(PageFileId(id as u32)));
    }

    assert_eq!(controller.num_page_groups(), num_expected_page_groups);
}

#[tokio::test]
async fn test_controller_create_page_file_allocator() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(0));

    let entries = &[
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(1),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(2),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(25_000),
            data_len: 0,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(3),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(169_000),
            data_len: 0,
            context: [0; 40],
        },
    ];
    controller.assign_pages_to_group(PageFileId(0), entries);

    let page_file_allocator = controller.create_page_file_allocator();
    assert_eq!(page_file_allocator.num_page_files(), 1);
    assert_eq!(page_file_allocator.capacity(), 442_368)
}
