use std::ops::Range;

use crate::controller::metadata::{MetadataController, PageTable};
use crate::ctx;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::{DISK_PAGE_SIZE, NUM_PAGES_PER_BLOCK};

#[test]
fn test_controller_insert_page_table() {
    let controller = MetadataController::empty();
    assert!(!controller.contains_page_table(PageFileId(1)));
    controller.insert_page_table(PageFileId(1), PageTable::default());
    assert!(controller.contains_page_table(PageFileId(1)));
}

#[test]
#[should_panic(expected = "page table already exists")]
fn test_controller_insert_page_table_panics_already_exists() {
    let controller = MetadataController::empty();
    assert!(!controller.contains_page_table(PageFileId(1)));
    controller.insert_page_table(PageFileId(1), PageTable::default());
    assert!(controller.contains_page_table(PageFileId(1)));
    controller.insert_page_table(PageFileId(1), PageTable::default());
}

#[test]
fn test_controller_create_blank_page_table() {
    let controller = MetadataController::empty();
    assert!(!controller.contains_page_table(PageFileId(1)));
    controller.create_blank_page_table(PageFileId(1));
    assert!(controller.contains_page_table(PageFileId(1)));
}

#[test]
fn test_controller_insert_page_group() {
    let controller = MetadataController::empty();
    controller.create_blank_page_table(PageFileId(1));

    assert_eq!(controller.find_first_page(PageGroupId(1)), None);
    controller.insert_page_group(PageGroupId(1), PageFileId(1), PageId(0));
    assert_eq!(
        controller.find_first_page(PageGroupId(1)),
        Some((PageFileId(1), PageId(0)))
    );
}

#[test]
#[should_panic(expected = "page table does not exist for page file: PageFileId(1)")]
fn test_controller_insert_page_group_panics_unknown_page_file() {
    let controller = MetadataController::empty();
    controller.insert_page_group(PageGroupId(1), PageFileId(1), PageId(0));
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
fn test_controller_write_pages(#[case] entries: &[PageMetadata]) {
    let controller = MetadataController::empty();
    controller.create_blank_page_table(PageFileId(1));
    controller.write_pages(PageFileId(1), entries);
    // NOTE: The page table has separate tests to check it was actually written.
    //       This is a sanity check.
}

#[test]
#[should_panic(expected = "page file ID should exist as provided by user")]
fn test_controller_write_pages_panics_unknown_page_file() {
    let controller = MetadataController::empty();
    controller.write_pages(PageFileId(1), &[]);
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
#[should_panic(expected = "BUG: page being referenced is empty")]
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
fn test_controller_collect_pages(
    #[case] entries: &[PageMetadata],
    #[case] start_page_id: PageId,
    #[case] data_range: Range<usize>,
    #[case] expected_pages: &[PageMetadata],
) {
    let controller = MetadataController::empty();
    controller.create_blank_page_table(PageFileId(1));
    controller.write_pages(PageFileId(1), entries);

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), start_page_id, data_range, &mut pages);
    assert_eq!(pages, expected_pages);
}

#[test]
#[should_panic(expected = "page file ID should exist as provided by user")]
fn test_controller_collect_pages_panics_unknown_page_file() {
    let controller = MetadataController::empty();
    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(0), 0..0, &mut pages);
}

#[test]
#[should_panic(expected = "page ID is beyond the bounds of the page table")]
fn test_controller_collect_pages_panics_out_of_bounds() {
    let controller = MetadataController::empty();
    controller.create_blank_page_table(PageFileId(1));

    let mut pages = Vec::new();
    controller.collect_pages(PageFileId(1), PageId(u32::MAX - 1), 0..0, &mut pages);
}

#[tokio::test]
async fn test_controller_checkpoint() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = MetadataController::empty();
    controller.create_blank_page_table(PageFileId(1));
    controller.create_blank_page_table(PageFileId(2));
    controller.create_blank_page_table(PageFileId(3));

    // The actual correctness of the files being written is tested in the checkpoint.rs file.
    let checkpointed_files = controller
        .checkpoint(ctx.clone())
        .await
        .expect("files should be checkpointed and returned");

    // No tables should be checkpointed as they have not changed.
    assert!(checkpointed_files.is_empty());

    controller.write_pages(
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
    controller.write_pages(
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

    let checkpointed_files = controller
        .checkpoint(ctx)
        .await
        .expect("files should be checkpointed and returned");
    let mut page_files_checkpointed = checkpointed_files
        .into_iter()
        .map(|(page_file_id, _)| page_file_id)
        .collect::<Vec<_>>();
    page_files_checkpointed.sort();
    assert_eq!(page_files_checkpointed, &[PageFileId(1), PageFileId(2)]);
}

#[tokio::test]
async fn test_controller_incremental_checkpoint() {
    let ctx = ctx::FileContext::for_test(false).await;

    let controller = MetadataController::empty();
    controller.create_blank_page_table(PageFileId(0));
    controller.create_blank_page_table(PageFileId(1));

    for id in 0..2 {
        controller.write_pages(
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

    let err = controller
        .checkpoint(ctx.clone())
        .await
        .expect_err("checkpoint error should occur");
    let first_page_file_id = err.completed_checkpoints[0].0;

    scenario.teardown();

    // The previously successfully tables have not changed, therefore they should be
    // appearing in the result of the next call.
    let checkpointed_files = controller
        .checkpoint(ctx)
        .await
        .expect("checkpoint should complete");
    assert_ne!(checkpointed_files[0].0, first_page_file_id);
}
