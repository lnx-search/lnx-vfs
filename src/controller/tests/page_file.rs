use std::sync::Arc;

use crate::controller::metadata::MetadataController;
use crate::controller::page_file::PageFileController;
use crate::directory::FileGroup;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::{DISK_PAGE_SIZE, PageFile};
use crate::{ctx, file};

#[rstest::rstest]
#[tokio::test]
async fn test_open_page_file_controller(#[values(0, 1, 4)] num_existing_files: u32) {
    let ctx = ctx::FileContext::for_test(false).await;

    for i in 0..num_existing_files {
        create_blank_page_file(ctx.clone(), PageFileId(i)).await;
    }

    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), num_existing_files as usize);
}

#[rstest::rstest]
#[case::empty(0, &[])]
#[case::one_page_partial(400, &[400])]
#[case::one_page_full(DISK_PAGE_SIZE as u64, &[DISK_PAGE_SIZE as u32])]
#[case::many_pages(60 << 10, &[DISK_PAGE_SIZE as u32, 28_672])]
#[should_panic(expected = "number of pages exceeds maximum page file size")]
#[case::panic_file_too_large(18 << 30, &[])]
#[tokio::test]
async fn test_controller_write(
    #[case] data_len: u64,
    #[case] expected_page_sizes: &[u32],
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let mut writer = controller
        .create_writer(data_len)
        .await
        .expect("writer should be created");
    assert_eq!(controller.num_page_files(), 1);

    writer
        .write(&vec![1; data_len as usize])
        .await
        .expect("submit write");

    let (_alloc_tx, metadata) = writer.finish().await.expect("finish writer");
    assert_eq!(metadata.len(), expected_page_sizes.len());
    assert_eq!(controller.num_page_files(), 1);

    for (metadata, expected_size) in std::iter::zip(metadata, expected_page_sizes) {
        assert_eq!(metadata.data_len, *expected_size);
        assert!(!metadata.context.iter().all(|c| *c == 0));
    }
}

#[tokio::test]
async fn test_controller_write_uses_existing_page_file() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;

    create_blank_page_file(ctx.clone(), PageFileId(1)).await;

    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 1);

    let writer = controller
        .create_writer(100)
        .await
        .expect("writer should be created");
    assert_eq!(controller.num_page_files(), 1);
    assert_eq!(writer.num_pages_allocated(), 1);
}

#[rstest::rstest]
#[case::empty_existing_file(&[], &[])]
#[case::read_empty_page(
    &[
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(0),
            data_len: 0,
            context: [0; 40],
        }
    ],
    &[0],
)]
#[case::single_page(
    &[
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(0),
            data_len: 500,
            context: [0; 40],
        }
    ],
    &[0],
)]
#[case::many_single_pages(
    &[
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(0),
            data_len: 500,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(4),
            data_len: DISK_PAGE_SIZE as u32,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(3),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(63_000),
            data_len: 0,
            context: [0; 40],
        },
    ],
    &[0, 1, 2],
)]
#[case::many_dense_pages(
    &[
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(0),
            data_len: 5_000,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(1),
            data_len: 5_000,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(2),
            data_len: 5_000,
            context: [0; 40],
        },
    ],
    &[0, 1, 2],
)]
#[case::sparse_pages_coalesced(
    &[
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(0),
            data_len: 5_000,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(1),
            data_len: 5_000,
            context: [0; 40],
        },
        PageMetadata {
            group: PageGroupId(0),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(2),
            data_len: 5_000,
            context: [0; 40],
        },
    ],
    &[0,  2],
)]
#[tokio::test]
async fn test_controller_read(
    #[case] write_metadata: &[PageMetadata],
    #[case] read_pages: &[usize],
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let page_file = create_blank_page_file(ctx.clone(), PageFileId(0)).await;
    let pages_written = populate_page_file(&ctx, &page_file, write_metadata).await;

    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 1);
    tracing::info!("about to read???");

    let mut pages_to_read = Vec::with_capacity(read_pages.len());
    for &idx in read_pages {
        pages_to_read.push(pages_written[idx]);
    }

    let mut results = controller
        .read_many(PageFileId(0), &pages_to_read)
        .await
        .expect("read pages");

    let mut pages_and_is_valid = Vec::new();
    while let Some(result) = results.join_next().await {
        let mut result = result
            .expect("read should not panic")
            .expect("read should not error");

        while let Some((metadata, slice)) = result.next_page() {
            pages_and_is_valid.push((metadata, slice.iter().all(|b| *b == 4)));
        }
    }
    pages_and_is_valid.sort_by_key(|(p, _)| p.id);

    assert_eq!(pages_and_is_valid.len(), read_pages.len());

    for ((actual_metadata, is_valid), expected_metadata) in
        std::iter::zip(pages_and_is_valid, pages_to_read)
    {
        assert_eq!(actual_metadata.id, expected_metadata.id);
        assert_eq!(actual_metadata.data_len, expected_metadata.data_len);
        assert_eq!(actual_metadata.group, expected_metadata.group);
        assert!(
            is_valid,
            "page was invalid, actual: {actual_metadata:?}, expected: {expected_metadata:?}"
        );
    }
}

#[rstest::rstest]
#[tokio::test]
async fn test_controller_write_finish_early_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let writer = controller
        .create_writer(100)
        .await
        .expect("writer should be created");
    assert_eq!(controller.num_page_files(), 1);

    let err = writer.finish().await.expect_err("finish should fail");
    assert_eq!(
        err.to_string(),
        "not all expected data has be submitted to the writer"
    );
}

#[rstest::rstest]
#[tokio::test]
async fn test_controller_write_too_many_bytes_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let mut writer = controller
        .create_writer(100)
        .await
        .expect("writer should be created");
    assert_eq!(controller.num_page_files(), 1);
    assert_eq!(
        format!("{writer:?}"),
        "PageDataWriter(page_file_id=PageFileId(1), expected_len=100)"
    );

    let err = writer
        .write(&[0; 500])
        .await
        .expect_err("write should fail");
    assert_eq!(
        err.to_string(),
        "write could not be completed as it would go beyond the bounds of the defined page size"
    );
}

#[rstest::rstest]
#[tokio::test]
async fn test_controller_read_page_file_not_found_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let err = controller
        .read_many(PageFileId(0), &[])
        .await
        .expect_err("finish should fail");
    assert_eq!(err.to_string(), "page file not found: PageFileId(0)");
}

#[tokio::test]
async fn test_controller_short_write_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let mut writer = controller
        .create_writer(100)
        .await
        .expect("writer should be created");
    assert_eq!(controller.num_page_files(), 1);

    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "i2o2::fail::try_get_result",
        "1*return(pending)->1*return(50)",
    )
    .unwrap();

    writer.write(&[1; 100]).await.expect("submit write");
    let err = writer.finish().await.expect_err("finish should fail");

    assert_eq!(err.to_string(), "short write occurred");
    scenario.teardown();

    let mut writer = controller
        .create_writer(100)
        .await
        .expect("writer should be created");
    assert_eq!(controller.num_page_files(), 1);

    // Checks that when process_completed_iops is called after the write, that
    // it handles the short write correctly.
    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", "return(50)").unwrap();

    let err = writer
        .write(&[1; 100])
        .await
        .expect_err("write should fail");

    assert_eq!(err.to_string(), "short write occurred");
    scenario.teardown();
}

#[tokio::test]
async fn test_controller_create_new_page_file_timeout_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "page_file_controller::create_new_page_file_delay",
        "return(300)",
    )
    .unwrap();

    let err = controller
        .create_writer(100)
        .await
        .expect_err("writer should error");
    assert_eq!(controller.num_page_files(), 0);
    assert_eq!(err.to_string(), "timed out");

    scenario.teardown();
}

#[tokio::test]
async fn test_controller_create_new_page_file_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let controller = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(controller.num_page_files(), 0);

    let scenario = fail::FailScenario::setup();
    fail::cfg("page_file::create", "return(-4)").unwrap();

    let err = controller
        .create_writer(100)
        .await
        .expect_err("writer should error");
    assert_eq!(controller.num_page_files(), 0);
    assert_eq!(err.to_string(), "Interrupted system call (os error 4)");

    scenario.teardown();
}

async fn create_blank_page_file(
    ctx: Arc<ctx::FileContext>,
    page_file_id: PageFileId,
) -> PageFile {
    let directory = ctx.directory();
    let file_id = directory.create_new_file(FileGroup::Pages).await.unwrap();
    let file = directory
        .get_rw_file(FileGroup::Pages, file_id)
        .await
        .unwrap();
    PageFile::create(ctx, file, page_file_id)
        .await
        .expect("create page file")
}

async fn populate_page_file(
    ctx: &ctx::FileContext,
    page_file: &PageFile,
    pages: &[PageMetadata],
) -> Vec<PageMetadata> {
    let mut populated_pages = Vec::new();
    for page in pages {
        let mut buffer = ctx.alloc::<DISK_PAGE_SIZE>();
        buffer[..page.data_len as usize].fill(4);

        let mut page_to_write = [*page];
        let reply = page_file
            .submit_write_at(&mut page_to_write, buffer)
            .await
            .expect("write should be submitted successfully");
        let n = file::wait_for_reply(reply)
            .await
            .expect("write should complete successfully");
        assert_eq!(n, DISK_PAGE_SIZE);
        populated_pages.push(page_to_write[0]);
    }
    populated_pages
}
