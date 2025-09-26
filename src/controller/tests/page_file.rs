use std::sync::Arc;

use crate::controller::metadata::MetadataController;
use crate::controller::page_file::PageFileController;
use crate::directory::FileGroup;
use crate::layout::PageFileId;
use crate::layout::page_metadata::PageMetadata;
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
    let page_file = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(page_file.num_page_files(), num_existing_files as usize);
}

#[rstest::rstest]
#[case::empty(0, &[])]
#[case::one_page_partial(400, &[400])]
#[case::one_page_full(DISK_PAGE_SIZE as u64, &[DISK_PAGE_SIZE as u32])]
#[case::many_pages(60 << 10, &[DISK_PAGE_SIZE as u32, 28_672])]
#[tokio::test]
async fn test_page_file_controller_write(
    #[case] data_len: u64,
    #[case] expected_page_sizes: &[u32],
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let page_file = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(page_file.num_page_files(), 0);

    let mut writer = page_file
        .create_writer(data_len)
        .await
        .expect("writer should be created");
    assert_eq!(page_file.num_page_files(), 1);

    writer
        .write(&vec![1; data_len as usize])
        .await
        .expect("submit write");

    let (_alloc_tx, metadata) = writer.finish().await.expect("finish writer");
    assert_eq!(metadata.len(), expected_page_sizes.len());
    assert_eq!(page_file.num_page_files(), 1);

    for (metadata, expected_size) in std::iter::zip(metadata, expected_page_sizes) {
        assert_eq!(metadata.data_len, *expected_size);
        assert!(!metadata.context.iter().all(|c| *c == 0));
    }
}

#[rstest::rstest]
#[tokio::test]
async fn test_page_file_controller_write_finish_early_err() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let metadata_controller = MetadataController::open(ctx.clone()).await.unwrap();
    let page_file = PageFileController::open(ctx.clone(), &metadata_controller)
        .await
        .expect("open page file");
    assert_eq!(page_file.num_page_files(), 0);

    let writer = page_file
        .create_writer(100)
        .await
        .expect("writer should be created");
    assert_eq!(page_file.num_page_files(), 1);

    let err = writer.finish().await.expect_err("finish should fail");
    assert_eq!(
        err.to_string(),
        "not all expected data has be submitted to the writer"
    );
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
) {
    for page in pages {
        let buffer = ctx.alloc::<DISK_PAGE_SIZE>();

        let reply = page_file
            .submit_write_at(&mut [*page], buffer)
            .await
            .expect("write should be submitted successfully");
        let n = file::wait_for_reply(reply)
            .await
            .expect("write should complete successfully");
        assert_eq!(n, 1);
    }
}
