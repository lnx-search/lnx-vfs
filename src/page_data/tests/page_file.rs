use std::io::Write;
use std::os::unix::fs::FileExt;

use crate::buffer::ALLOC_PAGE_SIZE;
use crate::directory::FileGroup;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageId, file_metadata};
use crate::page_data::page_file::PageFile;
use crate::page_data::{
    DISK_PAGE_SIZE,
    MAX_NUM_PAGES,
    MetadataHeader,
    page_associated_data,
};
use crate::{ctx, file};

#[rstest::rstest]
#[tokio::test]
async fn test_create_page_file(#[values(false, true)] encryption: bool) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;

    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .expect("page file should be created");
    drop(page_file);

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Pages, file.id())
        .await;

    let mut written_data = std::fs::read(file_path).expect("could not read page file");

    let header_associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Pages);
    let header: MetadataHeader = file_metadata::decode_metadata(
        ctx.cipher(),
        &header_associated_data,
        &mut written_data[..file_metadata::HEADER_SIZE],
    )
    .expect("valid header should be written");

    assert_eq!(header.file_id, file.id());
    assert_eq!(header.page_file_id, PageFileId(1));
    assert_eq!(header.max_num_pages, MAX_NUM_PAGES);
    assert_eq!(header.encryption, ctx.get_encryption_status());
}

#[rstest::rstest]
#[tokio::test]
async fn test_open_existing_page_file(#[values(false, true)] encryption: bool) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;

    let header_associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Pages);

    let header = MetadataHeader {
        file_id: file.id(),
        page_file_id: PageFileId(1),
        encryption: ctx.get_encryption_status(),
        max_num_pages: MAX_NUM_PAGES,
    };

    let mut header_buffer = vec![0; file_metadata::HEADER_SIZE];
    file_metadata::encode_metadata(
        ctx.cipher(),
        &header_associated_data,
        &header,
        &mut header_buffer[..],
    )
    .unwrap();

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Pages, file.id())
        .await;

    let mut raw_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    raw_file.write_all(&header_buffer).unwrap();
    raw_file.sync_all().unwrap();

    let page_file = PageFile::open(ctx.clone(), file.clone())
        .await
        .expect("page file should be opened");
    assert_eq!(page_file.id(), PageFileId(1));
}

#[tokio::test]
async fn test_page_file_read_at() {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Pages, file.id())
        .await;
    let raw_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let mut page_metadata = PageMetadata::unassigned(PageId(0));
    let mut data = vec![0; DISK_PAGE_SIZE];
    data[..13].copy_from_slice(b"Hello, world!");

    let associated_data = page_associated_data(file.id(), PageFileId(1), PageId(0));
    crate::page_data::encode::encode_page_data(
        None,
        &associated_data,
        &mut data,
        &mut page_metadata.context,
    )
    .unwrap();

    raw_file
        .write_all_at(&data, file_metadata::HEADER_SIZE as u64)
        .unwrap();
    raw_file.sync_all().unwrap();

    let buffer = ctx.alloc::<{ 32 << 10 }>();
    let (_, buffer) = page_file
        .read_at(&[page_metadata], buffer)
        .await
        .expect("read should be submitted successfully");
    assert_eq!(&buffer[..13], b"Hello, world!");
}

#[rstest::rstest]
#[case::encryption_false(false, "buffer verification failed")]
#[case::encryption_true(true, "buffer decryption failed")]
#[tokio::test]
async fn test_page_file_read_at_decode_err(
    #[case] encryption: bool,
    #[case] expected_error: &'static str,
) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Pages, file.id())
        .await;
    let raw_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let mut page_metadata = PageMetadata::unassigned(PageId(0));
    let mut data = vec![0; DISK_PAGE_SIZE];
    data[..13].copy_from_slice(b"Hello, world!");

    let associated_data = page_associated_data(file.id(), PageFileId(1), PageId(0));
    crate::page_data::encode::encode_page_data(
        ctx.cipher(),
        &associated_data,
        &mut data,
        &mut page_metadata.context,
    )
    .unwrap();

    data[50..][..8].copy_from_slice(b"mismatch");

    raw_file
        .write_all_at(&data, file_metadata::HEADER_SIZE as u64)
        .unwrap();
    raw_file.sync_all().unwrap();

    let buffer = ctx.alloc::<{ 32 << 10 }>();
    let error = page_file
        .read_at(&[page_metadata], buffer)
        .await
        .expect_err("read should error");
    assert_eq!(error.to_string(), expected_error);
}

#[rstest::rstest]
#[case::single_write_start(vec![PageMetadata::unassigned(PageId(0))], 1)]
#[case::dense_many_write_at_eof(
    vec![
        PageMetadata::unassigned(PageId(0)),
        PageMetadata::unassigned(PageId(1)),
        PageMetadata::unassigned(PageId(2)),
    ],
    3,
)]
#[case::dense_write_beyond_current_eof(
    vec![
        PageMetadata::unassigned(PageId(1)),
        PageMetadata::unassigned(PageId(2)),
    ],
    2,
)]
#[should_panic(
    expected = "write should be submitted successfully: IO(Custom { kind: InvalidInput, error: \"empty metadata entries\" })"
)]
#[case::empty_metadata_err(vec![], 0)]
#[should_panic(
    expected = "write should be submitted successfully: IO(Custom { kind: InvalidInput, error: \"provided page range is not contiguous\" })"
)]
#[case::sparse_metadata_err(
    vec![
        PageMetadata::unassigned(PageId(0)),
        PageMetadata::unassigned(PageId(3)),
    ],
    3,
)]
#[should_panic(
    expected = "write should be submitted successfully: IO(Custom { kind: InvalidInput, error: \"iop size too large, expected: 8 IOPS, got: 9\" })"
)]
#[case::too_big_iop_err(
    vec![
        PageMetadata::unassigned(PageId(0)),
        PageMetadata::unassigned(PageId(1)),
        PageMetadata::unassigned(PageId(2)),
        PageMetadata::unassigned(PageId(3)),
        PageMetadata::unassigned(PageId(4)),
        PageMetadata::unassigned(PageId(5)),
        PageMetadata::unassigned(PageId(6)),
        PageMetadata::unassigned(PageId(7)),
        PageMetadata::unassigned(PageId(8)),
    ],
    9,
)]
#[should_panic(
    expected = "write should be submitted successfully: IO(Custom { kind: InvalidInput, error: \"provided buffer too small\" })"
)]
#[case::buffer_too_small_err(
    vec![
        PageMetadata::unassigned(PageId(0)),
        PageMetadata::unassigned(PageId(1)),
        PageMetadata::unassigned(PageId(2)),
        PageMetadata::unassigned(PageId(3)),
    ],
    2,
)]
#[tokio::test]
async fn test_page_file_write(
    #[values(false, true)] encryption: bool,
    #[case] metadata: Vec<PageMetadata>,
    #[case] num_pages: usize,
) {
    let mut metadata = metadata;
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let num_alloc_pages = (num_pages * DISK_PAGE_SIZE) / ALLOC_PAGE_SIZE;
    let mut buffer = ctx.alloc_pages(num_alloc_pages);
    buffer.fill(4);

    let expected_len = buffer.len();
    let reply = page_file
        .submit_write_at(&mut metadata, buffer)
        .await
        .expect("write should be submitted successfully");
    let n = file::wait_for_reply(reply)
        .await
        .expect("write should complete successfully");
    assert_eq!(n, expected_len);
}

#[rstest::rstest]
#[should_panic(expected = "BUG: page ID is beyond the bounds of the page file")]
#[case(
    vec![
        PageMetadata::unassigned(PageId(u32::MAX)),
    ],
    DISK_PAGE_SIZE / ALLOC_PAGE_SIZE,
)]
#[case(
    vec![
        PageMetadata::unassigned(PageId(MAX_NUM_PAGES as u32 - 2)),
        PageMetadata::unassigned(PageId(MAX_NUM_PAGES as u32 - 1)),
    ],
    (DISK_PAGE_SIZE * 2) / ALLOC_PAGE_SIZE,
)]
#[should_panic(expected = "BUG: page ID is beyond the bounds of the page file")]
#[case(
    vec![
        PageMetadata::unassigned(PageId(MAX_NUM_PAGES as u32 - 2)),
        PageMetadata::unassigned(PageId(MAX_NUM_PAGES as u32 - 1)),
        PageMetadata::unassigned(PageId(MAX_NUM_PAGES as u32)),
    ],
    (DISK_PAGE_SIZE * 3) / ALLOC_PAGE_SIZE,
)]
#[tokio::test]
#[trace]
async fn test_page_file_write_out_of_bounds_panics(
    #[case] metadata: Vec<PageMetadata>,
    #[case] num_pages_to_alloc: usize,
) {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let mut metadata = metadata;
    let buffer = ctx.alloc_pages(num_pages_to_alloc);
    page_file
        .submit_write_at(&mut metadata, buffer)
        .await
        .unwrap();
}
