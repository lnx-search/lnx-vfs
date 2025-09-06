use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::directory::FileGroup;
use crate::layout::{PageFileId, PageId, file_metadata};
use crate::page_data::page_file::PageFile;
use crate::page_data::{MAX_NUM_PAGES, MetadataHeader};
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
    raw_file
        .write_all_at(b"Hello, world!", file_metadata::HEADER_SIZE as u64)
        .unwrap();
    raw_file.sync_all().unwrap();

    let mut buffer = ctx.alloc::<{ 32 << 10 }>();
    let reply = page_file
        .submit_read_at(PageId(0), &mut buffer)
        .await
        .expect("read should be submitted successfully");
    let n = file::wait_for_reply(reply)
        .await
        .expect("read should complete successfully");
    assert_eq!(n, 13);
    assert_eq!(&buffer[..13], b"Hello, world!");
}

#[should_panic(expected = "page ID is beyond the bounds of the page file")]
#[tokio::test]
async fn test_page_file_write_out_of_bounds_panics() {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let mut buffer = ctx.alloc::<{ 32 << 10 }>();
    let _ = page_file
        .submit_write_at(PageId(u32::MAX), &mut buffer, 32 << 10)
        .await;
}

#[tokio::test]
async fn test_page_file_write_and_sync_start() {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let mut buffer = ctx.alloc::<{ 32 << 10 }>();
    buffer.fill(4);
    let write_len = buffer.len();
    let reply = page_file
        .submit_write_at(PageId(0), &mut buffer, write_len)
        .await
        .expect("write should be submitted successfully");
    let n = file::wait_for_reply(reply)
        .await
        .expect("write should complete successfully");
    assert_eq!(n, 32 << 10);

    let op_stamp = page_file.sync().await.expect("sync should complete");
    assert_eq!(op_stamp, 2);

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Pages, file.id())
        .await;
    let content = std::fs::read(&file_path).unwrap();
    assert!(
        content[file_metadata::HEADER_SIZE..]
            .iter()
            .all(|b| *b == 4),
        "buffer was not written out correctly"
    );
}

#[tokio::test]
async fn test_page_file_sync_coalesce() {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let mut buffer = ctx.alloc::<{ 32 << 10 }>();
    buffer.fill(4);
    let write_len = buffer.len();

    let reply1 = page_file
        .submit_write_at(PageId(0), &mut buffer, write_len)
        .await
        .unwrap();
    let n = file::wait_for_reply(reply1)
        .await
        .expect("write should complete successfully");
    assert_eq!(n, 32 << 10);

    let reply2 = page_file
        .submit_write_at(PageId(1), &mut buffer, write_len)
        .await
        .unwrap();
    let n = file::wait_for_reply(reply2)
        .await
        .expect("write should complete successfully");
    assert_eq!(n, 32 << 10);

    let fut1 = page_file.sync();
    let fut2 = page_file.sync();

    // Only one flush should be performed, hence the counter should only be incremented,
    // 3 times (1, 2, 3).
    let (r1, r2) = tokio::join!(fut1, fut2);
    assert_eq!(r1.unwrap(), 3);
    assert_eq!(r2.unwrap(), 3);
}
