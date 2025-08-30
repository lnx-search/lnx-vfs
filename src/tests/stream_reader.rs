use std::io::{ErrorKind, Seek, SeekFrom, Write};

use crate::ctx;
use crate::directory::{FileGroup, FileId};
use crate::stream_reader::StreamReaderBuilder;

#[rstest::rstest]
#[case(32 << 10, 0)]
#[should_panic(expected = "buffer size must not be zero")]
#[case(0, 0)]
#[should_panic(expected = "buffer size must a multiple of DISK_ALIGN")]
#[case(100, 0)]
#[should_panic(expected = "offset must be a multiple of DISK_ALIGN")]
#[case(32 << 10, 15)]
#[tokio::test]
async fn test_stream_reader_builder_construction(
    #[case] buffer_size: usize,
    #[case] offset: u64,
) {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let _reader = StreamReaderBuilder::new(ctx.clone(), file)
        .with_buffer_size(buffer_size)
        .with_offset(offset)
        .build();
}

#[rstest::rstest]
#[tokio::test]
async fn test_reader_single_read(
    #[values(0, 4096)] offset: u64,
    #[values(20, 512, 4096, 64 << 10)] read_size: usize,
) {
    let sample_buffer = (0..(1 << 20))
        .map(|value| (value % 256) as u8)
        .collect::<Vec<u8>>();

    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    write_all_at(&ctx, file.id(), &sample_buffer, 0).await;

    let mut reader = StreamReaderBuilder::new(ctx.clone(), file)
        .with_offset(offset)
        .build();

    let mut buffer = vec![0; read_size];
    reader
        .read_exact(&mut buffer)
        .await
        .expect("read to buffer");
    assert_eq!(&buffer, &sample_buffer[..read_size]);
}

#[tokio::test]
async fn test_reader_short_read_error() {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    write_all_at(&ctx, file.id(), &vec![1; 4 << 10], 0).await;

    let mut reader = StreamReaderBuilder::new(ctx.clone(), file).build();

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::poll_reply_future", "return(12)").unwrap();

    // We use the fail point injection to simulate this, not an actual kernel call.
    let mut buffer = vec![0; 512];
    let err = reader
        .read_exact(&mut buffer)
        .await
        .expect_err("read exact should return error due to short read");
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    assert_eq!(
        err.to_string(),
        "kernel read returned small buffer after retries"
    );

    scenario.teardown();
}

#[tokio::test]
async fn test_reader_unexpected_eof() {
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    write_all_at(&ctx, file.id(), &vec![1; 4 << 10], 0).await;

    let mut reader = StreamReaderBuilder::new(ctx.clone(), file).build();

    let mut buffer = vec![0; 8 << 10];
    let err = reader
        .read_exact(&mut buffer)
        .await
        .expect_err("read exact should return error due to unexpected EOF");
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(err.to_string(), "could not fill buffer completely");
}

async fn write_all_at(
    ctx: &ctx::FileContext,
    file_id: FileId,
    data: &[u8],
    offset: u64,
) {
    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file_id)
        .await;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .unwrap();
    file.set_len(offset).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(data).unwrap();
}
