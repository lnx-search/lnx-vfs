use std::io;

use crate::directory::FileGroup;
use crate::layout::log::{FreeOp, LogOp};
use crate::layout::{PageGroupId, file_metadata, log};
use crate::page_op_log::reader::LogFileReader;
use crate::page_op_log::{MetadataHeader, op_log_associated_data};
use crate::{ctx, file};

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_log_reader(
    #[values(true, false)] encryption: bool,
    #[values(0, 4096)] offset: u64,
    #[values(0, 5, 18)] num_blocks: usize,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(encryption).await;
    let mut file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    make_sample_file(&ctx, &mut file, num_blocks, offset).await;

    let mut reader = LogFileReader::new(ctx, file.into(), 1, 5, offset);
    assert_eq!(reader.order_key(), 5);

    let mut num_transactions = 0;
    let mut ops = Vec::new();
    loop {
        let transaction_id = reader
            .next_transaction(&mut ops)
            .await
            .expect("Failed to read block");

        if transaction_id.is_none() {
            break;
        }

        num_transactions += 1;
    }
    assert_eq!(num_transactions, num_blocks);
    assert_eq!(ops.len(), num_blocks);
}

async fn make_sample_file(
    ctx: &ctx::Context,
    file: &mut file::RWFile,
    num_transactions: usize,
    offset: u64,
) {
    use std::io::{Seek, Write};

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut raw_file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .unwrap();
    raw_file.set_len(offset).unwrap();
    raw_file.seek(io::SeekFrom::Start(offset)).unwrap();

    let mut buffer = Vec::new();
    for transaction_id in 0..num_transactions {
        let ops = vec![LogOp::Free(FreeOp {
            page_group_id: PageGroupId(1),
        })];

        let associate_data = op_log_associated_data(
            file.id(),
            1,
            1 + transaction_id as u32,
            offset + buffer.len() as u64,
        );

        let mut tmp_buffer = Vec::new();
        log::encode_log_block(
            ctx.cipher(),
            &associate_data,
            transaction_id as u64,
            &ops,
            &mut tmp_buffer,
        )
        .unwrap();

        buffer.extend_from_slice(&tmp_buffer);
    }
    raw_file.write_all(&buffer).unwrap();
    raw_file.sync_all().unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn test_missing_header_processing(#[values(false, true)] encryption: bool) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let err = LogFileReader::open(ctx, file.into())
        .await
        .expect_err("log reader should not open due to missing metadata header");
    assert_eq!(err.to_string(), "missing metadata header");
}

#[rstest::rstest]
#[tokio::test]
async fn test_header_processing(#[values(false, true)] encryption: bool) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let header = MetadataHeader {
        log_file_id: 1,
        order_key: 50,
        encryption: ctx.get_encryption_status(),
    };
    setup_file_with_header(&ctx, &file, header).await;

    let reader = LogFileReader::open(ctx, file.into())
        .await
        .expect("log reader should process header");
    assert_eq!(reader.order_key(), 50);
    assert_eq!(
        format!("{reader:?}"),
        "LogFileReader(file_id=FileId(1000), log_file_id=1, order=50)",
    );
}

#[rstest::rstest]
#[tokio::test]
async fn test_header_encryption_missmatch() {
    let encoding_ctx = ctx::Context::for_test(false).await;
    let file = encoding_ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let header = MetadataHeader {
        log_file_id: 1,
        order_key: 5,
        encryption: encoding_ctx.get_encryption_status(),
    };
    setup_file_with_header(&encoding_ctx, &file, header).await;

    let tmp_dir = encoding_ctx.tmp_dir();
    let decoding_ctx = ctx::Context::for_test_in_dir(true, tmp_dir).await;
    let file_ids = decoding_ctx.directory().list_dir(FileGroup::Wal).await;

    let file = decoding_ctx
        .directory()
        .get_rw_file(FileGroup::Wal, file_ids[0])
        .await
        .unwrap();

    let err = LogFileReader::open(decoding_ctx, file.into())
        .await
        .expect_err("log reader should not open due to encryption mismatch");
    assert_eq!(
        err.to_string(),
        "file is not encrypted but system has encryption enabled"
    );
}

async fn setup_file_with_header(
    ctx: &ctx::Context,
    file: &file::RWFile,
    metadata_header: MetadataHeader,
) {
    let associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Wal);

    let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
    file_metadata::encode_metadata(
        ctx.cipher(),
        &associated_data,
        &metadata_header,
        &mut header_buffer[..],
    )
    .expect("encode header");
    file.write_buffer(&mut header_buffer, 0)
        .await
        .expect("Failed to write header");
}
