use rstest::rstest;

use crate::ctx;
use crate::directory::FileGroup;
use crate::file::DISK_ALIGN;
use crate::layout::log::{HEADER_SIZE, LogOp};
use crate::layout::{encrypt, log};
use crate::page_op_log::op_log_associated_data;
use crate::page_op_log::writer::LogFileWriter;

#[rstest]
#[tokio::test]
async fn test_single_transaction_correct_associated_data_tagging(
    #[values(0, 4096)] log_offset: u64,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, log_offset);

    writer.write_log(1, &Vec::new()).await.unwrap();
    writer.sync().await.unwrap();

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN + log_offset as usize);

    let buffer = &mut content[log_offset as usize..];
    let expected_associated_data = op_log_associated_data(file.id(), 1, 1, log_offset);

    let ops = decode_ops(ctx.cipher(), &expected_associated_data, buffer, 1);
    assert_eq!(ops.len(), 0);
}

#[rstest]
#[tokio::test]
async fn test_multi_transaction_correct_associated_data_tagging(
    #[values(0, 4096)] log_offset: u64,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, log_offset);

    let mut positions = Vec::new();
    for transaction_id in 0..15 {
        positions.push(writer.position());
        writer.write_log(transaction_id, &Vec::new()).await.unwrap();
    }
    writer.sync().await.unwrap();

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN * 2 + log_offset as usize);

    for (txn_id, &pos) in positions.iter().enumerate() {
        let buffer = &mut content[pos as usize..];
        let expected_associated_data =
            op_log_associated_data(file.id(), 1, 1 + txn_id as u32, pos);

        let ops = decode_ops(
            ctx.cipher(),
            &expected_associated_data,
            buffer,
            txn_id as u64,
        );
        assert!(ops.is_empty(), "oofies?");
    }
}

#[track_caller]
fn decode_ops(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
    expected_txn_id: u64,
) -> Vec<LogOp> {
    let (txn_id, buffer_len) = log::decode_log_header(cipher, associated_data, buffer)
        .expect("decode log header");
    assert_eq!(txn_id, expected_txn_id);

    let mut ops = Vec::new();
    log::decode_log_block(
        cipher,
        associated_data,
        &mut buffer[HEADER_SIZE..][..buffer_len],
        &mut ops,
    )
    .expect("block should be decodable");
    ops
}
