use rstest::rstest;

use crate::ctx;
use crate::directory::FileGroup;
use crate::file::DISK_ALIGN;
use crate::layout::log;
use crate::page_op_log::op_log_associated_data;
use crate::page_op_log::writer::LogFileWriter;

#[rstest]
#[tokio::test]
async fn test_single_transaction_correct_associated_data_tagging(
    #[values(0, 4096)] log_offset: u64,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, log_offset);

    writer.write_log(1, &Vec::new()).await.unwrap();
    writer.sync().await.unwrap();

    dbg!(writer.position());
    dbg!(log_offset);

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN + log_offset as usize);

    let buffer = &mut content[log_offset as usize..];
    let expected_associated_data = op_log_associated_data(file.id(), 1, 1, log_offset);
    let mut ops = Vec::new();
    log::decode_log_block(ctx.cipher(), &expected_associated_data, buffer, &mut ops)
        .expect("block should be decodable");
    assert!(ops.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_multi_transaction_correct_associated_data_tagging(
    #[values(0, 4096)] log_offset: u64,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
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
    assert_eq!(content.len(), DISK_ALIGN + log_offset as usize);

    for (txn_id, &pos) in positions.iter().enumerate() {
        let buffer = &mut content[pos as usize..];
        let expected_associated_data =
            op_log_associated_data(file.id(), 1, txn_id as u32, pos);
        let mut ops = Vec::new();
        log::decode_log_block(ctx.cipher(), &expected_associated_data, buffer, &mut ops)
            .expect("block should be decodable");
        assert!(ops.is_empty());
    }
}
