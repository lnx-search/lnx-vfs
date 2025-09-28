use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::log::LogOp;
use crate::layout::{PageFileId, PageGroupId, PageId, log, page_metadata};
use crate::page_op_log::reader::{LogDecodeError, LogFileReader};
use crate::page_op_log::writer::LogFileWriter;

#[rstest::rstest]
#[tokio::test]
async fn test_reader_can_decode_writer_output(
    #[values(false, true)] encryption: bool,
    #[values(0, 1, 237, 473, 1_000, 3230)] num_entries: usize,
) {
    fastrand::seed(7623572365235);

    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    super::write_log_entries(ctx.clone(), file.clone(), num_entries).await;

    let mut reader = LogFileReader::open(ctx.clone(), file.into())
        .await
        .expect("open reader on written file");

    let mut ops = Vec::new();
    let mut decoded_entries = 0;
    loop {
        ops.clear();
        match reader.next_transaction(&mut ops).await {
            Err(LogDecodeError::Decode(err)) => {
                tracing::error!(err = ?err, "failed to decode");
                continue;
            },
            Err(other) => panic!("got IO error: {other}"),
            Ok(Some(_txn_id)) => {
                decoded_entries += 1;
            },
            Ok(None) => break,
        }
    }
    assert_eq!(decoded_entries, num_entries);
}
