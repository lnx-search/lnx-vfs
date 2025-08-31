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
    #[values(0.0, 0.2, 0.8, 1.0)] with_metadata_ratio: f32,
    #[values(0, 1, 237, 473, 3230)] num_entries: usize,
) {
    fastrand::seed(7623572365235);

    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::create(ctx.clone(), file.clone())
        .await
        .expect("Failed to create writer");

    for page_id in 0..num_entries {
        let entry = log::LogEntry {
            transaction_id: fastrand::u64(..),
            transaction_n_entries: fastrand::u32(..),
            sequence_id: 0,
            page_file_id: PageFileId(1),
            page_id: PageId(page_id as u32),
            op: LogOp::Write,
        };

        let mut metadata = page_metadata::PageMetadata::empty();
        metadata.id = PageId(page_id as u32);
        metadata.group = PageGroupId(1);

        if fastrand::f32() < with_metadata_ratio {
            writer
                .write_log(entry, Some(metadata))
                .await
                .expect("failed to write log");
        } else {
            writer
                .write_log(entry, None)
                .await
                .expect("failed to write log");
        }
    }
    writer.sync().await.expect("failed to sync writer");
    drop(writer);

    let mut reader = LogFileReader::open(ctx.clone(), file.into())
        .await
        .expect("open reader on written file");

    let mut decoded_entries = 0;
    loop {
        match reader.next_block().await {
            Err(LogDecodeError::Decode(err)) => {
                tracing::error!(err = ?err, "failed to decode");
                continue;
            },
            Err(other) => panic!("got IO error: {other}"),
            Ok(Some(block)) => {
                decoded_entries += block.num_entries();
            },
            Ok(None) => break,
        }
    }
    assert_eq!(decoded_entries, num_entries);
}
