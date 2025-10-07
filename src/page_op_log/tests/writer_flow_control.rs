use std::io;
use std::io::ErrorKind;

use crate::directory::FileGroup;
use crate::file::DISK_ALIGN;
use crate::layout::log::{FreeOp, LogOp, ReassignOp, WriteOp};
use crate::layout::{PageFileId, PageGroupId};
use crate::page_op_log::writer::LogFileWriter;
use crate::{ctx, utils};

#[rstest::rstest]
#[case(2_000, 512 << 10)]
#[case(1_050, 512 << 10)]
#[case(500, 0)]
#[tokio::test]
async fn test_auto_flush(#[case] num_transactions: u64, #[case] expected_size: u64) {
    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let initial_len = file.get_len().await.unwrap();
    assert_eq!(initial_len, 0);

    let mut writer = LogFileWriter::new(ctx, file, 1, 0);

    let mut ops = Vec::new();
    for _ in 0..4 {
        ops.push(LogOp::Write(WriteOp {
            page_file_id: PageFileId(0),
            page_group_id: PageGroupId(1),
            altered_pages: vec![],
        }));
        ops.push(LogOp::Free(FreeOp {
            page_group_id: PageGroupId(1),
        }));
        ops.push(LogOp::Reassign(ReassignOp {
            old_page_group_id: PageGroupId(1),
            new_page_group_id: PageGroupId(2),
        }));
    }

    for txn_id in 0..num_transactions {
        writer.write_log(txn_id, &ops).await.expect("write entry");
    }

    // Inflight IOP might take a little bit.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let file = writer.into_file();
    let post_write_len = file.get_len().await.unwrap();
    assert_eq!(post_write_len, expected_size); // Flush single mem buffer.
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_all_entries_flush(
    #[values(true, false)] encryption: bool,
    #[values(1, 4, 8, 32)] number_of_entries: usize,
) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let initial_len = file.get_len().await.unwrap();
    assert_eq!(initial_len, 0);

    let mut writer = LogFileWriter::new(ctx, file, 1, 0);

    for id in 0..number_of_entries {
        writer
            .write_log(id as u64, &Vec::new())
            .await
            .expect("write log");
    }

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    writer.sync().await.expect("flush");

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    let file = writer.into_file();
    let post_flush_len = file.get_len().await.unwrap();
    assert_eq!(
        post_flush_len,
        utils::align_up(number_of_entries * 512, DISK_ALIGN) as u64,
    );
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_entries_and_metadata(
    #[values(false, true)] encryption: bool,
    #[values(1, 4, 8, 32)] number_of_entries: usize,
) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let initial_len = file.get_len().await.unwrap();
    assert_eq!(initial_len, 0);

    let mut writer = LogFileWriter::new(ctx, file, 1, 0);

    for id in 0..number_of_entries {
        writer
            .write_log(id as u64, &Vec::new())
            .await
            .expect("write log");
    }

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    writer.sync().await.expect("flush");

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    let file = writer.into_file();
    let post_flush_len = file.get_len().await.unwrap();
    assert_eq!(
        post_flush_len,
        utils::align_up(number_of_entries * 512, DISK_ALIGN) as u64,
    );
}

#[tokio::test]
async fn test_no_close_on_write_error_but_lockout() {
    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return").unwrap();

    let mut writer = LogFileWriter::new(ctx, file, 1, 0);
    writer
        .sync()
        .await
        .expect("sync should not error as no data was written");

    writer
        .write_log(1, &Vec::new())
        .await
        .expect("write log should succeed");

    let error = writer.sync().await.expect_err("write should error");
    assert_eq!(error.kind(), ErrorKind::Other);

    let err = writer
        .write_log(1, &Vec::new())
        .await
        .expect_err("write should return lockout");
    assert_eq!(err.kind(), ErrorKind::ReadOnlyFilesystem);
    assert_eq!(err.to_string(), "writer is locked due to prior error");

    scenario.teardown();
}

#[tokio::test]
async fn test_flush_mem_buffer_i2o2_error() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", "return(-12)").unwrap();

    let mut writer = LogFileWriter::new(ctx, file, 1, 0);

    let error = fill_buffer(&mut writer)
        .await
        .expect_err("write should error");
    assert_eq!(error.kind(), ErrorKind::OutOfMemory);

    assert!(writer.is_locked_out());

    scenario.teardown();
}

#[tokio::test]
async fn test_storage_full() {
    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", "return(20)").unwrap();

    let mut writer = LogFileWriter::new(ctx, file, 1, 0);

    writer.write_log(1, &Vec::new()).await.unwrap();
    let error = writer
        .sync()
        .await
        .expect_err("sync should error as data is flushed to disk");
    assert_eq!(error.kind(), ErrorKind::StorageFull);

    assert!(writer.is_locked_out());

    scenario.teardown();
}

#[rstest::rstest]
#[case::zero_offset(0)]
#[should_panic(expected = "log offset must be a multiple of the disk alignment")]
#[case::unaligned_offset1(13)]
#[should_panic(expected = "log offset must be a multiple of the disk alignment")]
#[case::unaligned_offset2(1024)]
#[case::aligned_offset1(4096)]
#[case::aligned_offset2(4096 * 3)]
#[tokio::test]
async fn test_log_offset(#[case] log_offset: u64) {
    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;
    let _writer = LogFileWriter::new(ctx, file, 1, log_offset);
}

async fn fill_buffer(writer: &mut LogFileWriter) -> io::Result<()> {
    for id in 0..8_000 {
        writer.write_log(id as u64, &Vec::new()).await?;
    }

    Ok(())
}
