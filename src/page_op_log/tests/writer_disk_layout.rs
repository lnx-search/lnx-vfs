use crate::ctx;
use crate::directory::FileGroup;
use crate::file::DISK_ALIGN;
use crate::layout::log::{LogEntry, LogOp};
use crate::layout::{PageFileId, PageId, file_metadata, log};
use crate::page_op_log::writer::LogFileWriter;
use crate::page_op_log::{MetadataHeader, op_log_associated_data};

#[rstest::rstest]
#[tokio::test]
async fn test_single_entry_write_layout(#[values(0, 4096)] log_offset: usize) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, log_offset as u64);

    let entry = LogEntry {
        sequence_id: 1,
        transaction_id: 6,
        transaction_n_entries: 7,
        page_id: PageId(1),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };
    writer.write_log(entry, None).await.unwrap();
    writer.sync().await.unwrap();

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN + log_offset);

    let expected_buffer = &mut content[log_offset..log_offset + log::LOG_BLOCK_SIZE];
    let block = log::decode_log_block(
        None,
        &op_log_associated_data(file.id(), 1, log_offset as u64),
        expected_buffer,
    )
    .expect("block should be decodable");
    assert_eq!(block.num_entries(), 1);

    let entries = block.entries();
    assert_eq!(entries[0].log, entry);
}

#[tokio::test]
async fn test_multiple_block_write_layout() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, 0);

    for page_id in 0..15 {
        let entry = LogEntry {
            sequence_id: 1,
            transaction_id: 6,
            transaction_n_entries: 7,
            page_id: PageId(page_id),
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.unwrap();
    }
    writer.sync().await.unwrap();

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN);

    let indices = [
        0..log::LOG_BLOCK_SIZE,
        log::LOG_BLOCK_SIZE..log::LOG_BLOCK_SIZE * 2,
    ];
    let [buffer1, buffer2] = content.get_disjoint_mut(indices).unwrap();

    let block1 =
        log::decode_log_block(None, &op_log_associated_data(file.id(), 1, 0), buffer1)
            .expect("block should be decodable");
    let block2 = log::decode_log_block(
        None,
        &op_log_associated_data(file.id(), 1, log::LOG_BLOCK_SIZE as u64),
        buffer2,
    )
    .expect("block should be decodable");
    assert_eq!(block1.num_entries(), 11);
    assert_eq!(block2.num_entries(), 4);

    let entries = block1.entries();
    assert_eq!(
        entries[0].log,
        LogEntry {
            sequence_id: 1,
            transaction_id: 6,
            transaction_n_entries: 7,
            page_id: PageId(0),
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        }
    );

    let entries = block2.entries();
    assert_eq!(
        entries[0].log,
        LogEntry {
            sequence_id: 12,
            transaction_id: 6,
            transaction_n_entries: 7,
            page_id: PageId(11),
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        }
    );
}

#[tokio::test]
async fn test_multiple_pages_write_layout() {
    const NUM_BLOCKS: usize = 9;

    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, 0);

    for page_id in 0..NUM_BLOCKS * 11 {
        let entry = LogEntry {
            sequence_id: 1,
            transaction_id: 6,
            transaction_n_entries: 7,
            page_id: PageId(page_id as u32),
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.unwrap();
    }
    writer.sync().await.unwrap();

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN * 2);

    for block_id in 0..NUM_BLOCKS {
        let buffer_start = block_id * log::LOG_BLOCK_SIZE;
        let buffer = &mut content[buffer_start..][..log::LOG_BLOCK_SIZE];

        let block = log::decode_log_block(
            None,
            &op_log_associated_data(file.id(), 1, buffer_start as u64),
            buffer,
        )
        .expect("block should be decodable");
        assert_eq!(block.num_entries(), 11);

        let entries = block.entries();
        assert_eq!(
            entries[0].log,
            LogEntry {
                sequence_id: ((block_id * 11) + 1) as u32,
                transaction_id: 6,
                transaction_n_entries: 7,
                page_id: PageId((block_id * 11) as u32),
                page_file_id: PageFileId(1),
                op: LogOp::Free,
            }
        );
    }
}

#[rstest::rstest]
#[tokio::test]
async fn test_header_writing(#[values(false, true)] encryption: bool) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let writer = LogFileWriter::create(ctx.clone(), file.clone())
        .await
        .expect("write log file with new header");
    drop(writer);

    let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
    file.read_buffer(&mut header_buffer, 0).await.unwrap();

    let associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Wal);

    let header: MetadataHeader = file_metadata::decode_metadata(
        ctx.cipher(),
        &associated_data,
        &mut header_buffer[..file_metadata::HEADER_SIZE],
    )
    .expect("writer should write valid header");
    assert_eq!(header.encryption, ctx.get_encryption_status());
    assert_ne!(header.log_file_id, 0);
}
