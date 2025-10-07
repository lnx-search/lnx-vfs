use crate::ctx;
use crate::directory::FileGroup;
use crate::file::DISK_ALIGN;
use crate::layout::log::{HEADER_SIZE, LogOp, WriteOp};
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId, file_metadata, log};
use crate::page_op_log::writer::LogFileWriter;
use crate::page_op_log::{MetadataHeader, op_log_associated_data};

#[rstest::rstest]
#[tokio::test]
async fn test_single_transaction_write_layout(#[values(0, 4096)] log_offset: usize) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut writer = LogFileWriter::new(ctx.clone(), file.clone(), 1, log_offset as u64);

    let ops = vec![LogOp::Write(WriteOp {
        page_file_id: PageFileId(0),
        page_group_id: PageGroupId(1),
        altered_pages: vec![PageMetadata::unassigned(PageId(1))],
    })];

    writer.write_log(999, &ops).await.unwrap();
    writer.sync().await.unwrap();

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN + log_offset);

    let associated_data = op_log_associated_data(file.id(), 1, 1, log_offset as u64);

    let buffer = &mut content[log_offset..];
    let (transaction_id, buffer_len) =
        log::decode_log_header(None, &associated_data, buffer)
            .expect("decode log header");
    assert_eq!(transaction_id, 999);

    let mut ops = Vec::new();
    log::decode_log_block(
        None,
        &associated_data,
        &mut buffer[HEADER_SIZE..][..buffer_len],
        &mut ops,
    )
    .expect("block should be decodable");
    assert_eq!(ops.len(), 1);
}

#[rstest::rstest]
#[tokio::test]
async fn test_header_writing(#[values(false, true)] encryption: bool) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let writer = LogFileWriter::create(ctx.clone(), file.clone())
        .await
        .expect("write log file with new header");
    drop(writer);

    let mut header_bytes = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
    file.read_buffer(&mut header_bytes, 0).await.unwrap();

    let associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Wal);

    let header: MetadataHeader = file_metadata::decode_metadata(
        ctx.cipher(),
        &associated_data,
        &mut header_bytes[..file_metadata::HEADER_SIZE],
    )
    .expect("writer should write valid header");
    assert_eq!(header.encryption, ctx.get_encryption_status());
    assert_ne!(header.log_file_id, 0);
}
