use std::sync::Arc;

use crate::layout::log::{LogOp, WriteOp};
use crate::layout::{PageFileId, PageGroupId, PageId, page_metadata};
use crate::page_op_log::writer::LogFileWriter;
use crate::{ctx, file};

mod e2e;
mod reader;
mod writer_associated_data;
mod writer_disk_layout;
mod writer_flow_control;

async fn write_log_entries(
    ctx: Arc<ctx::Context>,
    file: file::RWFile,
    num_entries: usize,
) {
    let mut writer = LogFileWriter::create(ctx, file)
        .await
        .expect("Failed to create writer");

    for page_id in 0..num_entries {
        let mut metadata = page_metadata::PageMetadata::null();
        metadata.id = PageId(page_id as u32);
        metadata.group = PageGroupId(1);

        let ops = vec![LogOp::Write(WriteOp {
            page_file_id: PageFileId(0),
            page_group_id: PageGroupId(1),
            altered_pages: vec![metadata],
        })];
        writer.write_log(page_id as u64, &ops).await.unwrap();
    }
    writer.sync().await.expect("failed to sync writer");
    drop(writer);
}
