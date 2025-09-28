use std::sync::Arc;

use crate::layout::log::{LogEntryHeader, LogOp};
use crate::layout::{PageFileId, PageGroupId, PageId, page_metadata};
use crate::page_op_log::writer::LogFileWriter;
use crate::{ctx, file};

mod e2e;
mod reader;
mod writer_associated_data;
mod writer_disk_layout;
mod writer_flow_control;

async fn write_log_entries(
    ctx: Arc<ctx::FileContext>,
    file: file::RWFile,
    num_entries: usize,
    metadata_ratio: f32,
) {
    let mut writer = LogFileWriter::create(ctx, file)
        .await
        .expect("Failed to create writer");

    for page_id in 0..num_entries {
        let entry = LogEntryHeader {
            transaction_id: fastrand::u64(..),
            transaction_n_entries: fastrand::u32(..),
            sequence_id: 0,
            page_file_id: PageFileId(1),
            page_id: PageId(page_id as u32),
            op: LogOp::Write,
        };

        let mut metadata = page_metadata::PageMetadata::null();
        metadata.id = PageId(page_id as u32);
        metadata.group = PageGroupId(1);

        if fastrand::f32() <= metadata_ratio {
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
}
