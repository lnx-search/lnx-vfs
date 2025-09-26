use std::sync::Arc;

use crate::directory::FileGroup;
use crate::layout::log::{LogEntry, LogOp};
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_op_log::LogFileWriter;
use crate::{ctx, file};

mod checkpoint;
mod metadata;
mod page_file;
mod wal;

fn make_log_entry(
    page_group_id: PageGroupId,
    page_id: PageId,
    transaction_id: u64,
    transaction_n_entries: u32,
    page_file_id: PageFileId,
    next_page_id: PageId,
) -> (LogEntry, PageGroupId, PageId) {
    let entry = LogEntry {
        transaction_id,
        transaction_n_entries,
        sequence_id: 0,
        page_file_id,
        page_id,
        op: LogOp::Write,
    };

    (entry, page_group_id, next_page_id)
}

async fn create_wal_file(ctx: &ctx::FileContext) -> file::RWFile {
    let file_id = ctx
        .directory()
        .create_new_file(FileGroup::Wal)
        .await
        .unwrap();
    ctx.directory()
        .get_rw_file(FileGroup::Wal, file_id)
        .await
        .unwrap()
}

async fn write_log_entries(
    ctx: Arc<ctx::FileContext>,
    file: file::RWFile,
    entries: &[(LogEntry, PageGroupId, PageId)],
    revision: u32,
) {
    let mut writer = LogFileWriter::create(ctx, file)
        .await
        .expect("Failed to create writer");

    for (entry, group, next_page_id) in entries {
        let mut metadata = PageMetadata::null();
        metadata.id = entry.page_id;
        metadata.next_page_id = *next_page_id;
        metadata.group = *group;
        metadata.revision = revision;

        writer
            .write_log(*entry, Some(metadata))
            .await
            .expect("failed to write log");
    }
    writer.sync().await.expect("failed to sync writer");
    drop(writer);
}
