use std::sync::Arc;

use crate::directory::FileGroup;
use crate::layout::PageGroupId;
use crate::layout::log::{FreeOp, LogOp};
use crate::page_op_log::LogFileWriter;
use crate::{ctx, file};

mod checkpoint;
mod metadata;
mod page_file;
mod storage;
mod wal;

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

async fn write_log_ops(
    ctx: Arc<ctx::FileContext>,
    file: file::RWFile,
    ops: &[(u64, Vec<LogOp>)],
) {
    let mut writer = LogFileWriter::create(ctx, file)
        .await
        .expect("Failed to create writer");

    for (transaction_id, ops) in ops {
        writer
            .write_log(*transaction_id, ops)
            .await
            .expect("failed to write log");
    }
    writer.sync().await.expect("failed to sync writer");
    drop(writer);
}

fn create_sample_ops(num_ops: usize) -> Vec<LogOp> {
    let mut ops = Vec::with_capacity(num_ops);
    for id in 0..num_ops {
        ops.push(LogOp::Free(FreeOp {
            page_group_id: PageGroupId(id as u64),
        }));
    }
    ops
}
