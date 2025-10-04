use std::mem;

use super::group_lock::{ConcurrentMutationError, GroupGuard};
use super::storage::StorageController;
use crate::controller::wal::WalError;
use crate::layout::log::{FreeOp, LogOp, ReassignOp, WriteOp};
use crate::layout::{PageGroupId, PageId};
use crate::page_data::SubmitWriterError;
use crate::page_file_allocator::WriteAllocTx;

#[derive(Debug, thiserror::Error)]
/// An error preventing the write transaction from completing.
pub enum StorageWriteError {
    #[error(transparent)]
    /// The target group is already being modified by another transaction.
    LockGroup(#[from] ConcurrentMutationError),
    #[error(transparent)]
    /// An error submitting the initial write IOPs.
    SubmitWrite(#[from] SubmitWriterError),
    #[error(transparent)]
    /// An error prevent the WAL from committing the transaction changes.
    Wal(#[from] WalError),
}

/// A [StorageWriteTxn] allows for performing multiple writes across multiple
/// page group IDs as part of a single transaction.
pub struct StorageWriteTxn<'c> {
    controller: &'c StorageController,
    ops: Vec<LogOp>,
    alloc_txs: Vec<WriteAllocTx<'c>>,
    guards: Vec<GroupGuard<'c>>,
    is_complete: bool,
}

impl std::fmt::Debug for StorageWriteTxn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageWriteTx")
    }
}

impl<'c> StorageWriteTxn<'c> {
    /// Creates a new [StorageWriteTxn].
    pub fn new(controller: &'c StorageController) -> Self {
        Self {
            controller,
            ops: Vec::new(),
            alloc_txs: Vec::new(),
            guards: Vec::new(),
            is_complete: false,
        }
    }

    /// Create a new populated writer.
    pub async fn add_writer(
        &mut self,
        page_group_id: PageGroupId,
        writer: super::page_file::PageDataWriter<'c>,
    ) -> Result<(), StorageWriteError> {
        self.try_lock_group(page_group_id)?;

        let page_file_id = writer.page_file_id();
        let (alloc_tx, mut altered_pages) = writer.finish().await?;

        // Connect the page change and metadata information the writer is not aware of.
        let mut i = 0;
        while i < altered_pages.len() {
            let next_page_id = altered_pages
                .get(i + 1)
                .map(|p| p.id)
                .unwrap_or(PageId::TERMINATOR);

            let page = &mut altered_pages[i];
            page.group = page_group_id;
            page.next_page_id = next_page_id;

            i += 1;
        }

        self.alloc_txs.push(alloc_tx);
        self.ops.push(LogOp::Write(WriteOp {
            page_file_id,
            page_group_id,
            altered_pages,
        }));

        Ok(())
    }

    /// Delete a group from the store.
    pub fn delete_group(
        &mut self,
        page_group_id: PageGroupId,
    ) -> Result<(), StorageWriteError> {
        self.try_lock_group(page_group_id)?;
        self.ops.push(LogOp::Free(FreeOp { page_group_id }));
        Ok(())
    }

    /// Reassign the data from one [PageGroupId] to another [PageGroupId].
    pub fn reassign_group(
        &mut self,
        old_page_group_id: PageGroupId,
        new_page_group_id: PageGroupId,
    ) -> Result<(), StorageWriteError> {
        self.try_lock_group(old_page_group_id)?;
        self.try_lock_group(new_page_group_id)?;
        self.ops.push(LogOp::Reassign(ReassignOp {
            old_page_group_id,
            new_page_group_id,
        }));
        Ok(())
    }

    /// Commit the current transaction and ensure all operations are durable.
    pub async fn commit(mut self) -> Result<(), StorageWriteError> {
        let alloc_txs = mem::take(&mut self.alloc_txs);
        let ops = mem::take(&mut self.ops);

        self.controller.commit_ops(ops).await?;

        // This branch only ever is hit *if* the data is persisted correctly.
        // - If there is an error and data was confirmed to be removed, then
        //   the commit call will error.
        // - If we _couldnt_ confirm the write hit disks or not, we will abort.
        for mut alloc in alloc_txs {
            alloc.commit();
        }

        Ok(())
    }

    /// Rollback the current transaction and abort.
    pub fn rollback(mut self) {
        self.is_complete = true;
    }

    fn try_lock_group(
        &mut self,
        page_group_id: PageGroupId,
    ) -> Result<(), ConcurrentMutationError> {
        let guard = self.controller.group_locks().try_lock(page_group_id)?;
        self.guards.push(guard);
        Ok(())
    }
}

impl<'c> Drop for StorageWriteTxn<'c> {
    fn drop(&mut self) {
        if !self.is_complete {
            tracing::warn!(
                num_ops = self.ops.len(),
                "transaction was dropped without being finalised via commit or rollback"
            );
        }
    }
}
