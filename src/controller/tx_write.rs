use super::storage::StorageController;
use crate::layout::PageGroupId;

/// A [StorageWriteTx] allows for performing multiple writes across multiple
/// page group IDs as part of a single transaction.
pub struct StorageWriteTx<'a> {
    transaction_id: u64,
    transaction_n_entries: u32,
    controller: &'a StorageController,

    ops: Vec<TxnOp>,
}

impl std::fmt::Debug for StorageWriteTx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageWriteTx(tx_id={}, num_ops={})",
            self.transaction_id, self.transaction_n_entries,
        )
    }
}

impl<'c> StorageWriteTx<'c> {
    /// Creates a new [StorageWriteTx].
    pub fn new(transaction_id: u64, controller: &'c StorageController) -> Self {
        Self {
            transaction_id,
            transaction_n_entries: 0,
            controller,
            ops: Vec::new(),
        }
    }

    /// Create a new group writer with a given length.
    pub async fn create_group_writer<'txn: 'c>(
        &'txn mut self,
        page_group_id: PageGroupId,
        len: u64,
    ) -> GroupWriter<'c, 'txn> {
        todo!()
    }

    /// Delete a group from the store.
    pub fn delete_group(&mut self, page_group_id: PageGroupId) {}

    /// Reassign the data from one [PageGroupId] to another [PageGroupId].
    pub fn reassign_group(
        &mut self,
        old_page_group_id: PageGroupId,
        new_page_group_id: PageGroupId,
    ) {
    }

    /// Commit the current transaction and ensure all operations are durable.
    pub async fn commit(mut self) {}
}

/// A writer for writing a single page group blob.
pub struct GroupWriter<'c, 'txn: 'c> {
    group: PageGroupId,
    expected_len: u64,
    txn: &'txn mut StorageWriteTx<'c>,
}

enum TxnOp {}
