use crate::layout::page_metadata::PageMetadata;
use super::storage::StorageController;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::SubmitWriterError;

/// A [StorageWriteTx] allows for performing multiple writes across multiple
/// page group IDs as part of a single transaction.
pub struct StorageWriteTx<'c> {
    transaction_id: u64,
    controller: &'c StorageController,
    ops: Vec<TxnOp<'c>>,
}

impl std::fmt::Debug for StorageWriteTx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageWriteTx(tx_id={})",
            self.transaction_id,
        )
    }
}

impl<'c> StorageWriteTx<'c> {
    /// Creates a new [StorageWriteTx].
    pub fn new(transaction_id: u64, controller: &'c StorageController) -> Self {
        Self {
            transaction_id,
            controller,
            ops: Vec::new(),
        }
    }

    /// Create a new populated writer.
    pub async fn add_writer(
        &mut self,
        page_group_id: PageGroupId,
        writer: super::page_file::PageDataWriter<'c>,
    ) -> Result<(), SubmitWriterError> {
        let page_file_id = writer.page_file_id();
        let (alloc_tx, mut pages_altered) = writer.finish().await?;

        // Connect the page change and metadata information the writer is not aware of.
        let mut i = 0;
        while i < pages_altered.len() {
            let next_page_id = pages_altered
                .get(i + 1)
                .map(|p| p.id)
                .unwrap_or(PageId::TERMINATOR);

            let page = &mut pages_altered[i];
            page.group = page_group_id;
            page.next_page_id = next_page_id;
            // TODO: Must add! page.revision = revision;

            i += 1;
        }

        self.ops.push(TxnOp::Write {
            page_file_id,
            alloc_tx,
            pages_altered,
        });

        Ok(())
    }

    /// Delete a group from the store.
    pub fn delete_group(&mut self, page_group_id: PageGroupId) {
        self.ops.push(TxnOp::Delete { page_group_id });
    }

    /// Reassign the data from one [PageGroupId] to another [PageGroupId].
    pub fn reassign_group(
        &mut self,
        old_page_group_id: PageGroupId,
        new_page_group_id: PageGroupId,
    ) {
        self.ops.push(TxnOp::Reassign { old_page_group_id, new_page_group_id });
    }

    /// Commit the current transaction and ensure all operations are durable.
    pub async fn commit(mut self) {
        todo!("implement wall write");
    }
}

enum TxnOp<'c> {
    Write {
        page_file_id: PageFileId,
        alloc_tx: crate::page_file_allocator::WriteAllocTx<'c>,
        pages_altered: Vec<PageMetadata>,
    },
    Reassign {
        old_page_group_id: PageGroupId,
        new_page_group_id: PageGroupId,
    },
    Delete {
        page_group_id: PageGroupId,
    }
}
