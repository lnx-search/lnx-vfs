use crate::CreatePageFileError;
use crate::controller::{PageDataWriter, StorageWriteError};
use crate::layout::PageGroupId;
use crate::page_data::SubmitWriterError;

#[derive(Debug, thiserror::Error)]
/// An error preventing the transaction from completing.
pub enum TxnError {
    #[error(transparent)]
    /// The system could not create a new page file to complete the write.
    CreatePageFile(#[from] CreatePageFileError),
    #[error(transparent)]
    /// The system could not write the provided data.
    StorageWrite(#[from] StorageWriteError),
    #[error(transparent)]
    /// The system could not submit the write.
    SubmitWrite(#[from] SubmitWriterError),
}

/// A [FileSystemTransaction] allows you to modify multiple files in the filesystem atomically.
pub struct FileSystemTransaction<'c> {
    pub(super) parent: &'c crate::VirtualFileSystem,
    pub(super) txn: crate::controller::StorageWriteTxn<'c>,
}

impl<'c> FileSystemTransaction<'c> {
    /// Apply the current pending operations.
    pub async fn commit(self) -> Result<(), StorageWriteError> {
        self.txn.commit().await
    }

    /// Rollback/abort the current pending operations.
    pub fn rollback(self) {
        self.txn.rollback()
    }

    /// A completed writer to the transaction under the given file ID.
    ///
    /// This will overwrite any existing file.
    pub async fn add_writer(
        &mut self,
        file_id: u64,
        writer: PageDataWriter<'c>,
    ) -> Result<(), TxnError> {
        self.txn
            .add_writer(PageGroupId(file_id), writer)
            .await
            .map_err(TxnError::StorageWrite)
    }

    /// Persist the given buffer and assign it the given file ID.
    ///
    /// This will overwrite any existing file.
    pub async fn write(&mut self, file_id: u64, data: &[u8]) -> Result<(), TxnError> {
        let mut writer = self.parent.create_writer(data.len() as u64).await?;
        writer.write(data).await?;
        self.add_writer(file_id, writer).await
    }

    #[inline]
    /// Remove the target file.
    ///
    /// If the file does not exist this is a no-op.
    pub fn remove(&mut self, file_id: u64) -> Result<(), TxnError> {
        self.txn
            .unassign_group(PageGroupId(file_id))
            .map_err(TxnError::StorageWrite)
    }

    #[inline]
    /// Remove the target file.
    ///
    /// If the `old_file_id` does not exist, this is a no-op.
    pub fn rename(
        &mut self,
        old_file_id: u64,
        new_file_id: u64,
    ) -> Result<(), TxnError> {
        self.txn
            .reassign_group(PageGroupId(old_file_id), PageGroupId(new_file_id))
            .map_err(TxnError::StorageWrite)
    }
}
