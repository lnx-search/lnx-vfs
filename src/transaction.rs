use std::ops::Range;

use crate::core::FileSystemCore;

/// A [FileSystemTransaction] allows you to modify multiple files in the filesystem atomically.
pub struct FileSystemTransaction {
    complete: bool,
}

impl FileSystemTransaction {
    /// Apply the current pending operations.
    pub async fn commit(mut self) -> Result<(), ()> {
        self.complete = true;
        Ok(())
    }

    /// Rollback/abort the current pending operations.
    pub fn rollback(mut self) {
        self.complete = true;
    }
}

impl FileSystemCore for FileSystemTransaction {
    async fn create_writer(&self, _file_id: u64) {
        todo!()
    }

    async fn create_reader(&self, _file_id: u64) {
        todo!()
    }

    async fn read(&self, _file_id: u64, _range: Range<u64>) {
        todo!()
    }

    async fn write(&self, _file_id: u64, _data: &[u8]) {
        todo!()
    }

    async fn remove(&self, _file_id: u64) {
        todo!()
    }

    async fn rename(&self, _new_file_id: u64, _old_file_id: u64) {
        todo!()
    }
}

impl Drop for FileSystemTransaction {
    fn drop(&mut self) {
        if !self.complete {
            tracing::warn!(
                "transaction was aborted without explicitly calling commit or rollback"
            );
        }
    }
}
