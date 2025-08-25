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
    async fn create_writer(&self, fp: &str) {
        todo!()
    }

    async fn create_reader(&self, fp: &str) {
        todo!()
    }

    async fn read(&self, fp: &str, range: Range<u64>) {
        todo!()
    }

    async fn write(&self, fp: &str, data: &[u8]) {
        todo!()
    }

    async fn remove(&self, fp: &str) {
        todo!()
    }

    async fn rename(&self, old_fp: &str, new_fp: &str) {
        todo!()
    }
}

impl Drop for FileSystemTransaction {
    fn drop(&mut self) {
        if !self.complete {
            tracing::warn!("transaction was aborted without explicitly calling commit or rollback");
        }
    }
}