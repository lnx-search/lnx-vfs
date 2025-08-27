mod arena;
mod buffer;
mod core;
mod directory;
mod file;
mod layout;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod transaction;
mod utils;

use std::ops::Range;

pub use self::core::FileSystemCore;
pub use self::transaction::FileSystemTransaction;

/// A virtual filesystem abstraction over underlying storage.
pub struct VirtualFileSystem {}

impl VirtualFileSystem {
    /// Begin a new [FileSystemTransaction] for applying multiple
    /// operations atomically.
    pub fn begin(&self) -> FileSystemTransaction {
        todo!()
    }
}

impl FileSystemCore for VirtualFileSystem {
    async fn create_writer(&self, file_id: u64) {
        todo!()
    }

    async fn create_reader(&self, file_id: u64) {
        todo!()
    }

    async fn read(&self, file_id: u64, range: Range<u64>) {
        todo!()
    }

    async fn write(&self, file_id: u64, data: &[u8]) {
        todo!()
    }

    async fn remove(&self, file_id: u64) {
        todo!()
    }

    async fn rename(&self, new_file_id: u64, old_file_id: u64) {
        todo!()
    }
}
