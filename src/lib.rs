mod core;
mod transaction;

use std::ops::Range;

pub use self::core::FileSystemCore;
pub use self::transaction::FileSystemTransaction;

/// A virtual filesystem abstraction over underlying storage.
pub struct VirtualFileSystem {

}

impl VirtualFileSystem {
    /// Begin a new [FileSystemTransaction] for applying multiple
    /// operations atomically.
    pub fn begin(&self) -> FileSystemTransaction {
        todo!()
    }
}

impl FileSystemCore for VirtualFileSystem {
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


