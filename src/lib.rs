#![cfg_attr(feature = "bench-lib-unstable", feature(test))]

mod arena;
mod buffer;
mod cache;
mod checkpoint;
mod core;
mod ctx;
mod directory;
mod disk_allocator;
mod encryption_key;
mod file;
mod layout;
mod page_data;
mod page_op_log;
mod stream_reader;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod transaction;
mod utils;
mod write_controller;

use std::ops::Range;

#[cfg(feature = "bench-internal")]
pub mod bench {
    pub use crate::cache::*;
}

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
    async fn create_writer(&self, _file_id: u64, _total_size: u64) {
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
