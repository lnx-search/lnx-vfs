#![cfg_attr(feature = "bench-lib-unstable", feature(test))]

mod arena;
mod buffer;
mod cache;
mod checkpoint;
pub mod coalesce;
mod controller;
mod core;
mod ctx;
mod directory;
mod disk_allocator;
mod encryption_key;
mod file;
mod layout;
mod page_data;
mod page_file_allocator;
mod page_op_log;
mod stream_reader;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod transaction;
mod utils;

use std::ops::Range;
use std::sync::Arc;

use self::controller::{PageDataWriter, ReadRef, StorageController};
use self::layout::PageGroupId;
pub use self::page_data::CreatePageFileError;
use self::page_data::ReadPageError;
pub use self::ctx::Context;
pub use self::transaction::FileSystemTransaction;

#[cfg(feature = "bench-internal")]
pub mod bench {
    pub use crate::cache::*;
}


/// A virtual filesystem abstraction over underlying storage.
pub struct VirtualFileSystem {
    ctx: Arc<Context>,
    storage_controller: Arc<StorageController>,
}

impl VirtualFileSystem {
    
    
    /// Begin a new [FileSystemTransaction] for applying multiple
    /// operations atomically.
    pub fn begin(&self) -> FileSystemTransaction<'_> {
        let txn = self.storage_controller.create_write_txn();
        FileSystemTransaction { parent: self, txn }
    }

    /// Create a new writer which allows for incremental writing of a large file
    /// with a known total length.
    pub async fn create_writer(
        &self,
        len: u64,
    ) -> Result<PageDataWriter<'_>, CreatePageFileError> {
        self.storage_controller.create_writer(len).await
    }

    /// Read the file at a given range.
    pub async fn read_file(
        &self,
        file_id: u64,
        range: Range<usize>,
    ) -> Result<ReadRef, ReadPageError> {
        self.storage_controller
            .read_group(PageGroupId(file_id), range)
            .await
    }
}
