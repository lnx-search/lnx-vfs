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
mod encryption_file;
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

use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use self::controller::{
    OpenStorageControllerError,
    PageDataWriter,
    ReadRef,
    StorageController,
};
pub use self::ctx::{Context, ContextBuilder};
use self::layout::PageGroupId;
pub use self::page_data::CreatePageFileError;
use self::page_data::ReadPageError;
pub use self::transaction::FileSystemTransaction;

/// Configuration options for the VFS components.
pub mod config {
    pub use crate::controller::{CacheConfig, WalConfig};
}

#[cfg(feature = "bench-internal")]
pub mod bench {
    pub use crate::cache::*;
    pub use crate::disk_allocator::*;
}

/// A virtual filesystem abstraction over underlying storage.
pub struct VirtualFileSystem {
    storage_controller: Arc<StorageController>,
}

impl VirtualFileSystem {
    /// Open the virtual file system using the given [Context].
    pub async fn open(ctx: Context) -> Result<Self, OpenStorageControllerError> {
        let ctx = Arc::new(ctx);
        let storage_controller = StorageController::open(ctx).await.map(Arc::new)?;
        Ok(Self { storage_controller })
    }

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
        range: impl RangeBounds<usize>,
    ) -> Result<ReadRef, ReadPageError> {
        let start = match range.start_bound() {
            Bound::Included(s) => Some(*s),
            Bound::Excluded(s) => Some(*s + 1),
            Bound::Unbounded => None,
        };

        let end = match range.end_bound() {
            Bound::Included(s) => Some(*s + 1),
            Bound::Excluded(s) => Some(*s),
            Bound::Unbounded => None,
        };

        self.storage_controller
            .read_group(PageGroupId(file_id), start, end)
            .await
    }
}
