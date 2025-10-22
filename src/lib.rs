#![cfg_attr(feature = "bench-lib-unstable", feature(test))]

mod arena;
mod buffer;
pub mod cache;
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
#[cfg(all(test, not(miri)))]
mod tests;
mod transaction;
mod utils;

use std::collections::Bound;
use std::mem;
use std::ops::RangeBounds;
use std::sync::Arc;

use self::controller::{
    OpenStorageControllerError,
    PageDataWriter,
    ReadRef,
    StorageController,
};
pub use self::ctx::{Context, ContextBuilder};
pub use self::encryption_file::change_password;
use self::layout::PageGroupId;
pub use self::page_data::CreatePageFileError;
use self::page_data::ReadPageError;
pub use self::transaction::FileSystemTransaction;

/// Configuration options for the VFS components.
pub mod config {
    pub use crate::controller::{CacheConfig, StorageConfig, WalConfig};
    pub use crate::page_data::PageFileConfig;
}

#[cfg(feature = "bench-internal")]
pub mod bench {
    pub use crate::cache::*;
    pub use crate::disk_allocator::*;
}

#[derive(Clone)]
/// A virtual filesystem abstraction over underlying storage.
pub struct VirtualFileSystem {
    storage_controller: Arc<StorageController>,
}

impl VirtualFileSystem {
    /// Open the virtual file system using the given [Context].
    pub async fn open(ctx: Context) -> Result<Self, OpenStorageControllerError> {
        let ctx = Arc::new(ctx);
        let storage_controller = StorageController::open(ctx).await?;
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
    pub fn read_file(
        &self,
        file_id: u64,
        range: impl RangeBounds<usize>,
    ) -> impl Future<Output = Result<ReadRef, ReadPageError>> + '_ {
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
    }

    /// Returns if the file system contains the given file ID.
    pub fn exists(&self, file_id: u64) -> bool {
        self.storage_controller
            .contains_page_group(PageGroupId(file_id))
    }

    /// List all files within the file system.
    pub fn list_files(&self) -> Vec<u64> {
        self.find_files(|_| true)
    }

    /// Find & collect all files that match the provided predicate.
    pub fn find_files<F>(&self, mut predicate: F) -> Vec<u64>
    where
        F: FnMut(u64) -> bool,
    {
        let groups = self
            .storage_controller
            .find_groups(|group| predicate(group.0));

        let mut groups = mem::ManuallyDrop::new(groups);
        let ptr = groups.as_mut_ptr();
        let len = groups.len();
        let capacity = groups.capacity();

        // SAFETY: PageGroupId is repr(transparent)
        unsafe { Vec::from_raw_parts(ptr.cast(), len, capacity) }
    }
}
