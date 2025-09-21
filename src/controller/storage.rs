use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, atomic};

use super::metadata::{LookupEntry, MetadataController, OpenMetadataControllerError};
use super::wal::{WalController, WalError};
use crate::checkpoint::WriteCheckpointError;
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::PageGroupId;

#[derive(Debug, thiserror::Error)]
/// An error preventing the [StorageController] from
pub enum OpenStorageControllerError {
    #[error(transparent)]
    /// The controller could not open the [MetadataController].
    MetadataOpen(#[from] OpenMetadataControllerError),
    #[error(transparent)]
    /// The controller could not create a new checkpoint after recovering the WAL.
    WriteCheckpoint(#[from] WriteCheckpointError),
    #[error("cannot cleanup WAL files: {0}")]
    /// The controller could not remove existing WAL files after checkpointing data.
    CleanupWalFiles(io::Error),
    #[error(transparent)]
    /// The WAL controller could not be created.
    WalError(#[from] WalError),
}

/// The [StorageController] manages persisting updates to pages of data.
pub struct StorageController {
    metadata_controller: MetadataController,
    wal_controller: WalController,
    transaction_id_counter: AtomicU64,
}

impl StorageController {
    /// Open the storage controller with a given context.
    ///
    /// This will automatically recover any existing state from the configured
    /// storage directory and checkpoint any WAL files before returning.
    pub async fn open(
        ctx: Arc<ctx::FileContext>,
    ) -> Result<Self, OpenStorageControllerError> {
        let metadata_controller = MetadataController::open(ctx.clone()).await?;

        // Checkpoint the controller so we can clean up any existing WAL files.
        metadata_controller.checkpoint().await?;

        // Remove any existing WAL files before we create the WAL controller otherwise
        // we will end up old WAL files being applied in the wrong order.
        remove_all_wal_files(&ctx)
            .await
            .map_err(OpenStorageControllerError::CleanupWalFiles)?;

        let wal_controller = WalController::create(ctx.clone()).await?;

        Ok(Self {
            metadata_controller,
            wal_controller,
            transaction_id_counter: AtomicU64::new(0),
        })
    }

    /// Create a new storage write transaction.
    pub fn create_write_tx(&self) -> StorageWriteTx<'_> {
        let transaction_id = self.transaction_id_counter.fetch_add(1, Ordering::Relaxed);
        StorageWriteTx {
            transaction_id,
            transaction_n_entries: 0,
            controller: self,
        }
    }

    /// Create a new reader for a given [PageGroupId].
    pub fn create_reader(&self, group: PageGroupId) -> Option<StorageReader<'_>> {
        let lookup = self.metadata_controller.find_first_page(group)?;
        Some(StorageReader {
            group,
            lookup,
            controller: self,
        })
    }
}

/// A [StorageWriteTx] allows for performing multiple writes across multiple
/// page group IDs as part of a single transaction.
pub struct StorageWriteTx<'a> {
    transaction_id: u64,
    transaction_n_entries: u32,
    controller: &'a StorageController,
}

impl std::fmt::Debug for StorageWriteTx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageWriteTx(tx_id={}, num_ops={})",
            self.transaction_id, self.transaction_n_entries,
        )
    }
}

/// A [StorageReader] allows you to read data pages for a given
/// page group.
pub struct StorageReader<'a> {
    group: PageGroupId,
    lookup: LookupEntry,
    controller: &'a StorageController,
}

impl std::fmt::Debug for StorageReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageReader(group={:?})", self.group)
    }
}

/// Removes all WAL files currently in the directory.
async fn remove_all_wal_files(ctx: &ctx::FileContext) -> io::Result<()> {
    let directory = ctx.directory();

    let file_ids = directory.list_dir(FileGroup::Wal).await;

    for file_id in file_ids {
        directory.remove_file(FileGroup::Wal, file_id).await?;
    }

    Ok(())
}
