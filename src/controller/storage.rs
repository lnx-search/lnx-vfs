use std::io;
use std::sync::Arc;

use super::metadata::{MetadataController, OpenMetadataControllerError};
use super::wal::{WalController, WalError};
use crate::checkpoint::WriteCheckpointError;
use crate::ctx;
use crate::directory::FileGroup;

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
        })
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
