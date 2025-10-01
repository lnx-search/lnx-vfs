use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::group_lock::{ConcurrentMutationError, GroupGuard, GroupLocks};
use super::metadata::{MetadataController, OpenMetadataControllerError};
use super::wal::{WalController, WalError};
use crate::checkpoint::WriteCheckpointError;
use crate::controller::page_file::{PageDataWriter, PageFileController};
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::PageGroupId;
use crate::layout::log::LogOp;
use crate::page_data::{CreatePageFileError, OpenPageFileError};

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
    #[error(transparent)]
    /// The page file controller could not be opened.
    PageFileOpen(#[from] OpenPageFileError),
    #[error(transparent)]
    /// The page file controller could not create a new page file.
    PageFileCreate(#[from] CreatePageFileError),
}

// TODO: We need to recover on a WAL error and prevent the memory state from diverging from
//       the on-disk state.
//  - All reads must be locked out while this occurs.
//  - If the WAL errors, we need to enter "recovery mode" where we lock all writes.
//  - Potential issue, if we have multiple writes queued up on the WAL, and one write errors
//    we may have inflight IOPS writing to a new WAL which may be successful.
//

/// The [StorageController] manages persisting updates to pages of data.
pub struct StorageController {
    metadata_controller: MetadataController,
    wal_controller: WalController,
    page_file_controller: PageFileController,
    group_locks: GroupLocks,

    /// An indicator flag for if the system is currently in recovery mode or not.
    ///
    /// If this is `true`, no writes are allowed to progress as the system must process
    /// its on-disk state before the memory state is confirmed to be correct.
    recovery_mode_active: AtomicBool,
    /// A guard to prevent multiple writers attempting to perform the recovery operation
    /// at the same time.
    recovery_mode_lock: tokio::sync::Mutex<()>,
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
        let page_file_controller =
            PageFileController::open(ctx.clone(), &metadata_controller).await?;

        Ok(Self {
            metadata_controller,
            wal_controller,
            page_file_controller,
            group_locks: GroupLocks::default(),

            recovery_mode_active: AtomicBool::new(false),
            recovery_mode_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Create a new storage write transaction.
    pub fn create_write_txn(&self) -> super::txn_write::StorageWriteTxn<'_> {
        super::txn_write::StorageWriteTxn::new(self)
    }

    // /// Create a new reader for a given [PageGroupId].
    // pub fn create_read_txn(
    //     &self,
    //     group: PageGroupId,
    // ) -> Option<super::txn_read::StorageReader<'_>> {
    //     // let lookup = self.metadata_controller.find_first_page(group)?;
    //     // Some(super::txn_read::StorageReader::new(group, lookup, self))
    //     todo!()
    // }

    /// Creates a new writer for a given length buffer.
    pub fn create_writer(
        &self,
        len: u64,
    ) -> impl Future<Output = Result<PageDataWriter<'_>, CreatePageFileError>> + '_ {
        self.page_file_controller.create_writer(len)
    }

    #[inline]
    pub(super) fn group_locks(&self) -> &GroupLocks {
        &self.group_locks
    }

    /// Writes the list of ops to the WAL and returns the assigned transaction ID.
    pub(super) async fn commit_ops(&self, ops: Vec<LogOp>) -> Result<u64, WalError> {
        // TODO: Must go into recovery mode if error!!!

        let result = self.wal_controller.write_updates(ops).await;
        if result.is_err() {
            self.trigger_recovery().await?;
        }

        result
    }

    /// Puts the storage controller into recovery mode.
    ///
    /// This will attempt to recover the memory state
    async fn trigger_recovery(&self) -> Result<(), WalError> {
        todo!()
    }
}

struct VolatileMetadataState {
    metadata_controller: MetadataController,
    wal_controller: WalController,
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
