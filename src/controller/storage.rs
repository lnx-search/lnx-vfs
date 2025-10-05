use std::io;
use std::sync::Arc;

use super::group_lock::GroupLocks;
use super::metadata::{MetadataController, OpenMetadataControllerError};
use super::wal::{WalController, WalError};
use crate::checkpoint::WriteCheckpointError;
use crate::controller::cache::CacheController;
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

/// The [StorageController] manages persisting updates to pages of data.
pub struct StorageController {
    metadata_controller: MetadataController,
    wal_controller: WalController,
    page_file_controller: PageFileController,
    cache_controller: CacheController,
    group_locks: GroupLocks,
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
        let cache_controller = CacheController::new(&ctx);

        Ok(Self {
            metadata_controller,
            wal_controller,
            page_file_controller,
            cache_controller,
            group_locks: GroupLocks::default(),
        })
    }

    #[inline]
    /// Returns if the controller contains a given page group.
    pub fn contains_page_group(&self, group: PageGroupId) -> bool {
        self.metadata_controller.contains_page_group(group)
    }

    /// Create a new storage write transaction.
    pub fn create_write_txn(&self) -> super::txn_write::StorageWriteTxn<'_> {
        super::txn_write::StorageWriteTxn::new(self)
    }

    /// Create a new reader for a given [PageGroupId].
    pub fn create_read_txn(&self, group: PageGroupId) {
        // let lookup = self.metadata_controller.collect_pages(group)?;
        // Some(super::txn_read::StorageReader::new(group, lookup, self))
        todo!()
    }

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
    ///
    /// If this operation errors, it is safe to assume that data will _not_ be recovered on
    /// restart.
    pub(super) async fn commit_ops(&self, ops: Vec<LogOp>) -> Result<(), WalError> {
        self.wal_controller.write_updates(ops.clone()).await?;

        for op in ops {
            match op {
                LogOp::Write(op) => {
                    self.cache_controller.remove_layer(op.page_group_id);
                    if !self
                        .metadata_controller
                        .contains_page_table(op.page_file_id)
                    {
                        self.metadata_controller
                            .create_blank_page_table(op.page_file_id);
                    }
                    self.metadata_controller.assign_pages_to_group(
                        op.page_file_id,
                        op.page_group_id,
                        &op.altered_pages,
                    );
                },
                LogOp::Free(op) => {
                    self.cache_controller.remove_layer(op.page_group_id);
                    self.metadata_controller
                        .unassign_pages_in_group(op.page_group_id);
                },
                LogOp::Reassign(op) => {
                    self.cache_controller
                        .reassign_layer(op.old_page_group_id, op.new_page_group_id);
                    self.metadata_controller
                        .reassign_pages(op.old_page_group_id, op.new_page_group_id);
                },
            }
        }

        Ok(())
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
