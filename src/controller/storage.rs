use std::ops::{Deref, Range};
use std::sync::Arc;
use std::time::Duration;
use std::{io, mem};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::group_lock::GroupLocks;
use super::metadata::{MetadataController, OpenMetadataControllerError};
use super::wal::{WalController, WalError};
use crate::checkpoint::WriteCheckpointError;
use crate::controller::cache::CacheController;
use crate::controller::page_file::{PageDataWriter, PageFileController};
use crate::directory::FileGroup;
use crate::layout::PageGroupId;
use crate::layout::log::LogOp;
use crate::page_data::{
    CreatePageFileError,
    DISK_PAGE_SIZE,
    OpenPageFileError,
    ReadPageError,
};
use crate::{cache, ctx};

const DEFAULT_WAL_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5 * 60);

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

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
/// Configuration options for the storage controller.
pub struct StorageConfig {
    /// The time that should elapse between WAL checkpoints.
    ///
    /// If this is `None` the checkpointing is disabled.
    pub wal_checkpoint_interval: Option<Duration>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            wal_checkpoint_interval: Some(DEFAULT_WAL_CHECKPOINT_INTERVAL),
        }
    }
}

/// The [StorageController] manages persisting updates to pages of data.
pub struct StorageController {
    config: StorageConfig,
    metadata_controller: MetadataController,
    wal_controller: WalController,
    page_file_controller: PageFileController,
    cache_controller: CacheController,
    group_locks: GroupLocks,
    active_checkpoint_task: Mutex<CheckpointTask>,
}

impl StorageController {
    /// Open the storage controller with a given context.
    ///
    /// This will automatically recover any existing state from the configured
    /// storage directory and checkpoint any WAL files before returning.
    pub async fn open(
        ctx: Arc<ctx::Context>,
    ) -> Result<Arc<Self>, OpenStorageControllerError> {
        let config: StorageConfig = ctx.config_opt().unwrap_or_default();

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

        let (kill_switch, _) = oneshot::channel();
        let slf = Arc::new(Self {
            config,
            metadata_controller,
            wal_controller,
            page_file_controller,
            cache_controller,
            group_locks: GroupLocks::default(),
            active_checkpoint_task: Mutex::new(CheckpointTask {
                handle: tokio::spawn(async move {}),
                _kill_switch: kill_switch,
            }),
        });
        slf.check_checkpoint_task().await;
        Ok(slf)
    }

    // Clippy is wrong, we drop the guard explicitly before the await.
    #[allow(clippy::await_holding_lock)]
    async fn check_checkpoint_task(self: &Arc<Self>) {
        let checkpoint_interval = match self.config.wal_checkpoint_interval {
            None => return,
            Some(interval) => interval,
        };

        let Some(mut guard) = self.active_checkpoint_task.try_lock() else {
            return;
        };
        if !guard.handle.is_finished() {
            return;
        }

        let this = self.clone();
        let (tx, rx) = oneshot::channel();
        let new_handle = tokio::spawn(async move {
            tokio::pin!(rx);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(checkpoint_interval) => {},
                    _ = &mut rx => return,
                }

                if let Err(err) = this.checkpoint().await {
                    tracing::error!(error = %err, "failed to run checkpointing job");
                }
            }
        });

        let new_task = CheckpointTask {
            handle: new_handle,
            _kill_switch: tx,
        };

        let task = mem::replace(&mut *guard, new_task);
        drop(guard);

        if let Err(err) = task.handle.await {
            tracing::error!(error = %err, "checkpoint task panicked");
        }
    }

    pub(super) async fn checkpoint(&self) -> Result<(), CheckpointError> {
        let op_stamp = self.wal_controller.prepare_checkpoint().await?;
        self.metadata_controller.checkpoint().await?;
        self.wal_controller.recycle_writers(op_stamp).await?;
        Ok(())
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

    /// Read a page group at a given position.
    ///
    /// The range provided is relative to the page group.
    pub async fn read_group(
        &self,
        group: PageGroupId,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<ReadRef, ReadPageError> {
        let lookup = self
            .metadata_controller
            .get_group_lookup(group)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

        let start = start.unwrap_or_default();
        let end = end.unwrap_or(lookup.total_size as usize);
        let range = start..end;

        let cache_layer = self.get_or_create_cache_layer(group)?;

        let mut pages = Vec::new();
        let lookup = self
            .metadata_controller
            .collect_pages(group, range.clone(), &mut pages)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

        let read_len = range.len();
        let read_offset = range.start % DISK_PAGE_SIZE;
        let page_range = resolve_to_pages(range, DISK_PAGE_SIZE);

        let mut prepared_read =
            cache_layer.prepare_read(page_range.start as u32..page_range.end as u32);

        let waiter = prepared_read.wait_for_signal();
        tokio::pin!(waiter);

        let mut pages_to_read = Vec::new();
        let mut user_data = Vec::new();
        loop {
            waiter.as_mut().enable();

            match prepared_read.try_finish() {
                Ok(inner) => {
                    // Final compare step to ensure reads don't accidentally return
                    // overwritten data.
                    let new_lookup = self
                        .metadata_controller
                        .get_group_lookup(group)
                        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

                    return if lookup != new_lookup {
                        Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "read has been interrupted by write",
                        )
                        .into())
                    } else {
                        Ok(ReadRef {
                            inner,
                            offset: read_offset,
                            len: read_len,
                        })
                    };
                },
                Err(read) => {
                    prepared_read = read;
                },
            }

            pages_to_read.clear();
            user_data.clear();

            let mut permits = Vec::new();
            for permit in prepared_read.get_outstanding_write_permits() {
                let page_metadata = &pages[permit.page() as usize - page_range.start];
                pages_to_read.push(*page_metadata);
                user_data.push(permit.page() as usize);
                permits.push(permit);
            }

            if permits.is_empty() {
                waiter.as_mut().await;
                waiter.set(prepared_read.wait_for_signal());
            }

            let mut result = self
                .page_file_controller
                .read_many(lookup.page_file_id, &pages_to_read, Some(&user_data))
                .await?;

            while let Some(read_result) = result.join_next().await {
                let mut iop = read_result.expect("BUG: read task panicked")?;

                while let Some((_page, data, user_data)) = iop.next_page() {
                    let page = user_data.expect("BUG: user data did not exist") as u32;
                    let permit_pos = permits
                        .iter()
                        .position(|p| p.page() == page)
                        .expect("BUG: page permit did not exist");
                    let permit = permits.swap_remove(permit_pos);
                    prepared_read.write_page(permit, data);
                }
            }
        }
    }

    fn get_or_create_cache_layer(
        &self,
        group: PageGroupId,
    ) -> io::Result<Arc<cache::CacheLayer>> {
        let lookup = self
            .metadata_controller
            .get_group_lookup(group)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

        let total_group_cache_pages =
            lookup.total_size.div_ceil(DISK_PAGE_SIZE as u64) as usize;

        self.cache_controller
            .get_or_create_layer(group, total_group_cache_pages)
    }

    /// Creates a new writer for a given length buffer.
    pub async fn create_writer(
        self: &Arc<Self>,
        len: u64,
    ) -> Result<PageDataWriter<'_>, CreatePageFileError> {
        assert!(len > 0, "length must be greater than zero");
        self.check_checkpoint_task().await;
        self.page_file_controller.create_writer(len).await
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

                    let mut pages = Vec::new();
                    let lookup = self.metadata_controller.collect_pages(
                        op.page_group_id,
                        0..usize::MAX,
                        &mut pages,
                    );
                    let Some(_lookup) = lookup else { continue };

                    // TODO: Implement freeing logic
                    // self.page_file_controller
                    //     .free_pages(lookup.page_file_id, &pages);
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

struct CheckpointTask {
    handle: tokio::task::JoinHandle<()>,
    _kill_switch: oneshot::Sender<()>,
}

#[derive(Clone)]
/// A read buffer that spans a set of pages in a page group.
pub struct ReadRef {
    inner: cache::ReadRef,
    offset: usize,
    len: usize,
}

impl std::fmt::Debug for ReadRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadRef(ptr={:?}, len={}, offset={})",
            self.inner.as_ptr(),
            self.len,
            self.offset,
        )
    }
}

impl Deref for ReadRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner[self.offset..][..self.len]
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CheckpointError {
    #[error("failed to checkpoint metadata: {0}")]
    CheckpointMetadata(#[from] WriteCheckpointError),
    #[error("failed to prepare WAL for checkpointing: {0}")]
    PrepareCheckpoint(#[from] WalError),
    #[error("failed to recycle WAL files: {0}")]
    RecycleWalFiles(#[from] io::Error),
}

/// Removes all WAL files currently in the directory.
async fn remove_all_wal_files(ctx: &ctx::Context) -> io::Result<()> {
    let directory = ctx.directory();

    let file_ids = directory.list_dir(FileGroup::Wal).await;

    for file_id in file_ids {
        directory.remove_file(FileGroup::Wal, file_id).await?;
    }

    Ok(())
}

const fn resolve_to_pages(data_range: Range<usize>, page_size: usize) -> Range<usize> {
    let start = data_range.start / page_size;
    let end = data_range.end.div_ceil(page_size);
    start..end
}
