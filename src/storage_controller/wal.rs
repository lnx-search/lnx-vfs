use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io, mem};

use parking_lot::Mutex;

use crate::directory::FileGroup;
use crate::layout::log::LogEntry;
use crate::layout::page_metadata::PageMetadata;
use crate::{ctx, file, page_op_log};

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
/// Configuration options for the WAL controller.
pub struct WalConfig {
    /// The maximum number of free WAL files the system should
    /// keep open and reuse.
    pub max_wal_file_pool_size: usize,
    /// The maximum size of a WAL file writer before it is rotated.
    pub soft_max_wal_size: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_wal_file_pool_size: 5,
            soft_max_wal_size: 2 << 30,
        }
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the controller from writing set of metadata updates
/// to the WAL file.
pub enum WalError {
    #[error(transparent)]
    /// The controller could not open a new WAL file during a WAL rotation
    /// operation.
    RotateError(#[from] page_op_log::LogOpenWriteError),
    #[error(transparent)]
    /// An IO error occurred.
    Io(#[from] io::Error),
}

/// The [WalController] manages a set of [page_op_log::LogFileWriter]s and
/// handles checkpointing and write coelesing to improve efficiency of the writer.
///
/// There is only one WAL file being written to at any one point in time.
pub struct WalController {
    ctx: Arc<ctx::FileContext>,
    config: WalConfig,
    /// A list of open writers that are free to be used.
    ///
    /// Writers are reused after being checkpointed and assigned a new
    /// unique ID for encryption & validation purposes.
    free_writers: Mutex<VecDeque<page_op_log::LogFileWriter>>,
    /// A list of writers that are currently waiting for a checkpoint operation
    /// to complete in order for them to be closed or recycled.
    checkpoint_pending_writers: Mutex<VecDeque<TaggedWriter>>,
    /// The current active WAL writer.
    active_writer: tokio::sync::Mutex<page_op_log::LogFileWriter>,
    /// The current op stamp that is incremented for every update
    /// to the page metadata state.
    op_stamp: AtomicU64,
}

impl WalController {
    /// Create a new [WalController] using the given [WalConfig].
    ///
    /// A new WAL file will automatically be created.
    pub async fn create(
        ctx: Arc<ctx::FileContext>,
        config: WalConfig,
    ) -> Result<WalController, WalError> {
        let writer = create_new_wal_file(ctx.clone()).await?;
        tracing::info!(file_id = ?writer.file_id(), "created new WAL writer");
        Ok(Self {
            ctx,
            config,
            free_writers: Mutex::new(VecDeque::new()),
            checkpoint_pending_writers: Mutex::new(VecDeque::new()),
            active_writer: tokio::sync::Mutex::new(writer),
            op_stamp: AtomicU64::new(0),
        })
    }

    /// Write a set of updates to the WAL.
    pub async fn write_updates(
        &self,
        updates: Vec<(LogEntry, Option<PageMetadata>)>,
    ) -> Result<(), WalError> {
        // TODO: This could be greatly optimised by allowing multiple updates to be coalesced.
        let mut writer = self.active_writer.lock().await;
        if writer.is_locked_out() || writer.position() >= self.config.soft_max_wal_size {
            if writer.is_locked_out() {
                tracing::warn!(
                    file_id = ?writer.file_id(),
                    "writer is locked out due to prior error, rotating WAL file",
                );
            } else {
                tracing::info!(
                    file_id = ?writer.file_id(),
                    "max size reached for WAL, rotating WAL file",
                );
            }
            let new_writer = self.rotate_writer().await?;
            let old_writer = mem::replace(&mut *writer, new_writer);
            self.add_writer_to_checkpoint_pending(old_writer);
        };

        tracing::debug!(
            file_id = ?writer.file_id(),
            num_updates = updates.len(),
            "writing updates to WAL",
        );

        for (entry, metadata) in updates {
            self.next_op_stamp();
            writer.write_log(entry, metadata).await?;
        }

        writer.sync().await?;

        Ok(())
    }

    #[cfg(test)]
    pub async fn active_writer_id(&self) -> crate::directory::FileId {
        self.active_writer.lock().await.file_id()
    }

    /// Increment the op stamp counter and return the value.
    pub fn next_op_stamp(&self) -> u64 {
        self.op_stamp.fetch_add(1, Ordering::Relaxed)
    }

    /// Returns the number of writers which are free and ready
    /// to be reused.
    pub fn num_free_writers(&self) -> usize {
        self.free_writers.lock().len()
    }

    /// Returns the number of writers which are waiting for a checkpoint
    /// operation to complete them.
    pub fn num_checkpoint_pending_writers(&self) -> usize {
        self.checkpoint_pending_writers.lock().len()
    }

    /// Rotate the current writer so it can be checkpointed later.
    ///
    /// This is used as a way to do some WAL maintenance when the system
    /// is not heavily loaded with writes and speedup startup/recovery.
    pub async fn prepare_checkpoint(&self) -> Result<(), WalError> {
        let mut writer = self.active_writer.lock().await;
        let new_writer = self.rotate_writer().await?;
        let old_writer = mem::replace(&mut *writer, new_writer);
        self.add_writer_to_checkpoint_pending(old_writer);
        Ok(())
    }

    /// Put any WAL writer that has an op stamp tag less than or equal to the checkpointed
    /// op stamp back into the free writer pool.
    pub async fn recycle_writers(&self, checkpoint_op_stamp: u64) -> io::Result<()> {
        while let Some(file) = self.get_next_checkpointed_writer(checkpoint_op_stamp) {
            if let Err(err) = self.recycle_writer(file).await {
                tracing::error!(error = %err, "failed to recycle WAL file");
            }
        }
        Ok(())
    }

    async fn recycle_writer(&self, file: file::RWFile) -> Result<(), WalError> {
        let num_to_repopulate = {
            let num_free_writers = self.free_writers.lock().len();
            self.config.max_wal_file_pool_size - num_free_writers
        };

        if num_to_repopulate > 0 {
            let writer =
                page_op_log::LogFileWriter::create(self.ctx.clone(), file).await?;
            let mut free_writers = self.free_writers.lock();
            free_writers.push_back(writer);
        } else {
            let file_id = file.id();
            drop(file);
            let directory = self.ctx.directory();
            directory.remove_file(FileGroup::Wal, file_id).await?;
        }

        Ok(())
    }

    fn get_next_checkpointed_writer(
        &self,
        checkpoint_op_stamp: u64,
    ) -> Option<file::RWFile> {
        let mut pending_writers = self.checkpoint_pending_writers.lock();
        if let Some(writer) = pending_writers.pop_front() {
            return if writer.op_stamp > checkpoint_op_stamp {
                pending_writers.push_front(writer);
                None
            } else {
                Some(writer.file)
            };
        }
        None
    }

    fn add_writer_to_checkpoint_pending(&self, writer: page_op_log::LogFileWriter) {
        let file = writer.into_file();
        let op_stamp = self.next_op_stamp();
        let tagged = TaggedWriter { file, op_stamp };
        let mut waiting_writers = self.checkpoint_pending_writers.lock();
        waiting_writers.push_back(tagged);
    }

    /// Gets a free WAL writer or creates a new log file & writer.
    async fn rotate_writer(&self) -> Result<page_op_log::LogFileWriter, WalError> {
        {
            let mut free_writers = self.free_writers.lock();
            if let Some(free_writer) = free_writers.pop_front() {
                return Ok(free_writer);
            }
        }

        create_new_wal_file(self.ctx.clone())
            .await
            .map_err(WalError::RotateError)
    }
}

async fn create_new_wal_file(
    ctx: Arc<ctx::FileContext>,
) -> Result<page_op_log::LogFileWriter, page_op_log::LogOpenWriteError> {
    let directory = ctx.directory();
    let file_id = directory.create_new_file(FileGroup::Wal).await?;
    let file = directory.get_rw_file(FileGroup::Wal, file_id).await?;
    let writer = page_op_log::LogFileWriter::create(ctx, file).await?;
    Ok(writer)
}

struct TaggedWriter {
    /// The file of the WAL writer.
    file: file::RWFile,
    /// The op stamp the writer contains data up to.
    ///
    /// Once all changes up to this op stamp have been checkpointed,
    /// the writer can go back into circulation.
    op_stamp: u64,
}
