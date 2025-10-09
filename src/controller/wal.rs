use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{io, mem};

use parking_lot::Mutex;
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::directory::FileGroup;
use crate::layout::log::LogOp;
use crate::{ctx, file, page_op_log};

const SYNC_COALESCE_DURATION: Duration = Duration::from_millis(5);

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
/// An error preventing the WAL from completing the operation.
pub enum WalError {
    #[error("WAL Error: {0}")]
    /// The controller could not open a new WAL file during a WAL rotation
    /// operation.
    RotateError(#[from] page_op_log::LogOpenWriteError),
    #[error("WAL Error: {0}")]
    /// An IO error occurred.
    Io(#[from] io::Error),
}

/// The [WalController] manages a set of [page_op_log::LogFileWriter]s and
/// handles checkpointing and coalesce writes to improve efficiency of the writer.
///
/// There is only one WAL file being written to at any one point in time.
pub struct WalController {
    ctx: Arc<ctx::Context>,
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
    /// The current op stamp that is incremented for every WAL rotation operation
    /// performed on the log.
    op_stamp: AtomicU64,
    /// Enqueued operations from other tasks that are waiting for the writer
    /// lock. This allows the system to coalesce writes.
    enqueued_operations: Mutex<EnqueuedOperations>,

    // controller metrics - see their getters for docs.
    total_write_operations: AtomicU64,
    total_coalesced_write_operations: AtomicU64,
    total_coalesced_failures: AtomicU64,
}

impl WalController {
    /// Create a new [WalController] using the given [WalConfig].
    ///
    /// A new WAL file will automatically be created.
    pub async fn create(ctx: Arc<ctx::Context>) -> Result<WalController, WalError> {
        let config: WalConfig = ctx.config_opt().unwrap_or_default();
        let writer = create_new_wal_file(ctx.clone()).await?;
        tracing::info!(file_id = ?writer.file_id(), "created new WAL writer");
        Ok(Self {
            ctx,
            config,
            free_writers: Mutex::new(VecDeque::new()),
            checkpoint_pending_writers: Mutex::new(VecDeque::new()),
            active_writer: tokio::sync::Mutex::new(writer),
            op_stamp: AtomicU64::new(1),
            enqueued_operations: Mutex::new(EnqueuedOperations::default()),
            total_write_operations: AtomicU64::new(0),
            total_coalesced_write_operations: AtomicU64::new(0),
            total_coalesced_failures: AtomicU64::new(0),
        })
    }

    /// Write a set of updates to the WAL.
    ///
    /// It is guaranteed that an error returned by the controller will
    /// force a WAL rotation.
    ///
    /// Returns the assigned transaction ID.
    pub async fn write_updates(&self, updates: Vec<LogOp>) -> Result<u64, WalError> {
        self.total_write_operations.fetch_add(1, Ordering::Relaxed);

        let (op_id, mut waiter) = {
            let mut enqueued_operations = self.enqueued_operations.lock();
            enqueued_operations.push(updates)
        };
        tracing::debug!(op_id = op_id, "pushed update to queue");

        // Wait for us to either get the writer lock, or for our write to
        // be completed by another task.
        let mut writer = tokio::select! {
            guard = self.active_writer.lock() => guard,
            result = &mut waiter => {
                self.total_coalesced_write_operations.fetch_add(1, Ordering::Relaxed);
                return match result {
                    Ok(_) => Ok(op_id),
                    Err(_) => {
                        self.total_coalesced_failures.fetch_add(1, Ordering::Relaxed);
                        Err(WalError::Io(interrupted_prior_error()))
                    },
                }
            },
        };

        match waiter.try_recv() {
            Ok(()) => {
                self.total_coalesced_write_operations
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(op_id);
            },
            Err(oneshot::error::TryRecvError::Closed) => {
                self.total_coalesced_write_operations
                    .fetch_add(1, Ordering::Relaxed);
                self.total_coalesced_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(WalError::Io(interrupted_prior_error()));
            },
            Err(oneshot::error::TryRecvError::Empty) => {},
        }

        if writer.is_locked_out() {
            writer.reset_to_last_safe_point().await;
        }

        match self.write_to_log(op_id, &mut writer).await {
            Ok(op_id) => Ok(op_id),
            Err(err) => {
                tracing::info!(error = %err, "WAL write failed, attempting to recover");
                writer.reset_to_last_safe_point().await;
                Err(err)
            },
        }
    }

    async fn write_to_log(
        &self,
        op_id: u64,
        writer: &mut page_op_log::LogFileWriter,
    ) -> Result<u64, WalError> {
        let start = Instant::now();
        self.maybe_rotate_writer(writer).await?;

        let mut seen_own_op = false;
        let mut replies_to_complete = SmallVec::<[oneshot::Sender<()>; 4]>::new();
        while start.elapsed() <= SYNC_COALESCE_DURATION || !seen_own_op {
            let op = match self.pop_next_enqueued_operation() {
                None => break,
                Some(op) => op,
            };

            if op.id == op_id {
                seen_own_op = true;
            } else {
                replies_to_complete.push(op.tx);
            }

            writer.write_log(op_id, &op.updates).await?;
        }

        writer.sync().await?;

        let num_coalesced_updates = replies_to_complete.len();
        for reply in replies_to_complete {
            let _ = reply.send(());
        }
        tracing::debug!(
            file_id = ?writer.file_id(),
            num_coalesced_updates = num_coalesced_updates,
            "wrote updates to WAL",
        );

        // This should ALWAYS be true as we got the writer lock, checked our own waiter if
        // something has already been set to account for
        assert!(
            seen_own_op,
            "BUG: WAL exhausted enqueued ops but its own op did not appear"
        );

        Ok(op_id)
    }

    #[cfg(test)]
    pub async fn active_writer_id(&self) -> crate::directory::FileId {
        self.active_writer.lock().await.file_id()
    }

    fn pop_next_enqueued_operation(&self) -> Option<EnqueuedOperation> {
        self.enqueued_operations.lock().pop()
    }

    #[inline]
    /// Increment the op stamp counter and return the value.
    fn next_op_stamp(&self) -> u64 {
        self.op_stamp.fetch_add(1, Ordering::AcqRel) + 1
    }

    #[inline]
    pub fn op_stamp(&self) -> u64 {
        self.op_stamp.load(Ordering::Acquire)
    }

    #[inline]
    /// Returns the number of writers which are free and ready
    /// to be reused.
    pub fn num_free_writers(&self) -> usize {
        self.free_writers.lock().len()
    }

    #[inline]
    /// Returns the number of writers which are waiting for a checkpoint
    /// operation to complete them.
    pub fn num_checkpoint_pending_writers(&self) -> usize {
        self.checkpoint_pending_writers.lock().len()
    }

    #[inline]
    /// The total number of write operations the controller has seen.
    pub fn total_write_operations(&self) -> u64 {
        self.total_write_operations.load(Ordering::Relaxed)
    }

    #[inline]
    /// The total number of write operations that have been coalesced.
    pub fn total_coalesced_write_operations(&self) -> u64 {
        self.total_coalesced_write_operations
            .load(Ordering::Relaxed)
    }

    #[inline]
    /// The total number of write operations that have failed because a coalesced call
    /// failed.
    pub fn total_coalesced_failures(&self) -> u64 {
        self.total_coalesced_failures.load(Ordering::Relaxed)
    }

    /// Rotate the current writer so it can be checkpointed later.
    ///
    /// This is used as a way to do some WAL maintenance when the system
    /// is not heavily loaded with writes and speedup startup/recovery.
    pub async fn prepare_checkpoint(&self) -> Result<u64, WalError> {
        let mut writer = self.active_writer.lock().await;
        let old_writer_op_stamp = self.op_stamp();
        let new_writer = self.rotate_writer().await?;
        let old_writer = mem::replace(&mut *writer, new_writer);
        self.add_writer_to_checkpoint_pending(old_writer, old_writer_op_stamp);
        Ok(old_writer_op_stamp)
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

    /// Rotate the currently active writer if it is locked out due to a prior error,
    /// or the file has grown to the target soft-max file size.
    async fn maybe_rotate_writer(
        &self,
        writer: &mut page_op_log::LogFileWriter,
    ) -> Result<(), WalError> {
        if writer.is_sealed() || writer.position() >= self.config.soft_max_wal_size {
            tracing::info!(
                file_id = ?writer.file_id(),
                "max size reached for WAL or has become sealed, rotating WAL file",
            );
            let current_op_stamp = self.op_stamp();
            let new_writer = self.rotate_writer().await?;
            let old_writer = mem::replace(&mut *writer, new_writer);
            self.add_writer_to_checkpoint_pending(old_writer, current_op_stamp);
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

    fn add_writer_to_checkpoint_pending(
        &self,
        writer: page_op_log::LogFileWriter,
        op_stamp: u64,
    ) {
        let file = writer.into_file();
        let tagged = TaggedWriter { file, op_stamp };
        let mut waiting_writers = self.checkpoint_pending_writers.lock();
        waiting_writers.push_back(tagged);
    }

    /// Gets a free WAL writer or creates a new log file & writer.
    async fn rotate_writer(&self) -> Result<page_op_log::LogFileWriter, WalError> {
        let op_stamp = self.next_op_stamp();
        tracing::info!(assigned_op_stamp = op_stamp, "rotating WAL file");

        {
            let mut free_writers = self.free_writers.lock();
            if let Some(free_writer) = free_writers.pop_front() {
                tracing::debug!(assigned_op_stamp = op_stamp, "using free WAL file");
                return Ok(free_writer);
            }
        }

        tracing::debug!(assigned_op_stamp = op_stamp, "creating new WAL file");
        create_new_wal_file(self.ctx.clone())
            .await
            .map_err(WalError::RotateError)
    }
}

async fn create_new_wal_file(
    ctx: Arc<ctx::Context>,
) -> Result<page_op_log::LogFileWriter, page_op_log::LogOpenWriteError> {
    let directory = ctx.directory();
    let file_id = directory.create_new_atomic_file(FileGroup::Wal).await?;
    let file = directory.get_rw_file(FileGroup::Wal, file_id).await?;
    let writer = page_op_log::LogFileWriter::create(ctx.clone(), file).await?;
    directory
        .persist_atomic_file(FileGroup::Wal, file_id)
        .await?;
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

struct EnqueuedOperations {
    id: u64,
    ops: VecDeque<EnqueuedOperation>,
}

impl Default for EnqueuedOperations {
    fn default() -> Self {
        Self {
            id: 1,
            ops: VecDeque::new(),
        }
    }
}

impl EnqueuedOperations {
    fn push(&mut self, updates: Vec<LogOp>) -> (u64, oneshot::Receiver<()>) {
        let id = self.id;
        self.id += 1;
        let (tx, rx) = oneshot::channel();
        self.ops.push_back(EnqueuedOperation { id, tx, updates });
        (id, rx)
    }

    fn pop(&mut self) -> Option<EnqueuedOperation> {
        self.ops.pop_front()
    }
}

struct EnqueuedOperation {
    id: u64,
    tx: oneshot::Sender<()>,
    updates: Vec<LogOp>,
}

fn interrupted_prior_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "WAL write failed due to prior error",
    )
}
