use std::fmt::{Debug, Formatter};
use std::ops::{Deref, Range};
use std::sync::Arc;

use tokio::sync::Notify;

use super::mem_block::{
    PageFreePermit,
    PageIndex,
    PageOrRetry,
    PageWritePermit,
    PrepareDirtyEvictionError,
    PrepareWriteError,
    ReadResult,
    TryFreeError,
    VirtualMemoryBlock,
};
use super::{LayerId, LivePagesLfu, PageSize};
use crate::cache::evictions::PendingEvictions;

/// The global waker that notifies pending readers when a page has been written.
///
/// This might wake for page that the reader does not care about, but typically
/// the read rate on disk is not expected to be high enough to warrant independent
/// waker for each file (especially since that can ramp up the memory pressure on smaller cached
/// with lots of small files.)
///
/// This does use a slightly sharded approach to reduce contention and incorrect wakes
/// by taking the file ID and selecting the waker based on the hash mod of that.
static PAGE_WRITE_WAKER: [Notify; 32] = [const { Notify::const_new() }; 32];

/// The [CacheLayer] adds an in-memory LFU cache to a page file.
///
/// The cached pages within the file are controlled by a global LFU which allows
/// for optimal caching across several page files.
pub struct CacheLayer {
    pub(super) layer_id: LayerId,
    pub(super) live_pages: LivePagesLfu,
    pub(super) memory: VirtualMemoryBlock,
    pub(super) pending_evictions: PendingEvictions,
}

impl CacheLayer {
    #[tracing::instrument("cache::prepare_read", skip(self))]
    /// Prepare a new read of pages.
    ///
    /// Once all pages are confirmed to be allocated the read can be completed.
    pub fn prepare_read(self: &Arc<Self>, page_range: Range<PageIndex>) -> PreparedRead {
        tracing::debug!(page_range = ?page_range, "create prepared read");

        // Register the access within the cache's policy.
        for page in page_range.clone() {
            self.live_pages.get(&(self.layer_id, page));
        }

        self.pending_evictions.try_cleanup(&self.memory);

        let inner = self.memory.prepare_read(page_range.clone());

        // SAFETY: The lifetime of this read is tied to the parent, which is kept next to the read
        //         we need to do this to avoid having several Arc's that need to be cloned and allocated.
        let inner_with_static_lifetime = unsafe {
            std::mem::transmute::<
                super::mem_block::PreparedRead<'_>,
                super::mem_block::PreparedRead<'static>,
            >(inner)
        };

        PreparedRead {
            parent: self.clone(),
            inner: inner_with_static_lifetime,
        }
    }

    #[tracing::instrument("cache::dirty_page_range", skip(self))]
    /// Dirty all pages in the cache for the given file.
    ///
    /// This method can block due to syscall and waiting on page locks to become free.
    ///
    /// # Deadlock warning
    ///
    /// This method will wait for the page locks to become free, if the same thread currently
    /// has a reader being prepared then this can cause a deadlock.
    ///
    /// # Safety
    ///
    /// Great care must be taken when using this method, as previous _but still alive_ reads
    /// will observe modifications that might happen as a result of the pages being dirtied.
    ///
    /// The behaviour is very similar to a `mmap`, and carries the same safety risks,
    /// if `read1` occurs observing data of a full array of `4`s, then the pages are dirtied
    /// and new data is written filling parts with `2`s. `read1` will observe the partial change
    /// and now be in an undefined state for the application using it.
    ///
    /// NOTE: The act of dirtying the page while a reader is active does not cause immediate UB
    ///       as readers will not observe any change. However, any writes to the dirtied pages
    ///       afterward while older readers are still active will be UB.
    pub unsafe fn dirty_page_range(&self, range: Range<PageIndex>) {
        tracing::debug!("dirty pages");

        let mut retries = Vec::new();
        for page in range {
            self.live_pages.invalidate(&(self.layer_id, page));

            match self.memory.try_dirty_page(PageOrRetry::Page(page)) {
                Ok(permit) => {
                    tracing::trace!("adding page free op to backlog");
                    self.free_or_add_to_backlog(permit);
                },
                Err(
                    PrepareDirtyEvictionError::AlreadyFree
                    | PrepareDirtyEvictionError::AlreadyDirty
                    | PrepareDirtyEvictionError::OperationStale,
                ) => {},
                Err(PrepareDirtyEvictionError::PageLocked(retry)) => {
                    tracing::trace!(retry = ?retry, "page locked");
                    retries.push(retry);
                },
            }
        }

        tracing::trace!(num_retries = retries.len(), "considering pages for retry");

        for retry in retries {
            match self.memory.dirty_page(PageOrRetry::Retry(retry)) {
                Ok(permit) => {
                    self.free_or_add_to_backlog(permit);
                },
                Err(
                    PrepareDirtyEvictionError::AlreadyFree
                    | PrepareDirtyEvictionError::AlreadyDirty
                    | PrepareDirtyEvictionError::OperationStale,
                ) => {},
                Err(PrepareDirtyEvictionError::PageLocked(_)) => {
                    unreachable!(
                        "dirty_page should not happen as locks are acquired by blocking"
                    );
                },
            }
        }
    }

    /// Run the cleanup step explicitly to clean up pages.
    ///
    /// Unlike the maintenance task that occur during reads, this will wait
    /// for locks to become available to ensure the backlog is processed.
    pub fn run_cleanup(&self) {
        self.memory.advance_generation();
        self.pending_evictions.cleanup(&self.memory);
    }

    /// Runs any pending cache cleanup tasks.
    pub fn run_cache_tasks(&self) {
        self.live_pages.run_pending_tasks();
    }

    #[inline]
    /// The unique ID assigned to the page layer.
    pub fn id(&self) -> LayerId {
        self.layer_id
    }

    #[inline]
    /// Returns the page size used by the cache.
    pub fn page_size(&self) -> PageSize {
        self.memory.page_size()
    }

    #[inline]
    /// Returns the size of the backlog.
    ///
    /// This contains freeing operations which cannot currently take place
    /// due to locks or readers in use.
    pub fn backlog_size(&self) -> usize {
        self.pending_evictions.size()
    }

    fn free_or_add_to_backlog(&self, permit: PageFreePermit) {
        match self.memory.try_free(&permit) {
            Ok(()) => {},
            Err(TryFreeError::PermitExpired) => {},
            Err(TryFreeError::InUse | TryFreeError::Locked | TryFreeError::Io(_)) => {
                self.pending_evictions.push_page_dirty_permit(permit);
            },
        }
    }

    fn register_page_write_in_cache(&self, page_id: PageIndex) {
        self.live_pages.insert((self.layer_id, page_id), ());
    }
}

impl Drop for CacheLayer {
    fn drop(&mut self) {
        for page in 0..self.memory.num_pages() {
            self.live_pages
                .invalidate(&(self.layer_id, page as PageIndex));
        }
    }
}

/// A prepared read marks a caller's intent to read a span of pages
/// and ensures that a read can only be performed once all pages that need to be read
/// are both allocated and valid.
pub struct PreparedRead {
    parent: Arc<CacheLayer>,
    // NOTE: The lifetime of this is `parent`.
    inner: super::mem_block::PreparedRead<'static>,
}

impl PreparedRead {
    /// Produces an iterator of [PageWritePermit] for any of the pages
    /// with outstanding writes that are able to acquire the individual page locks.
    pub fn get_outstanding_write_permits(
        &self,
    ) -> impl Iterator<Item = PageWritePermit> + use<'_> {
        self.inner.outstanding_writes().iter().filter_map(|page| {
            // If the lock is acquired or already allocated we can ignore
            // the error as the next `try_finish` call will resolve any outstanding
            // writes left.
            let result = self.inner.parent().try_prepare_for_write(*page);

            match result {
                Ok(permit) => Some(permit),
                Err(PrepareWriteError::Locked) => None,
                Err(PrepareWriteError::AlreadyAllocated) => None,
                Err(PrepareWriteError::EvictionReverted) => {
                    self.parent.register_page_write_in_cache(*page);
                    None
                },
            }
        })
    }

    /// Write the provided byte array to the page guarded by the permit.
    pub fn write_page(&self, permit: PageWritePermit, bytes: &[u8]) {
        self.parent.register_page_write_in_cache(permit.page());
        self.inner.parent().write_page(permit, bytes);
        self.get_waker().notify_waiters();
    }

    /// Returns a future that waits until a write operation has been completed on the page file.
    pub fn wait_for_signal(&self) -> impl Future<Output = ()> + use<'_> {
        let waker = self.get_waker();
        waker.notified()
    }

    /// Attempt to finish the read if all pages are allocated and valid.
    pub fn try_finish(mut self) -> Result<ReadRef, Self> {
        match self.inner.try_finish() {
            Err(inner) => {
                self.inner = inner;
                Err(self)
            },
            Ok(result) => {
                Ok(ReadRef {
                    parent: self.parent,
                    // Safety:
                    // We can extend the lifetime as long as `parent` is kept alive, as that is
                    // what actually owns the memory we are now referencing.
                    inner: unsafe {
                        std::mem::transmute::<ReadResult<'_>, ReadResult<'static>>(
                            result,
                        )
                    },
                })
            },
        }
    }

    fn get_waker(&self) -> &'static Notify {
        let notify_id = self.parent.layer_id as usize % PAGE_WRITE_WAKER.len();
        &PAGE_WRITE_WAKER[notify_id]
    }
}

#[repr(C)]
/// A reference pointer to a span of memory pages.
///
/// This struct can be cheaply cloned as it just increments a ref-count.
pub struct ReadRef {
    parent: Arc<CacheLayer>,
    // NOTE: This is not actually `static`, it lives only for as long as `parent`.
    inner: ReadResult<'static>,
}

impl Debug for ReadRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let start = self.inner.as_ref();
        write!(
            f,
            "ReadRef(start={:?}, len={})",
            start.as_ptr(),
            start.len()
        )
    }
}

impl Clone for ReadRef {
    fn clone(&self) -> Self {
        Self {
            parent: self.parent.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl AsRef<[u8]> for ReadRef {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl Deref for ReadRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

unsafe impl stable_deref_trait::StableDeref for ReadRef {}
