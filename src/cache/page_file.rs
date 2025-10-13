use std::fmt::{Debug, Formatter};
use std::io;
use std::ops::{Deref, Range};
use std::sync::Arc;

use super::mem_block::{
    PageIndex,
    PageWritePermit,
    PrepareWriteError,
    ReadResult,
    VirtualMemoryBlock,
};
use super::{LayerId, PageSize, tracker};
use crate::cache::evictions::EvictionBacklog;

/// The [CacheLayer] adds an in-memory LFU cache to a page file.
///
/// The cached pages within the file are controlled by a global LFU which allows
/// for optimal caching across several page files.
pub struct CacheLayer {
    pub(super) layer_id: LayerId,
    pub(super) live_pages: tracker::SharedLfuCacheTracker,
    pub(super) memory: VirtualMemoryBlock,
}

impl CacheLayer {
    #[tracing::instrument("cache::prepare_read", skip(self))]
    /// Prepare a new read of pages.
    ///
    /// Once all pages are confirmed to be allocated the read can be completed.
    pub fn prepare_read(self: &Arc<Self>, page_range: Range<PageIndex>) -> PreparedRead {
        tracing::debug!(page_range = ?page_range, "create prepared read");

        // Register the access within the cache's policy.
        self.live_pages
            .lock()
            .insert(self.layer_id, page_range.clone());

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

    /// Reset the state of the memory map, freeing all pages.
    ///
    /// This will evict all entries from the cache as well as handing back the memory
    /// to the OS.
    ///
    /// # Safety
    /// This method cannot be used if there are any reads or writes currently still referencing
    /// the cache layer, this method does _not_ protect against any kinds of race conditions
    /// or prevent you from freeing data that while a read is taking place.
    ///
    /// This method is primarily designed to be used for testing and resetting the mmap state
    /// once you know there are no more possible users.
    pub unsafe fn reset(&self) -> io::Result<()> {
        self.live_pages.lock().remove(
            self.layer_id,
            0..self.memory.num_pages() as PageIndex,
            true,
        );

        unsafe { self.memory.force_reset() }
    }

    /// Run the bookkeeping steps within the layer.
    ///
    /// This advanced the generation and attempts to collapse pages of memory
    /// into huge pages to reduce load on the kernel if possible.
    pub fn run_bookkeeping(&self) {
        self.memory.advance_generation();
        if self.memory.try_collapse().is_ok() {
            tracing::trace!("successfully collapsed pages");
        }
    }

    /// Process any outstanding items in the provided [EvictionBacklog].
    pub fn process_evictions(&self, backlog: &mut EvictionBacklog) -> usize {
        super::evictions::cleanup_revertible_evictions(usize::MAX, &self.memory, backlog)
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
}

impl Drop for CacheLayer {
    fn drop(&mut self) {
        self.live_pages.lock().remove(
            self.layer_id,
            0..self.memory.num_pages() as PageIndex,
            true,
        );
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
    ) -> impl Iterator<Item = PageWritePermit<'_>> + use<'_> {
        self.inner.outstanding_writes().iter().filter_map(|page| {
            // If the lock is acquired or already allocated we can ignore
            // the error as the next `try_finish` call will resolve any outstanding
            // writes left.
            let result = self.inner.parent().try_prepare_for_write(*page);

            match result {
                Ok(permit) => Some(permit),
                Err(PrepareWriteError::Locked) => None,
                Err(PrepareWriteError::AlreadyAllocated) => None,
                Err(PrepareWriteError::EvictionReverted) => None,
            }
        })
    }

    /// Write the provided byte array to the page guarded by the permit.
    pub fn write_page(&self, permit: PageWritePermit, bytes: &[u8]) {
        self.inner.parent().write_page(permit, bytes);
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
