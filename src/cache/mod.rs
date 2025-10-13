mod evictions;
mod mem_block;
mod page_file;
mod tracker;

use std::collections::VecDeque;
use std::sync::Arc;
use std::{io, mem};

use self::mem_block::VirtualMemoryBlock;
pub use self::mem_block::{PageIndex, PageSize, PageWritePermit, PreparedRead};
pub use self::page_file::{CacheLayer, ReadRef};
use crate::cache::tracker::LfuCacheTracker;

/// A unique identifier for a cache layer.
pub type LayerId = u64;

/// The page file cache
///
/// This system is built around reserving blocks of virtual memory and then incrementally
/// populating and evicting pages in a similar fashion to a memory mapped file.
///
/// Each page file has its own virtual memory reservation which holds only the actual page
/// data (i.e. no metadata, no structure overhead, etc...) and stores each block contiguously
/// allowing the system to return contiguous blocks of memory on reads without additional copies.
///
/// The caching strategy used is a LFU (Least Frequently Used) policy provided by `moka`.
/// The system itself may not immediately free memory when it has been marked for eviction
/// due to reads holding a reference to the memory currently, therefore, it is recommended
/// to allow some buffer zone of memory allowance as the system is not guaranteed to stay
/// within the bounds set constantly.
///
pub struct PageFileCache {
    live_pages: tracker::SharedLfuCacheTracker,
    num_pages: u64,
    page_size: PageSize,
}

impl PageFileCache {
    /// Create a new [PageFileCache] with a given amount of memory capacity
    /// and with a given page size.
    ///
    /// `memory_allowance` is in bytes.
    ///
    /// The cache will create `memory_allowance / page_size` number of pages.
    pub fn new(memory_allowance: u64, page_size: PageSize) -> Self {
        let num_pages = memory_allowance / page_size as u64;
        let target_memory_usage = num_pages * page_size as u64;
        tracing::debug!(
            "page file cache has capacity for {num_pages} {page_size:?} pages ({memory_target} total)",
            memory_target =
                humansize::format_size(target_memory_usage, humansize::DECIMAL),
        );

        let live_pages =
            LfuCacheTracker::with_capacity(num_pages as usize).into_shared();

        Self {
            num_pages,
            live_pages,
            page_size,
        }
    }

    /// Creates a new layer entry within the cache and allow it to hold
    /// upto `num_pages`.
    pub fn create_page_file_layer(
        &self,
        layer_id: LayerId,
        num_pages: usize,
    ) -> io::Result<Arc<CacheLayer>> {
        let memory = VirtualMemoryBlock::allocate(num_pages, self.page_size)?;

        tracing::debug!(
            virtual_memory_allocation = %humansize::format_size(
                num_pages as u64 * self.page_size as u64,
                humansize::DECIMAL,
            ),
            num_pages = num_pages,
            page_size = %self.page_size,
            layer_id = ?layer_id,
            "creating new layer"
        );

        let layer = CacheLayer {
            layer_id,
            live_pages: self.live_pages.clone(),
            memory,
        };

        Ok(Arc::new(layer))
    }

    /// Take the currently queued up evictions from the cache.
    ///
    /// These evictions need to be processed by the caller.
    pub fn take_cache_evictions(&self) -> VecDeque<(LayerId, PageIndex)> {
        let mut lock = self.live_pages.lock();
        mem::take(lock.evicted_entries())
    }

    #[inline]
    /// Returns the number of pages of capacity in the cache.
    pub fn page_capacity(&self) -> u64 {
        self.num_pages
    }

    #[inline]
    /// Returns the capacity of the cache in bytes.
    pub fn memory_capacity(&self) -> u64 {
        self.num_pages * self.page_size as u64
    }

    #[inline]
    /// Returns the approximate amount of memory used by the cache.
    pub fn pages_used(&self) -> usize {
        self.live_pages.lock().size()
    }

    #[inline]
    /// Returns the approximate amount of memory used by the cache.
    pub fn memory_used(&self) -> u64 {
        self.pages_used() as u64 * self.page_size() as u64
    }

    #[inline]
    /// Returns the page size used by the cache.
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }
}
