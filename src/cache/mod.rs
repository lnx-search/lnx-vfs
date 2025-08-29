mod evictions;
mod mem_block;
mod page_file;

use std::io;
use std::sync::Arc;

use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use parking_lot::Mutex;

use self::evictions::PendingEvictions;
use self::mem_block::VirtualMemoryBlock;
pub use self::mem_block::{PageIndex, PageSize};
pub use self::page_file::CacheLayer;

/// A unique identifier for a cache layer.
pub type LayerId = u32;

type LivePagesLfu = moka::sync::Cache<(LayerId, PageIndex), (), ahash::RandomState>;
type LayerEvictionSenders =
    ahash::HashMap<LayerId, crossbeam_channel::Sender<PageIndex>>;

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
    live_pages: LivePagesLfu,
    num_pages: u64,
    page_size: PageSize,
    layer_eviction_senders: Arc<Mutex<LayerEvictionSenders>>,
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
        tracing::info!(
            "page file cache has capacity for {num_pages} {page_size:?} pages ({memory_target} total)",
            memory_target =
                humansize::format_size(target_memory_usage, humansize::DECIMAL),
        );

        let layer_eviction_senders =
            Arc::new(Mutex::new(LayerEvictionSenders::default()));

        let live_pages = moka::sync::Cache::builder()
            .max_capacity(num_pages)
            .eviction_listener({
                let layer_eviction_senders = layer_eviction_senders.clone();
                move |key: Arc<(LayerId, PageIndex)>, _, cause| {
                    if !matches!(cause, RemovalCause::Size | RemovalCause::Expired) {
                        return;
                    }

                    let (file_id, page) = key.as_ref();

                    let mut lock = layer_eviction_senders.lock();
                    let did_error = if let Some(tx) = lock.get(file_id) {
                        tx.send(*page).is_err()
                    } else {
                        return;
                    };

                    if did_error {
                        lock.remove(file_id);
                    }
                }
            })
            .eviction_policy(EvictionPolicy::tiny_lfu())
            .build_with_hasher(ahash::RandomState::new());

        Self {
            num_pages,
            live_pages,
            page_size,
            layer_eviction_senders,
        }
    }

    /// Creates a new layer entry within the cache and allow it to hold
    /// upto `num_pages`.
    pub fn create_page_file_layer(
        &self,
        layer_id: LayerId,
        num_pages: usize,
    ) -> io::Result<Arc<CacheLayer>> {
        let (pending_evictions, tx) = PendingEvictions::new();
        let memory = VirtualMemoryBlock::allocate(num_pages, self.page_size)?;

        tracing::info!(
            virtual_memory_allocation = %humansize::format_size(
                num_pages as u64 * self.page_size as u64,
                humansize::DECIMAL,
            ),
            num_pages = num_pages,
            page_size = %self.page_size,
            layer_id = ?layer_id,
            "creating new layer"
        );

        self.register_file_layer_with_listener(layer_id, tx);

        let layer = CacheLayer {
            layer_id,
            live_pages: self.live_pages.clone(),
            memory,
            pending_evictions,
        };

        Ok(Arc::new(layer))
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
    pub fn pages_used(&self) -> u64 {
        self.live_pages.entry_count()
    }

    #[inline]
    /// Returns the approximate amount of memory used by the cache.
    pub fn memory_used(&self) -> u64 {
        self.live_pages.weighted_size()
    }

    #[inline]
    /// Returns the page size used by the cache.
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    fn register_file_layer_with_listener(
        &self,
        layer_id: LayerId,
        tx: crossbeam_channel::Sender<PageIndex>,
    ) {
        let mut lock = self.layer_eviction_senders.lock();
        lock.insert(layer_id, tx);
    }
}
