use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cache::{CacheLayer, PageFileCache, PageSize};
use crate::ctx;
use crate::layout::PageGroupId;
use crate::page_data::DISK_PAGE_SIZE;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
/// Configuration options for the cache controller.
pub struct CacheConfig {
    /// The target amount of memory for the cache to consume.
    pub memory_allowance: u64,
    /// Disable the GC background worker for the cache.
    pub disable_gc_worker: bool,
}

/// The cache controller manages the caching layer for IO.
pub struct CacheController {
    cache: PageFileCache,
    layers: papaya::HashMap<PageGroupId, Arc<CacheLayer>, foldhash::fast::RandomState>,
    cache_layer_id_counter: AtomicU64,
}

impl CacheController {
    /// Create a new [CacheController] using the given context.
    pub fn new(ctx: &ctx::FileContext) -> Self {
        let config: CacheConfig = ctx.config();
        let cache = PageFileCache::new(config.memory_allowance, PageSize::Std32KB);

        // NOTE: The system relies on these two sizes matching currently.
        assert_eq!(cache.page_size() as usize, DISK_PAGE_SIZE);

        Self {
            cache,
            layers: papaya::HashMap::default(),
            cache_layer_id_counter: AtomicU64::new(0),
        }
    }

    /// Get or create a new [CacheLayer] for the given page group.
    pub fn get_or_create_layer(
        &self,
        group: PageGroupId,
        num_pages: usize,
    ) -> io::Result<Arc<CacheLayer>> {
        let layers = self.layers.pin();
        if let Some(layer) = layers.get(&group) {
            return Ok(layer.clone());
        }

        let new_layer_id = self.next_cache_layer_id();
        let new_layer = self.cache.create_page_file_layer(new_layer_id, num_pages)?;
        Ok(layers.get_or_insert(group, new_layer).clone())
    }

    /// Reassign the cache layer from one-page group to another.
    ///
    /// If the `old_group` layer does not exist, no new layer is assigned to the new group.
    pub fn reassign_layer(&self, old_group: PageGroupId, new_group: PageGroupId) {
        let layers = self.layers.pin();
        let Some(layer) = layers.remove(&old_group) else {
            return;
        };
        layers.insert(new_group, layer.clone());
    }

    /// Remove the cache layer for a given page group if it exists.
    pub fn remove_layer(&self, group: PageGroupId) {
        let layers = self.layers.pin();
        layers.remove(&group);
    }

    fn next_cache_layer_id(&self) -> u64 {
        self.cache_layer_id_counter.fetch_add(1, Ordering::Relaxed)
    }
}
