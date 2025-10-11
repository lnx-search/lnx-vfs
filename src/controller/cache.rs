use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, io};

use crate::cache::{CacheLayer, PageFileCache, PageSize};
use crate::ctx;
use crate::layout::PageGroupId;
use crate::page_data::DISK_PAGE_SIZE;

const GC_TARGET_RELEASE_SIZE: usize = 500 << 20;
const GC_MAX_INTERVAL: Duration = Duration::from_secs(1);
const GC_TRACE_N_COUNTS: usize = 5;

type LayerMap =
    papaya::HashMap<PageGroupId, Arc<CacheLayer>, foldhash::fast::RandomState>;

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
    state: Arc<CacheState>,
    cache_layer_id_counter: AtomicU64,
}

impl CacheController {
    /// Create a new [CacheController] using the given context.
    pub fn new(ctx: &ctx::Context) -> Self {
        let config: CacheConfig = ctx.config();
        let cache = PageFileCache::new(config.memory_allowance, PageSize::Std32KB);

        // NOTE: The system relies on these two sizes matching currently.
        assert_eq!(cache.page_size() as usize, DISK_PAGE_SIZE);

        let state = Arc::new(CacheState {
            cache,
            layers: LayerMap::default(),
        });

        if !config.disable_gc_worker {
            let worker = CacheGcWorker::new(state.clone());
            std::thread::Builder::new()
                .name("lnx_vfs_gc_worker".to_string())
                .spawn(move || worker.run())
                .expect("spawn cache gc worker");
        }

        Self {
            state,
            cache_layer_id_counter: AtomicU64::new(0),
        }
    }

    /// Get or create a new [CacheLayer] for the given page group.
    pub fn get_or_create_layer(
        &self,
        group: PageGroupId,
        num_pages: usize,
    ) -> io::Result<Arc<CacheLayer>> {
        let layers = self.layers().pin();
        if let Some(layer) = layers.get(&group) {
            return Ok(layer.clone());
        }

        let new_layer_id = self.next_cache_layer_id();
        let new_layer = self
            .cache()
            .create_page_file_layer(new_layer_id, num_pages)?;
        Ok(layers.get_or_insert(group, new_layer).clone())
    }

    /// Reassign the cache layer from one-page group to another.
    ///
    /// If the `old_group` layer does not exist, no new layer is assigned to the new group.
    pub fn reassign_layer(&self, old_group: PageGroupId, new_group: PageGroupId) {
        let layers = self.layers().pin();
        let Some(layer) = layers.remove(&old_group) else {
            return;
        };
        layers.insert(new_group, layer.clone());
    }

    /// Remove the cache layer for a given page group if it exists.
    pub fn remove_layer(&self, group: PageGroupId) {
        let layers = self.layers().pin();
        layers.remove(&group);
    }

    fn next_cache_layer_id(&self) -> u64 {
        self.cache_layer_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn layers(&self) -> &LayerMap {
        &self.state.layers
    }

    fn cache(&self) -> &PageFileCache {
        &self.state.cache
    }
}

struct CacheState {
    cache: PageFileCache,
    layers: LayerMap,
}

struct CacheGcWorker {
    last_gc_warning_log: Instant,
    last_gc_info_log: Instant,

    gc_cycle_interval: Duration,
    state: Arc<CacheState>,

    last_n_counts: [usize; GC_TRACE_N_COUNTS],
    last_n_cursor: usize,
}

impl CacheGcWorker {
    fn new(state: Arc<CacheState>) -> Self {
        Self {
            last_gc_warning_log: Instant::now(),
            last_gc_info_log: Instant::now(),
            gc_cycle_interval: GC_MAX_INTERVAL,
            state,
            last_n_counts: [0; GC_TRACE_N_COUNTS],
            last_n_cursor: 0,
        }
    }

    fn run(mut self) {
        tracing::info!("cache gc thread is starting...");

        loop {
            if self.should_exit() {
                tracing::info!("cache gc thread is exiting");
                break;
            }

            let start = Instant::now();
            self.run_gc_cycle();
            self.log_gc_info();
            let elapsed = start.elapsed();

            if self.should_exit() {
                tracing::info!("cache gc thread is exiting");
                break;
            }

            self.wait_for_gc_cycle(elapsed);
        }
    }

    fn should_exit(&self) -> bool {
        Arc::strong_count(&self.state) == 1
    }

    fn run_gc_cycle(&mut self) {
        let mut total_pages_reclaimed = 0;
        let layers = self.state.layers.pin();
        for layer in layers.values() {
            layer.run_cache_tasks();
            total_pages_reclaimed += layer.run_cleanup();
        }
        drop(layers);
        self.write_last_num_frees(total_pages_reclaimed);
    }

    /// Wait for a given time period before returning, triggering the next GC cycle.
    ///
    /// The interval between cycles is adaptive and calculated based on the activity
    /// of the cache an amount of work done in the last N cycles.
    fn wait_for_gc_cycle(&mut self, elapsed: Duration) {
        let max_num_frees = self.max_frees_in_window();
        let last_interval = self.gc_cycle_interval;
        self.gc_cycle_interval =
            adaptive_gc_interval(self.gc_cycle_interval, max_num_frees);
        let wait_for = self.gc_cycle_interval.saturating_sub(elapsed);
        if !wait_for.is_zero() {
            std::thread::sleep(wait_for);
        } else if elapsed > last_interval {
            self.log_gc_slow();
        }
    }

    fn write_last_num_frees(&mut self, num_frees: usize) {
        self.last_n_counts[self.last_n_cursor] = num_frees;
        self.last_n_cursor = (self.last_n_cursor + 1) % GC_TRACE_N_COUNTS;
    }

    fn max_frees_in_window(&self) -> usize {
        let mut max_frees = 0;
        for n in self.last_n_counts {
            max_frees = cmp::max(max_frees, n);
        }
        max_frees
    }

    fn log_gc_slow(&mut self) {
        let elapsed = self.last_gc_warning_log.elapsed();
        if elapsed < Duration::from_secs(3) {
            return;
        }

        let history = self.render_history();
        tracing::warn!(
            "cache gc cannot keep up! interval: {:?}, last runs: {history}",
            self.gc_cycle_interval,
        );
        self.last_gc_warning_log = Instant::now();
    }

    fn log_gc_info(&mut self) {
        let elapsed = self.last_gc_info_log.elapsed();
        if elapsed < Duration::from_secs(10) {
            return;
        }

        let max = self.max_frees_in_window();
        if max == 0 {
            return;
        }

        let history = self.render_history();
        tracing::warn!(
            "cache gc status interval: {:?}, last runs: {history}",
            self.gc_cycle_interval,
        );
    }

    fn render_history(&self) -> String {
        use std::fmt::Write;
        let mut last_runs = String::with_capacity(40);
        for (idx, num_frees) in self.last_n_counts.into_iter().enumerate() {
            let memory_size = (num_frees * DISK_PAGE_SIZE) as u64;
            write!(
                last_runs,
                " {idx}:{}",
                humansize::format_size(memory_size, humansize::DECIMAL),
            )
            .unwrap();
        }
        last_runs
    }
}

/// Calculates an adaptive interval to trigger the next GC cycle based
/// on the current activity of the cache.
///
/// The base target is to free `500MB` of memory per sweep, if the number of frees
/// ends up being higher than this, we lower the interval to trigger the GC more often.
fn adaptive_gc_interval(base_interval: Duration, num_frees: usize) -> Duration {
    if num_frees == 0 {
        return GC_MAX_INTERVAL;
    }

    let memory_reclaimed = num_frees * DISK_PAGE_SIZE;
    let target_ratio = GC_TARGET_RELEASE_SIZE as f64 / memory_reclaimed as f64;
    let adjusted_interval =
        Duration::from_secs_f64(base_interval.as_secs_f64() * target_ratio);
    cmp::min(adjusted_interval, GC_MAX_INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::gc_1sec_interval_matching_rate(
        Duration::from_secs(1),
        16_000,
        Duration::from_secs(1)
    )]
    #[case::gc_1sec_interval_lower_rate(Duration::from_secs(1), 10_000, GC_MAX_INTERVAL)]
    #[case::gc_1sec_interval_higher_rate(
        Duration::from_secs(1),
        28_000,
        Duration::from_millis(571)
    )]
    #[case::gc_already_adapted_interval_raises1(
        Duration::from_millis(571),
        8_000,
        Duration::from_millis(1000)
    )]
    #[case::gc_already_adapted_interval_raises2(
        Duration::from_millis(200),
        8_000,
        Duration::from_millis(400)
    )]
    #[case::no_frees_defaults_to_max(Duration::from_millis(200), 0, GC_MAX_INTERVAL)]
    fn test_adaptive_gc_interval(
        #[case] input_interval: Duration,
        #[case] num_frees: usize,
        #[case] expected_interval: Duration,
    ) {
        let adjusted_interval = adaptive_gc_interval(input_interval, num_frees);
        let millis = adjusted_interval.as_millis() as u64;
        let rounded_interval = Duration::from_millis(millis);
        assert_eq!(rounded_interval, expected_interval);
    }

    fn create_empty_worker() -> CacheGcWorker {
        let cache = PageFileCache::new(128 << 10, PageSize::Std32KB);
        let state = Arc::new(CacheState {
            cache,
            layers: Default::default(),
        });
        CacheGcWorker::new(state)
    }

    #[test]
    fn test_cache_gc_worker_last_n_counts() {
        let mut worker = create_empty_worker();
        assert_eq!(worker.max_frees_in_window(), 0);

        worker.write_last_num_frees(5);
        assert_eq!(worker.max_frees_in_window(), 5);
        assert_eq!(worker.last_n_counts, [5, 0, 0, 0, 0]);

        worker.write_last_num_frees(4);
        assert_eq!(worker.max_frees_in_window(), 5);
        assert_eq!(worker.last_n_counts, [5, 4, 0, 0, 0]);

        worker.write_last_num_frees(1);
        worker.write_last_num_frees(2);
        worker.write_last_num_frees(3);
        worker.write_last_num_frees(8);
        assert_eq!(worker.max_frees_in_window(), 8);
        assert_eq!(worker.last_n_counts, [8, 4, 1, 2, 3]);
    }

    #[test]
    fn test_cache_gc_worker_wait_behaviour() {
        let mut worker = create_empty_worker();

        worker.write_last_num_frees(1);

        let now = Instant::now();
        worker.wait_for_gc_cycle(Duration::from_secs(1));
        let elapsed = now.elapsed().as_millis();
        assert_eq!(elapsed, 0);

        worker.write_last_num_frees(64_000);
        let now = Instant::now();
        worker.wait_for_gc_cycle(Duration::default());
        let elapsed = now.elapsed().as_millis();
        assert!(elapsed >= 250);
        assert!(elapsed <= 260);
    }

    #[test]
    fn test_cache_gc_exits() {
        let worker = create_empty_worker();
        worker.run();
        // Worker should only complete if it is signalled to shut down, i.e. it's parent
        // being lost.
    }

    #[tokio::test]
    async fn test_cache_frees_memory() {
        let mut worker = create_empty_worker();

        let layer = worker.state.cache.create_page_file_layer(1, 3).unwrap();
        let layers = worker.state.layers.pin();
        layers.insert(PageGroupId(1), layer.clone());
        drop(layers);

        let prepared = layer.prepare_read(0..1);
        for permit in prepared.get_outstanding_write_permits() {
            prepared.write_page(permit, &vec![1; 32 << 10]);
        }
        let read = match prepared.try_finish() {
            Err(_) => unreachable!(),
            Ok(read) => read,
        };
        drop(read);

        unsafe { layer.dirty_page_range(0..1) };

        worker.run_gc_cycle();
        assert_eq!(layer.backlog_size(), 0);
    }
}
