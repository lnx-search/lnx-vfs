use std::borrow::Cow;
use std::ops::Range;
use std::time::Duration;

use lnx_vfs::bench::PageSize;

pub struct BenchmarkMetrics {
    name: Cow<'static, str>,
    pub elapsed: Duration,
    pub num_iters: usize,
    pub num_pages_read: usize,
    page_size: PageSize,
    read_size: Range<usize>,
}

impl BenchmarkMetrics {
    pub fn new(
        name: Cow<'static, str>,
        page_size: PageSize,
        read_size: Range<usize>,
    ) -> Self {
        Self {
            name,
            elapsed: Duration::default(),
            num_iters: 0,
            num_pages_read: 0,
            page_size,
            read_size,
        }
    }
}

impl std::fmt::Display for BenchmarkMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_transfer = self.num_pages_read as u64 * self.page_size as u64;
        let transfer_per_second =
            (total_transfer as f64 / self.elapsed.as_secs_f64()) as u64;
        write!(
            f,
            "{:<55} | num_pages: {:<12} | {:<8.2?} total, {:<8.2?}/iter | {}/s",
            self.name,
            format!("{:?}", self.read_size),
            self.elapsed,
            self.elapsed / self.num_iters as u32,
            humansize::format_size(transfer_per_second, humansize::DECIMAL),
        )
    }
}

#[derive(Copy, Clone)]
pub struct BenchmarkConfig {
    pub page_size: PageSize,
    pub cache_size: u64,
    pub layer_size: u64,
}

impl std::fmt::Debug for BenchmarkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Config({}, cache_size={}, layer_size={})",
            self.page_size,
            humansize::format_size(self.cache_size, humansize::DECIMAL),
            humansize::format_size(self.layer_size, humansize::DECIMAL),
        )
    }
}

impl BenchmarkConfig {
    pub const fn new(page_size: PageSize, cache_size: u64, layer_size: u64) -> Self {
        Self {
            page_size,
            cache_size,
            layer_size,
        }
    }
}
