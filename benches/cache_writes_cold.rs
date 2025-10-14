mod cache_shared;

use std::cmp;
use std::hint::black_box;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use lnx_vfs::bench::*;

use self::cache_shared::*;

const NUM_RUNS: usize = 100;
const CONFIG: &[BenchmarkConfig] = &[
    BenchmarkConfig::new(PageSize::Std8KB, 512 << 20, 512 << 20),
    BenchmarkConfig::new(PageSize::Std8KB, 2 << 30, 2 << 30),
    BenchmarkConfig::new(PageSize::Std32KB, 512 << 20, 512 << 20),
    BenchmarkConfig::new(PageSize::Std32KB, 2 << 30, 2 << 30),
    BenchmarkConfig::new(PageSize::Std64KB, 512 << 20, 512 << 20),
    BenchmarkConfig::new(PageSize::Std64KB, 2 << 30, 2 << 30),
    BenchmarkConfig::new(PageSize::Std128KB, 512 << 20, 512 << 20),
    BenchmarkConfig::new(PageSize::Std128KB, 2 << 30, 2 << 30),
];

static PAGE_LEN_RANGES: &[Range<usize>] = &[1..2, 50..101, 5000..5101, 50_000..50_101];

fn main() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    tracing_subscriber::fmt::init();

    tracing::info!("starting benchmark");

    let mut results = Vec::new();
    for &config in CONFIG {
        tracing::info!("starting: {:?}", config);
        for page_len_range in PAGE_LEN_RANGES.iter().cloned() {
            let result = bench_cold_page_writes(config, page_len_range);
            results.push(result);
        }
    }

    tracing::info!("benchmark complete:");
    for result in results {
        tracing::info!("{result}");
    }
}

#[inline(never)]
#[unsafe(no_mangle)]
fn bench_cold_page_writes(
    config: BenchmarkConfig,
    page_len_range: Range<usize>,
) -> BenchmarkMetrics {
    let name = format!(
        "Cold Writes - page_size:{}, cache_size:{}",
        config.page_size,
        humansize::format_size(config.cache_size, humansize::DECIMAL),
    );
    let mut metrics =
        BenchmarkMetrics::new(name.into(), config.page_size, page_len_range.clone());

    let cache = PageFileCache::new(config.cache_size, config.page_size);
    let layer_num_pages = config.layer_size / config.page_size as u64;

    let layer = cache
        .create_page_file_layer(1, layer_num_pages as usize)
        .expect("create page cache layer");

    let buffer = vec![4; 128 << 10];
    for _ in 0..NUM_RUNS {
        let page_start = fastrand::u32(0..layer_num_pages as u32);
        let num_pages = fastrand::usize(page_len_range.clone()) as u32;
        let page_end = cmp::min(page_start + num_pages, layer_num_pages as u32);
        let page_range = page_start..page_end;
        let true_pages_read = page_range.len();

        // unsafe { layer.dirty_page_range(page_range.clone()) };
        layer.advance_gc_generation();

        let start = Instant::now();
        let result = black_box(do_write_layer(
            black_box(&layer),
            black_box(page_range),
            black_box(&buffer),
        ));
        metrics.elapsed += start.elapsed();
        drop(result);

        metrics.num_iters += 1;
        metrics.num_pages_read += true_pages_read;
    }

    metrics
}

#[inline(never)]
#[unsafe(no_mangle)]
fn do_write_layer(
    layer: &Arc<CacheLayer>,
    read_range: Range<PageIndex>,
    buffer: &[u8],
) -> ReadRef {
    let mut prepared = layer.prepare_read(read_range);
    loop {
        prepared = match prepared.try_finish() {
            Ok(read_view) => break read_view,
            Err(prepared) => prepared,
        };

        for permit in prepared.get_outstanding_write_permits() {
            prepared.write_page(permit, &buffer[..layer.page_size() as usize]);
        }
    }
}
