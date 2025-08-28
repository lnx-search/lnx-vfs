use std::ops::Range;
use crate::cache::{PageFileCache, PageIndex, PageSize};
use crate::layout::PageFileId;

#[rstest::rstest]
#[trace]
#[case::page_8kb(PageSize::Std8KB)]
#[case::page_32kb(PageSize::Std32KB)]
#[case::page_64kb(PageSize::Std64KB)]
#[case::page_128kb(PageSize::Std128KB)]
#[cfg_attr(feature = "test-huge-pages", case::page_2mb(PageSize::Huge2MB))]
fn test_cache_create(
    #[values(23623578623, 987052376923, 12908673556789)] rng_seed: u64,
    #[values(0, 1, 16 << 10, 256 << 20, 1 << 30)] capacity: u64,
    #[case] page_size: PageSize,
) {
    fastrand::seed(rng_seed);

    let expected_capacity_pages = capacity / page_size as u64;
    let expected_capacity_memory = expected_capacity_pages * page_size as u64;

    let cache = PageFileCache::new(capacity, page_size);
    assert_eq!(cache.page_capacity(), expected_capacity_pages);
    assert_eq!(cache.memory_capacity(), expected_capacity_memory);
    assert_eq!(cache.memory_used(), 0);
    assert_eq!(cache.pages_used(), 0);

    let num_pages = fastrand::usize(1..1000);
    let _layer = cache
        .create_page_file_layer(PageFileId(1), num_pages)
        .expect("create page cache layer");
}

#[rstest::rstest]
#[trace]
#[case::empty1(PageIndex(0)..PageIndex(0))]
#[case::empty2(PageIndex(5)..PageIndex(5))]
#[case::empty3(PageIndex(9)..PageIndex(9))]
#[case::full(PageIndex(0)..PageIndex(10))]
#[case::partial1(PageIndex(0)..PageIndex(2))]
#[case::partial2(PageIndex(1)..PageIndex(7))]
#[case::partial3(PageIndex(3)..PageIndex(5))]
fn test_cache_layer_prepare_read(#[case] page_range: Range<PageIndex>) {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer1 = cache
        .create_page_file_layer(PageFileId(1), 10)
        .expect("create page cache layer");

    let read = layer1.prepare_read(page_range);

    let permits = read
        .get_outstanding_write_permits()
        .collect::<Vec<_>>();
    assert_eq!(permits.len(), 1);
}


#[rstest::rstest]
#[trace]
#[case::start_before_end(PageIndex(5)..PageIndex(2))]
#[case::start_out_of_bounds(PageIndex(500)..PageIndex(550))]
#[case::end_out_of_bounds(PageIndex(0)..PageIndex(50))]
#[should_panic]
fn test_cache_layer_read_outside_bounds_panic(#[case] page_range: Range<PageIndex>) {
    let cache = PageFileCache::new(256 << 10, PageSize::Std8KB);

    let layer1 = cache
        .create_page_file_layer(PageFileId(1), 10)
        .expect("create page cache layer");

    let _read = layer1.prepare_read(page_range);
}
