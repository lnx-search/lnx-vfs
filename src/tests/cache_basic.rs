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
#[case::empty1(0..0)]
#[case::empty2(5..5)]
#[case::empty3(9..9)]
#[case::full(0..10)]
#[case::partial1(0..2)]
#[case::partial2(1..7)]
#[case::partial3(3..5)]
fn test_cache_layer_prepare_read_range_handling(#[case] page_range: Range<PageIndex>) {
    let page_size = page_range.len();
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer1 = cache
        .create_page_file_layer(PageFileId(1), 10)
        .expect("create page cache layer");

    let read = layer1.prepare_read(page_range.clone());

    let permits = read
        .get_outstanding_write_permits()
        .map(|permit| permit.page())
        .collect::<Vec<_>>();
    assert_eq!(permits.len(), page_size);
    assert_eq!(permits, page_range.collect::<Vec<_>>());

    let buffer_data = vec![4; 8 << 10];
    for permit in read.get_outstanding_write_permits() {
        read.write_page(permit, &buffer_data);
    }

    let read_view = match read.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };

    let expected_buffer = vec![4; page_size * (8 << 10)];
    assert_eq!(read_view.as_ref(), &expected_buffer);
}

#[rstest::rstest]
#[trace]
#[case::start_before_end(5..2)]
#[case::start_out_of_bounds(500..550)]
#[case::end_out_of_bounds(0..50)]
#[should_panic]
fn test_cache_layer_read_outside_bounds_panic(#[case] page_range: Range<PageIndex>) {
    let cache = PageFileCache::new(256 << 10, PageSize::Std8KB);

    let layer1 = cache
        .create_page_file_layer(PageFileId(1), 10)
        .expect("create page cache layer");

    let _read = layer1.prepare_read(page_range);
}
