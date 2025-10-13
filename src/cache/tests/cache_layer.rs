use std::ops::Range;

use crate::cache::evictions::EvictionBacklog;
use crate::cache::{PageFileCache, PageIndex, PageSize};

#[rstest::rstest]
#[case::page_8kb(PageSize::Std8KB)]
#[case::page_32kb(PageSize::Std32KB)]
#[case::page_64kb(PageSize::Std64KB)]
#[case::page_128kb(PageSize::Std128KB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::page_2mb(PageSize::Huge2MB)]
#[trace]
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
    assert_eq!(cache.page_size(), page_size);

    let num_pages = fastrand::usize(1..1000);
    let layer = cache
        .create_page_file_layer(1, num_pages)
        .expect("create page cache layer");
    assert_eq!(layer.id(), 1);
    assert_eq!(layer.page_size(), page_size);
}

#[rstest::rstest]
#[case::empty1(0..0)]
#[case::empty2(5..5)]
#[case::empty3(9..9)]
#[case::full(0..10)]
#[case::partial1(0..2)]
#[case::partial2(1..7)]
#[case::partial3(3..5)]
#[trace]
fn test_prepare_read_range_handling(#[case] page_range: Range<PageIndex>) {
    let page_size = page_range.len();
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(page_range.clone());

    let permits = prepared
        .get_outstanding_write_permits()
        .map(|permit| permit.page())
        .collect::<Vec<_>>();
    assert_eq!(permits.len(), page_size);
    assert_eq!(permits, page_range.collect::<Vec<_>>());

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    let read_view = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };

    let expected_buffer = vec![4; page_size * (8 << 10)];
    assert_eq!(read_view.as_ref(), &expected_buffer);
}

#[test]
fn test_read_clone_and_debug() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 4)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..4);
    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }
    assert_eq!(format!("{prepared:?}"), "PreparedRead(page_range=0..4)");

    let read = prepared.try_finish().unwrap();
    assert_eq!(
        format!("{read:?}"),
        format!("ReadRef(start={:?}, len=32768)", read.as_ptr()),
    );

    let read_clone = read.clone();
    assert!(std::ptr::addr_eq(read.as_ptr(), read_clone.as_ptr()));

    drop(read);
    assert!(read_clone.as_ref().iter().all(|v| *v == 4));
}

#[rstest::rstest]
#[case::start(&[0])]
#[case::end(&[9])]
#[case::middle(&[4])]
#[case::many_sparse(&[2, 7, 8])]
#[case::many_dense(&[3, 4, 5, 6])]
#[case::all(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
#[trace]
fn test_partial_write(#[case] skip_pages: &[PageIndex]) {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let buffer_data = vec![4; 8 << 10];
    let mut prepared = layer.prepare_read(0..10);
    for permit in prepared.get_outstanding_write_permits() {
        if skip_pages.contains(&permit.page()) {
            continue;
        }
        prepared.write_page(permit, &buffer_data);
    }

    prepared = match prepared.try_finish() {
        Ok(_) => {
            panic!("read should not be available as page data is yet to be completed")
        },
        Err(prepared) => prepared,
    };

    let expected_remaining_pages = (0..10)
        .filter(|page| skip_pages.contains(page))
        .collect::<Vec<_>>();
    let remaining_pages = prepared
        .get_outstanding_write_permits()
        .map(|permit| permit.page())
        .collect::<Vec<_>>();
    assert_eq!(remaining_pages, expected_remaining_pages);
}

#[rstest::rstest]
#[allow(clippy::reversed_empty_ranges)]
#[case::start_before_end(5..2)]
#[case::start_out_of_bounds(500..550)]
#[case::end_out_of_bounds(0..50)]
#[trace]
#[should_panic]
fn test_read_outside_bounds_panic(#[case] page_range: Range<PageIndex>) {
    let cache = PageFileCache::new(256 << 10, PageSize::Std8KB);

    let layer1 = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let _read = layer1.prepare_read(page_range);
}

#[rstest::rstest]
fn test_force_reset_pages() {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..10);
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &[1; 8 << 10]);
    }
    drop(prepared);

    unsafe { layer.reset().expect("memory should reset") };

    layer
        .prepare_read(0..10)
        .try_finish()
        .expect_err("data should have been cleared");
}

#[rstest::rstest]
fn test_dirty_pages_updates_memory_for_old_readers_ub() {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..10);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    // Read view 1 should never change after the prepared read is finished.
    let read_view1 = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };
    assert!(read_view1.iter().all(|v| *v == 4));

    // Dirty pages should invalidate the cache in the given ranges.
    unsafe { layer.reset().unwrap() };

    // Dirtying the page should cause immediate UB.
    assert!(read_view1.iter().all(|v| *v == 0));
}

#[rstest::rstest]
fn test_lfu_does_not_evict_within_capacity() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..4);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    let evicted = cache.take_cache_evictions();
    assert!(evicted.is_empty());
}

#[test]
fn test_lfu_does_evict_when_capacity_exceeded() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 6)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..6);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    assert_eq!(cache.pages_used(), 4);
    let evicted = cache.take_cache_evictions();
    assert_eq!(evicted, [(1, 0), (1, 1)]);

    let read_view = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };
    assert!(read_view.iter().all(|v| *v == 4));
}

#[test]
fn test_evictions_and_bookkeeping_ran() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 6)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..6);
    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    let evictions = cache.take_cache_evictions();
    let mut backlog = EvictionBacklog::default();
    for (_, page) in evictions {
        backlog.mark(page);
    }

    layer.process_evictions(&mut backlog);
    assert_eq!(backlog.size(), 2);

    layer.advance_gc_generation();

    drop(prepared);
    layer.process_evictions(&mut backlog);
    dbg!(&backlog);
    assert!(backlog.is_empty());

    layer.try_collapse_memory();
}
