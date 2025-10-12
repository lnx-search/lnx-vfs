use std::ops::Range;

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
#[case::all(0..10)]
#[case::prefix(0..3)]
#[case::suffix(7..9)]
#[case::punch_hole(4..7)]
#[trace]
fn test_dirty_pages_invalidates_cache_with_no_live_readers(
    #[case] page_range: Range<PageIndex>,
) {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..10);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    let read_view = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };
    assert!(read_view.iter().all(|v| *v == 4));
    drop(read_view);

    unsafe { layer.dirty_page_range(page_range.clone()) };

    let prepared = layer.prepare_read(0..10);

    let expected_missing_pages = page_range.collect::<Vec<_>>();
    let remaining_pages = prepared
        .get_outstanding_write_permits()
        .map(|permit| permit.page())
        .collect::<Vec<_>>();
    assert_eq!(remaining_pages, expected_missing_pages);
}

#[rstest::rstest]
fn test_dirty_pages_already_free() {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    // Should be a no-op
    unsafe { layer.dirty_page_range(0..10) };
}

#[rstest::rstest]
fn test_dirty_pages_page_locked() {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let mut prepared = layer.prepare_read(0..10);
    let permits = prepared.get_outstanding_write_permits().collect::<Vec<_>>();

    let handle = std::thread::spawn({
        let layer = layer.clone();
        move || {
            assert_eq!(layer.backlog_size(), 0);
            unsafe { layer.dirty_page_range(0..10) };
            assert_eq!(layer.backlog_size(), 10);
        }
    });
    std::thread::sleep(std::time::Duration::from_millis(100));

    let buffer_data = vec![4; 8 << 10];
    for permit in permits {
        prepared.write_page(permit, &buffer_data);
    }

    let read_view = loop {
        prepared = match prepared.try_finish() {
            Ok(read_view) => break read_view,
            Err(prepared) => prepared,
        };

        for permit in prepared.get_outstanding_write_permits() {
            prepared.write_page(permit, &buffer_data);
        }
    };

    handle.join().unwrap();

    // Read view should still be valid.
    assert!(read_view.iter().all(|v| *v == 4));
    drop(format!("{read_view:?}"));

    // Check clone behaviour
    let read_view_clone = read_view.clone();
    assert!(read_view_clone.iter().all(|v| *v == 4));

    // The backlog will remain because of the generation cleanup system.
    assert_eq!(layer.backlog_size(), 10);
}

#[rstest::rstest]
#[case::all(0..10)]
#[case::prefix(0..3)]
#[case::suffix(7..9)]
#[case::punch_hole(4..7)]
#[trace]
fn test_dirty_pages_invalidates_cache_with_live_readers(
    #[case] page_range: Range<PageIndex>,
) {
    let cache = PageFileCache::new(512 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..10);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    let read_view = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };

    // To future self, this test put some crazy thoughts in my mind wondering
    // how the fuck the system handles pages being dirtied while still holding
    // references.
    unsafe { layer.dirty_page_range(page_range.clone()) };

    let prepared = layer.prepare_read(0..10);

    let expected_missing_pages = page_range.collect::<Vec<_>>();
    let remaining_pages = prepared
        .get_outstanding_write_permits()
        .map(|permit| permit.page())
        .collect::<Vec<_>>();
    assert_eq!(remaining_pages, expected_missing_pages);

    drop(read_view);
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
    unsafe { layer.dirty_page_range(0..5) };

    // Dirtying the page should not cause immediate UB.
    assert!(read_view1.iter().all(|v| *v == 4));

    let prepared = layer.prepare_read(0..10);

    let buffer_data = vec![2; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    // Read view 2 should observe the new changes.
    let read_view2 = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };
    assert!(read_view2[..5 * (8 << 10)].iter().all(|v| *v == 2));
    assert!(read_view2[5 * (8 << 10)..].iter().all(|v| *v == 4));

    // This is UB because our immutable buffer has changed underneath us.
    assert!(!read_view1.iter().all(|v| *v == 4));
}

#[rstest::rstest]
fn test_lfu_does_not_evict_within_capacity() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "cache::eviction_callback",
        "panic(cache should not evict pages)",
    )
    .unwrap();

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..4);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    scenario.teardown();
}

#[rstest::rstest]
fn test_lfu_does_evict_when_capacity_exceeded() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let layer = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer.prepare_read(0..10);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    // TODO: Handle this better with metrics
    layer.run_cache_tasks();
    assert_eq!(cache.pages_used(), 4);

    let read_view = match prepared.try_finish() {
        Ok(read_view) => read_view,
        Err(_) => panic!("read should be successful after writes have occurred"),
    };
    assert!(read_view.iter().all(|v| *v == 4));

    layer.run_cleanup();
}

#[rstest::rstest]
fn test_lfu_eviction_with_locked_page() {
    let cache = PageFileCache::new(32 << 10, PageSize::Std8KB);

    let layer1 = cache
        .create_page_file_layer(1, 10)
        .expect("create page cache layer");

    let prepared = layer1.prepare_read(0..10);

    let buffer_data = vec![4; 8 << 10];
    for permit in prepared.get_outstanding_write_permits() {
        prepared.write_page(permit, &buffer_data);
    }

    let permits = prepared.get_outstanding_write_permits().collect::<Vec<_>>();
    layer1.run_cache_tasks();
    drop(permits);
    drop(prepared);

    assert_eq!(layer1.backlog_size(), 6);
    layer1.run_cleanup();
    assert_eq!(layer1.backlog_size(), 0);
}
