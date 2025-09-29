use crate::disk_allocator::{AllocSpan, InitState, PageAllocator};
use crate::layout::PageFileId;
use crate::page_data::{MAX_NUM_PAGES, NUM_PAGES_PER_BLOCK};
use crate::page_file_allocator::PageFileAllocator;

#[test]
fn test_empty_write_controller() {
    let controller = PageFileAllocator::default();
    let tx = controller.get_alloc_tx(5);
    assert!(tx.is_none());
}

#[should_panic(expected = "BUG: page file does not exist while trying to free pages")]
#[test]
fn test_panic_on_free_non_existent_file() {
    let controller = PageFileAllocator::default();
    controller.free(PageFileId(1), 44, 10);
}

#[test]
fn test_page_alloc_commited() {
    let controller = PageFileAllocator::default();
    let allocator = PageAllocator::new(InitState::Free);
    controller.insert_page_file(PageFileId(1), allocator);

    let mut tx = controller
        .get_alloc_tx(5)
        .expect("page allocation transaction should be produced");
    assert_eq!(tx.page_file_id(), PageFileId(1));
    assert_eq!(
        tx.spans(),
        &[
            AllocSpan {
                start_page: 0,
                span_len: 4
            },
            AllocSpan {
                start_page: 4,
                span_len: 1
            },
        ]
    );
    tx.commit();
    drop(tx);

    // The allocation cursors should not be reset due to the block internally
    // within the allocator being re-marked as free.
    let tx = controller
        .get_alloc_tx(5)
        .expect("page allocation transaction should be produced");
    assert_eq!(tx.page_file_id(), PageFileId(1));
    assert_eq!(
        tx.spans(),
        &[
            AllocSpan {
                start_page: 5,
                span_len: 4
            },
            AllocSpan {
                start_page: 9,
                span_len: 1
            },
        ]
    );
}

#[test]
fn test_page_alloc_reverted() {
    let controller = PageFileAllocator::default();
    let allocator = PageAllocator::new(InitState::Free);
    controller.insert_page_file(PageFileId(1), allocator);

    let tx = controller
        .get_alloc_tx(5)
        .expect("page allocation transaction should be produced");
    assert_eq!(tx.page_file_id(), PageFileId(1));
    assert_eq!(
        tx.spans(),
        &[
            AllocSpan {
                start_page: 0,
                span_len: 4
            },
            AllocSpan {
                start_page: 4,
                span_len: 1
            },
        ]
    );
    drop(tx);

    // The allocation cursors should not be reset due to the block internally
    // within the allocator becoming free.
    let tx = controller
        .get_alloc_tx(5)
        .expect("page allocation transaction should be produced");
    assert_eq!(tx.page_file_id(), PageFileId(1));
    assert_eq!(
        tx.spans(),
        &[
            AllocSpan {
                start_page: 0,
                span_len: 4
            },
            AllocSpan {
                start_page: 4,
                span_len: 1
            },
        ]
    );
}

#[test]
fn test_page_alloc_page_file_full() {
    let controller = PageFileAllocator::default();
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);
    let tx = controller.get_alloc_tx(1);
    assert!(tx.is_none());
}

#[test]
fn test_remove_page_file() {
    let controller = PageFileAllocator::default();

    let allocator = PageAllocator::new(InitState::Free);
    controller.insert_page_file(PageFileId(1), allocator);

    let tx = controller
        .get_alloc_tx(5)
        .expect("page allocation transaction should be produced");

    controller.remove_page_file(PageFileId(1));

    // Shouldn't cause any issues as the drop should ignore now removed files.
    drop(tx);

    let tx = controller.get_alloc_tx(1);
    assert!(tx.is_none());
}

#[test]
fn test_free() {
    let controller = PageFileAllocator::default();
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);

    controller.free(PageFileId(1), 0, NUM_PAGES_PER_BLOCK as u16);
    controller
        .get_alloc_tx(5)
        .expect("page allocation transaction should be produced");
}

#[should_panic(expected = "BUG: page file already exists in writer controller")]
#[test]
fn test_double_page_file_insert_panic() {
    let controller = PageFileAllocator::default();
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);
}

#[test]
fn test_num_pages_count() {
    let allocator = PageFileAllocator::default();
    assert_eq!(allocator.num_page_files(), 0);

    allocator.insert_page_file(PageFileId(1), PageAllocator::new(InitState::Free));
    assert_eq!(allocator.num_page_files(), 1);

    allocator.remove_page_file(PageFileId(1));
    assert_eq!(allocator.num_page_files(), 0);
}

#[test]
fn test_capacity_count() {
    let allocator = PageFileAllocator::default();
    assert_eq!(allocator.capacity(), 0);

    allocator.insert_page_file(PageFileId(1), PageAllocator::new(InitState::Free));
    assert_eq!(allocator.capacity(), MAX_NUM_PAGES);

    allocator.remove_page_file(PageFileId(1));
    assert_eq!(allocator.capacity(), 0);
}

#[test]
fn test_write_alloc_tx_debug_format() {
    let allocator = PageFileAllocator::default();
    allocator.insert_page_file(PageFileId(1), PageAllocator::new(InitState::Free));

    let mut txn = allocator.get_alloc_tx(4).unwrap();
    assert_eq!(format!("{txn:?}"), "WriteAllocTx(page_file_id=PageFileId(1), committed=false, num_spans=1)");

    txn.commit();
    assert_eq!(format!("{txn:?}"), "WriteAllocTx(page_file_id=PageFileId(1), committed=true, num_spans=1)");
}
