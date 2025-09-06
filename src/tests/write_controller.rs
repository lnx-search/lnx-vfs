use crate::disk_allocator::{AllocSpan, InitState, PageAllocator};
use crate::layout::PageFileId;
use crate::page_data::NUM_PAGES_PER_BLOCK;
use crate::write_controller::WriteController;

#[test]
fn test_empty_write_controller() {
    let controller = WriteController::default();
    let tx = controller.get_alloc_tx(5);
    assert!(tx.is_none());
}

#[should_panic(expected = "BUG: page file does not exist while trying to free pages")]
#[test]
fn test_panic_on_free_non_existent_file() {
    let controller = WriteController::default();
    controller.free(PageFileId(1), 44, 10);
}

#[test]
fn test_page_alloc_commited() {
    let controller = WriteController::default();
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
    let controller = WriteController::default();
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
    let controller = WriteController::default();
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);
    let tx = controller.get_alloc_tx(1);
    assert!(tx.is_none());
}

#[test]
fn test_remove_page_file() {
    let controller = WriteController::default();

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
    let controller = WriteController::default();
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
    let controller = WriteController::default();
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);
    let allocator = PageAllocator::new(InitState::Allocated);
    controller.insert_page_file(PageFileId(1), allocator);
}
