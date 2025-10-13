use std::cmp;
use std::collections::VecDeque;

use super::PageIndex;
use super::mem_block::{
    PageFreePermit,
    PageOrRetry,
    PrepareRevertibleEvictionError,
    TryFreeError,
    VirtualMemoryBlock,
};

/// Attempt to mark and cleanup any preventable evictions within the cache.
pub(super) fn cleanup_revertible_evictions(
    limit: usize,
    memory: &VirtualMemoryBlock,
    backlog: &mut EvictionBacklog,
) -> usize {
    // We cap the number of things we process in order to even the load on tasks.
    for _ in 0..backlog.pages_to_mark.len() {
        let Some(page) = backlog.pages_to_mark.pop_front() else {
            break;
        };
        match memory.try_mark_for_revertible_eviction(page) {
            Ok(permit) => {
                backlog.pages_to_free.push_back(permit);
            },
            Err(PrepareRevertibleEvictionError::PageLocked(retry)) => {
                backlog.pages_to_mark.push_back(PageOrRetry::Retry(retry));
            },
            Err(PrepareRevertibleEvictionError::AlreadyFree) => {},
            Err(PrepareRevertibleEvictionError::OperationStale) => {},
            // Currently not possible outside of tests.
            Err(PrepareRevertibleEvictionError::Dirty) => {},
        }
    }

    free_pages(limit, memory, &mut backlog.pages_to_free)
}

fn free_pages(
    limit: usize,
    memory: &VirtualMemoryBlock,
    permits: &mut VecDeque<PageFreePermit>,
) -> usize {
    let mut num_pages_freed = 0;
    let pop_n = cmp::min(permits.len(), limit);
    for _ in 0..pop_n {
        let Some(permit) = permits.pop_front() else {
            break;
        };

        match memory.try_free(&permit) {
            Ok(()) => {
                num_pages_freed += 1;
            },
            Err(TryFreeError::InUse) => {
                permits.push_back(permit);
                break;
            },
            Err(TryFreeError::Locked) => {
                permits.push_back(permit);
            },
            Err(TryFreeError::PermitExpired) => {},
            Err(TryFreeError::Io(error)) => {
                tracing::error!(error = %error, "cache failed to free page");
                permits.push_back(permit);
            },
        }
    }

    num_pages_freed
}

#[derive(Default)]
/// A set of page operations to apply at a later stage.
pub struct EvictionBacklog {
    pages_to_mark: VecDeque<PageOrRetry>,
    pages_to_free: VecDeque<PageFreePermit>,
}

impl EvictionBacklog {
    /// Add a new page to mark for eviction.
    pub fn mark(&mut self, page: PageIndex) {
        self.pages_to_mark.push_back(PageOrRetry::Page(page));
    }

    /// Indicates if the backlog is empty or not.
    ///
    /// This includes both pages waiting to be marked and pages
    /// to evict.
    pub fn is_empty(&self) -> bool {
        self.pages_to_mark.is_empty() && self.pages_to_free.is_empty()
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::cache::mem_block::{PageSize, PrepareWriteError};

    #[test]
    fn test_cleanup_skips_already_free_pages() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();

        let mut evictions = EvictionBacklog::default();
        evictions.mark(0);
        evictions.mark(2);
        evictions.mark(3);

        cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);

        assert!(evictions.pages_to_mark.is_empty());
        assert!(evictions.pages_to_free.is_empty());
        assert!(evictions.is_empty());
    }

    #[test]
    fn test_cleanup_frees_pages() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();

        let permit = memory.try_prepare_for_write(0).unwrap();
        memory.write_page(permit, &[1; 8 << 10]);
        let permit = memory.try_prepare_for_write(2).unwrap();
        memory.write_page(permit, &[1; 8 << 10]);

        let mut evictions = EvictionBacklog::default();
        evictions.mark(0);
        evictions.mark(2);
        evictions.mark(3);

        cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);

        assert!(evictions.pages_to_mark.is_empty());
        assert_eq!(evictions.pages_to_free.len(), 2);
        assert!(!evictions.is_empty());

        memory.advance_generation();

        let pages_freed =
            cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);
        assert!(evictions.pages_to_free.is_empty());
        assert!(evictions.is_empty());
        assert_eq!(pages_freed, 2);
    }

    #[test]
    fn test_marked_pages_respect_reverted_eviction() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();
        let permit = memory.try_prepare_for_write(0).unwrap();
        memory.write_page(permit, &[1; 8 << 10]);

        let mut evictions = EvictionBacklog::default();
        evictions.mark(0);

        let pages_freed =
            cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);
        assert_eq!(pages_freed, 0);

        memory.advance_generation();
        let err = memory
            .try_prepare_for_write(0)
            .expect_err("writer should revert eviction");
        assert!(matches!(err, PrepareWriteError::EvictionReverted));

        let pages_freed =
            cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);
        assert!(evictions.pages_to_free.is_empty());
        assert!(evictions.is_empty());
        assert_eq!(pages_freed, 0);
    }

    #[test]
    fn test_mark_pages_locked() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();
        let permit = memory.try_prepare_for_write(0).unwrap();

        let mut evictions = EvictionBacklog::default();
        evictions.mark(0);

        let pages_freed =
            cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);
        assert_eq!(pages_freed, 0);
        assert_eq!(evictions.pages_to_mark.len(), 1);
        assert!(evictions.pages_to_free.is_empty());
        drop(permit);

        let pages_freed =
            cleanup_revertible_evictions(usize::MAX, &memory, &mut evictions);
        assert_eq!(pages_freed, 0);
        assert!(evictions.pages_to_mark.is_empty());
        assert!(evictions.pages_to_free.is_empty());
    }

    #[test]
    fn test_free_pages_respects_queue_order() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();

        for page in 0..3 {
            let permit = memory.try_prepare_for_write(page).unwrap();
            memory.write_page(permit, &[1; 8 << 10]);
        }

        let permit1 = memory
            .try_mark_for_revertible_eviction(PageOrRetry::Page(1))
            .unwrap();
        memory.advance_generation();

        let read = memory.prepare_read(0..1).try_finish().unwrap();
        let permit2 = memory
            .try_mark_for_revertible_eviction(PageOrRetry::Page(2))
            .unwrap();

        // We put permit2 in front to check that the system breaks early, in the real world
        // this can't happen, but useful for testing.
        let mut backlog = VecDeque::new();
        backlog.push_back(permit2);
        backlog.push_back(permit1);

        let pages_freed = free_pages(usize::MAX, &memory, &mut backlog);
        assert_eq!(pages_freed, 0);
        drop(read);

        memory.advance_generation();
        let pages_freed = free_pages(usize::MAX, &memory, &mut backlog);
        assert_eq!(pages_freed, 2);
    }
}
