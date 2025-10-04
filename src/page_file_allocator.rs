use std::collections::BTreeMap;

use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;

use super::disk_allocator::{self, AllocSpan};
use crate::layout::PageFileId;

#[derive(Default)]
/// The page file allocator manages what pages should be written within the file
/// and internally manages the page allocator.
pub(super) struct PageFileAllocator {
    /// The set of page files currently available for writing.
    page_files: RwLock<BTreeMap<PageFileId, Mutex<disk_allocator::PageAllocator>>>,
}

impl PageFileAllocator {
    /// Insert a new page file into the page file allocator if it does not
    /// already exist.
    pub(super) fn insert_page_file(
        &self,
        id: PageFileId,
        allocator: disk_allocator::PageAllocator,
    ) {
        let mut lock = self.page_files.write();
        if lock.contains_key(&id) {
            return;
        }
        lock.insert(id, Mutex::new(allocator));
    }

    #[cfg(test)]
    pub(super) fn num_page_files(&self) -> usize {
        self.page_files.read().len()
    }

    /// Returns the amount of capacity the allocator has in pages across
    /// all page files.
    pub(super) fn capacity(&self) -> usize {
        let mut total = 0;
        let files = self.page_files.read();
        for allocator in files.values() {
            total += allocator.lock().spare_capacity() as usize;
        }
        total
    }

    /// Remove an existing page file from the page file allocator if it exists.
    pub(super) fn remove_page_file(&self, id: PageFileId) {
        let mut lock = self.page_files.write();
        debug_assert!(lock.contains_key(&id), "SOFT-BUG: page file does not exist");
        lock.remove(&id);
    }

    /// Get a new [WriteAllocTx] holding a new set of pages on disk reserved for the current
    /// operation.
    ///
    /// `None` is returned in the event there is no capacity for the number of pages needing
    /// to be written. A new page file should be created.
    pub(super) fn get_alloc_tx(&self, num_pages: u32) -> Option<WriteAllocTx<'_>> {
        // TODO: We might want to improve the heuristics of what files
        //       we prioritise writing to in order to reduce fragmentation.
        let lock = self.page_files.read();
        for (page_file_id, allocator) in lock.iter() {
            let mut lock = allocator.lock();
            let Some(spans) = lock.alloc(num_pages) else {
                continue;
            };
            return Some(WriteAllocTx {
                controller: self,
                page_file_id: *page_file_id,
                spans,
                is_commited: false,
            });
        }
        None
    }

    /// Free a set of pages in the target page file, starting from `start_page_id` and
    /// freeing `span_len` number of pages after it (including itself.)
    ///
    /// The page ranges must lay within the bounds of an allocation block.
    pub fn free(&self, page_file_id: PageFileId, start_page_id: u32, span_len: u16) {
        let lock = self.page_files.read();
        if let Some(allocator) = lock.get(&page_file_id) {
            let mut lock = allocator.lock();
            lock.free(start_page_id, span_len);
        } else {
            tracing::error!(id = ?page_file_id, "BUG: page file does not exist while trying to free pages");
            panic!("BUG: page file does not exist while trying to free pages");
        }
    }
}

/// The write alloc transaction transactionally commits the pages
/// that have been reserved for the current write for the allocator.
///
/// If the transaction is dropped prematurely the system will revert
/// the reservation preventing free page space being used by failed
/// write operations.
pub(super) struct WriteAllocTx<'controller> {
    controller: &'controller PageFileAllocator,
    page_file_id: PageFileId,
    spans: SmallVec<[AllocSpan; 8]>,
    is_commited: bool,
}

impl std::fmt::Debug for WriteAllocTx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WriteAllocTx(page_file_id={:?}, committed={}, num_spans={})",
            self.page_file_id,
            self.is_commited,
            self.spans.len(),
        )
    }
}

impl WriteAllocTx<'_> {
    #[inline]
    /// Returns the page file ID the allocation is assigned to.
    pub(super) fn page_file_id(&self) -> PageFileId {
        self.page_file_id
    }

    #[inline]
    /// Returns a reference to the spans the allocation is made up of.
    pub(super) fn spans(&self) -> &[AllocSpan] {
        &self.spans[..]
    }

    /// Commit the write allocation.
    ///
    /// The disk space is now permanently consumed until some other system
    /// marks the pages as free.
    pub(super) fn commit(&mut self) {
        self.is_commited = true;
    }
}

impl Drop for WriteAllocTx<'_> {
    fn drop(&mut self) {
        if !self.is_commited {
            tracing::warn!(
                "page write operation aborted, rolling back disk allocation reservation"
            );

            let state = self.controller.page_files.read();
            let page_file_state = match state.get(&self.page_file_id) {
                // If the page file is no longer in the writer state, then we ignore
                // this step as the allocator is removed.
                None => return,
                Some(page_file_state) => page_file_state,
            };

            let mut allocator = page_file_state.lock();
            for alloc in self.spans.iter() {
                allocator.free(alloc.start_page, alloc.span_len);
            }
        }
    }
}
