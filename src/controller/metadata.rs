use std::mem;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use super::checkpoint::checkpoint_page_table;
use crate::checkpoint::WriteCheckpointError;
use crate::ctx;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::{MAX_NUM_PAGES, NUM_BLOCKS_PER_FILE, NUM_PAGES_PER_BLOCK};

/// The metadata controller handles the global page table state
/// and checkpointing the metadata tied to each page of data.
pub struct MetadataController {
    ctx: Arc<ctx::FileContext>,
    lookup_table: papaya::HashMap<PageGroupId, u32>,
    page_tables: papaya::HashMap<PageFileId, PageTable>,
}

impl MetadataController {
    /// Create a new empty [MetadataController].
    pub fn empty(ctx: Arc<ctx::FileContext>) -> Self {
        Self {
            ctx,
            lookup_table: papaya::HashMap::new(),
            page_tables: papaya::HashMap::new(),
        }
    }

    /// Checkpoint the current memory state.
    ///
    /// Any page table that has changed since the last checkpoint and creates a new checkpoint file.
    ///
    /// This operation is technically incremental, if a page table has not changed from the last
    /// checkpoint then a new checkpoint file is not created.
    pub async fn checkpoint(&self) -> Result<(), WriteCheckpointError> {
        for (page_file_id, page_table) in self.page_tables.pin().iter() {
            checkpoint_page_table(self.ctx.clone(), *page_file_id, page_table).await?;
        }
        Ok(())
    }
}

type GuardedPages = RwLock<Box<[PageMetadata; NUM_PAGES_PER_BLOCK]>>;

/// A [PageTable] holds the individual [PageMetadata] entries for each page file,
/// tying the data stored in the data file with its metadata.
pub(super) struct PageTable {
    /// A set of shards containing the page metadata.
    ///
    /// This is done in order to reduce lock contention under load.
    page_shards: [GuardedPages; NUM_BLOCKS_PER_FILE],
    change_op_stamp: AtomicU64,
    last_checkpoint: AtomicU64,
}

impl Default for PageTable {
    fn default() -> Self {
        let mut uninit_shards: [MaybeUninit<GuardedPages>; NUM_BLOCKS_PER_FILE] = [const { MaybeUninit::uninit() }; NUM_BLOCKS_PER_FILE];

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_BLOCKS_PER_FILE {
            let mut uninit_pages = Box::new_uninit_slice(NUM_PAGES_PER_BLOCK);
            for page_id in 0..NUM_PAGES_PER_BLOCK {
                uninit_pages[page_id].write(PageMetadata::empty());
            }

            // SAFETY: We have initialised each element in the array and the size of the array
            //         is aligned with the size of the array we're casting to.
            let pages = unsafe {
                let raw = Box::into_raw(uninit_pages);
                Box::from_raw(raw as *mut [PageMetadata; NUM_PAGES_PER_BLOCK])
            };

            uninit_shards[idx].write(RwLock::new(pages));
        }

        let init_shards = unsafe { mem::transmute::<_, [GuardedPages; NUM_BLOCKS_PER_FILE]>(uninit_shards) };

        Self {
            page_shards: init_shards,
            change_op_stamp: AtomicU64::new(0),
            last_checkpoint: AtomicU64::new(0),
        }
    }
}

impl PageTable {
    /// Update a specific page in the page table.
    fn set_page(&self, page: PageMetadata) {
        assert!(
            page.id.0 < MAX_NUM_PAGES as u32,
            "page ID is beyond the bounds of the page table"
        );

        // TODO: Too strong?
        self.change_op_stamp.fetch_add(1, Ordering::SeqCst);

        let block_idx = (page.id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let page_idx = (page.id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        let mut shard = self.page_shards[block_idx].write();
        shard[page_idx] = page;
    }

    /// Get an existing page from the page table.
    ///
    /// Returns `None` if the page is empty/unset.
    fn get_page(&self, page_id: PageId) -> Option<PageMetadata> {
        assert!(
            page_id.0 < MAX_NUM_PAGES as u32,
            "page ID is beyond the bounds of the page table"
        );

        let block_idx = (page_id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let page_idx = (page_id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        let shard = self.page_shards[block_idx].read();
        let page_metadata = shard[page_idx];

        if page_metadata.is_empty() {
            None
        } else {
            Some(page_metadata)
        }
    }

    /// Fills the vector with any pages that are not empty within the page table.
    pub(super) fn collect_non_empty_pages(&self, pages: &mut Vec<PageMetadata>) {
        for shard in self.page_shards.iter() {
            for metadata in shard.read().iter() {
                if !metadata.is_empty() {
                    pages.push(*metadata);
                }
            }
        }
    }

    /// Returns if the page table has been modified since the last checkpoint.
    fn has_changed(&self) -> bool {
        // TODO: Too strong?
        let current_op_stamp = self.change_op_stamp.load(Ordering::Acquire);
        current_op_stamp > self.last_checkpoint.load(Ordering::Acquire)
    }

    /// Returns the current op stamp of the page table.
    pub(super) fn get_current_op_stamp(&self) -> u64 {
        self.change_op_stamp.load(Ordering::Acquire)
    }

    /// Updates the last checkpointed op stamp to the new value.
    pub(super) fn checkpoint(&self, op_stamp: u64) {
        self.last_checkpoint.store(op_stamp, Ordering::Release);
    }
}


#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[test]
    fn test_page_table_set_page() {
        let table = PageTable::default();
        assert!(table.get_page(PageId(4)).is_none());

        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };

        table.set_page(metadata);

        assert!(table.get_page(PageId(4)).is_some());
    }

    #[test]
    fn test_page_table_collect_non_empty_pages() {
        let table = PageTable::default();
        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };
        table.set_page(metadata);

        let mut pages = Vec::new();
        table.collect_non_empty_pages(&mut pages);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id.0, 4);
    }

    #[test]
    fn test_page_table_change_detection() {
        let table = PageTable::default();

        assert!(!table.has_changed());

        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };
        table.set_page(metadata);

        assert!(table.has_changed());

        let op_stamp = table.get_current_op_stamp();
        assert_eq!(op_stamp, 1);

        table.checkpoint(1);
        assert!(!table.has_changed());
    }

}