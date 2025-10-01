use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io, mem};

use parking_lot::{Mutex, RwLock};

use crate::checkpoint::{ReadCheckpointError, WriteCheckpointError};
use crate::directory::{FileGroup, FileId};
use crate::disk_allocator::InitState;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::{
    DISK_PAGE_SIZE,
    MAX_NUM_PAGES,
    NUM_BLOCKS_PER_FILE,
    NUM_PAGES_PER_BLOCK,
};
use crate::{ctx, disk_allocator, page_file_allocator};

type ConcurrentHashMap<K, V> = papaya::HashMap<K, V, foldhash::fast::RandomState>;

#[derive(Debug, thiserror::Error)]
/// An error preventing the [MetadataController] from opening
/// and recovering the previously persisted state.
pub enum OpenMetadataControllerError {
    #[error(transparent)]
    /// The controller could not recover page data from existing checkpoints.
    ReadCheckpoint(#[from] ReadCheckpointError),
    #[error(transparent)]
    /// The controller could not recovery metadata updates from replaying
    /// the log.
    WalRecovery(#[from] super::checkpoint::RecoverWalError),
}

/// The metadata controller handles the global page table state
/// and checkpointing the metadata tied to each page of data.
pub struct MetadataController {
    ctx: Arc<ctx::FileContext>,
    lookup_table: ConcurrentHashMap<PageGroupId, LookupEntry>,
    page_tables: ConcurrentHashMap<PageFileId, PageTable>,
    active_checkpoint_files: Mutex<BTreeMap<PageFileId, FileId>>,
    files_to_cleanup: Mutex<Vec<FileId>>,
}

impl MetadataController {
    /// Opens a new [MetadataController] and re-populates data using the page table
    /// checkpoints and replays any WAL file changes.
    pub async fn open(
        ctx: Arc<ctx::FileContext>,
    ) -> Result<Self, OpenMetadataControllerError> {
        let checkpointed_state =
            super::checkpoint::read_checkpoints(ctx.clone()).await?;

        tracing::info!(
            num_checkpointed_page_files = checkpointed_state.page_tables.len(),
            "checkpoints read",
        );

        let slf = Self::empty(ctx.clone());
        for (page_file_id, page_table) in checkpointed_state.page_tables {
            dbg!(page_file_id);
            slf.insert_page_table(page_file_id, page_table);
        }

        let lookup_table_guard = slf.lookup_table.pin();
        for (page_group_id, lookup_entry) in checkpointed_state.lookup_table {
            lookup_table_guard.insert(page_group_id, lookup_entry);
        }
        drop(lookup_table_guard);

        super::checkpoint::recover_wal_updates(ctx.clone(), &slf).await?;

        tracing::info!(
            num_page_groups = slf.num_page_groups(),
            "recovered state from WAL & checkpoints"
        );

        Ok(slf)
    }

    /// Create a new empty [MetadataController].
    pub fn empty(ctx: Arc<ctx::FileContext>) -> Self {
        Self {
            ctx,
            lookup_table: ConcurrentHashMap::with_hasher(
                foldhash::fast::RandomState::default(),
            ),
            page_tables: ConcurrentHashMap::with_hasher(
                foldhash::fast::RandomState::default(),
            ),
            active_checkpoint_files: Mutex::new(BTreeMap::new()),
            files_to_cleanup: Mutex::new(Vec::new()),
        }
    }

    /// Insert a new page table with some pre-existing page table.
    pub fn insert_page_table(&self, page_file_id: PageFileId, page_table: PageTable) {
        let tables = self.page_tables.pin();
        if tables.contains_key(&page_file_id) {
            panic!("BUG: page table already exists");
        }
        tables.insert(page_file_id, page_table);
    }

    /// Creates a new blank page table for the given page file.
    pub fn create_blank_page_table(&self, page_file_id: PageFileId) {
        self.insert_page_table(page_file_id, PageTable::default())
    }

    /// Returns whether a given page file & its page table
    /// exists within the controller.
    pub fn contains_page_table(&self, page_file_id: PageFileId) -> bool {
        let tables = self.page_tables.pin();
        tables.contains_key(&page_file_id)
    }

    /// Returns whether a given page group exists.
    pub fn contains_page_group(&self, page_group_id: PageGroupId) -> bool {
        let lookup = self.lookup_table.pin();
        lookup.contains_key(&page_group_id)
    }

    /// Crates a [page_file_allocator::PageFileAllocator] for each existing page file
    /// and sets the allocator to represent its current state.
    ///
    /// This does _not_ update the allocators after they have been created.
    pub fn create_page_file_allocator(&self) -> page_file_allocator::PageFileAllocator {
        let allocator = page_file_allocator::PageFileAllocator::default();

        let tables = self.page_tables.pin();
        for (page_file_id, page_table) in tables.iter() {
            let page_allocator = page_table.create_allocator();
            allocator.insert_page_file(*page_file_id, page_allocator);
        }

        allocator
    }

    #[inline]
    /// Returns the number of page groups in the system.
    pub fn num_page_groups(&self) -> usize {
        self.lookup_table.len()
    }

    /// Find the first page of a page group and return the page table
    /// it is stored at and the page ID of the first page.
    ///
    /// Returns `None` if the group does not exist.
    fn find_first_page(&self, group: PageGroupId) -> Option<LookupEntry> {
        self.lookup_table.pin().get(&group).copied()
    }

    /// Remove the group lookup entry, returning the existing entry if
    /// applicable.
    ///
    /// Returns `None` if the group does not exist.
    fn remove_group_lookup(&self, group: PageGroupId) -> Option<LookupEntry> {
        self.lookup_table.pin().remove(&group).copied()
    }

    /// Insert the given page group into the controller and associate it with
    /// the given page table (via the page file ID) and the first page containing
    /// the start of the group data.
    ///
    /// WARNING: This assumes the given page file ID exists.
    fn insert_page_group(&self, group: PageGroupId, entry: LookupEntry) {
        debug_assert!(
            self.contains_page_table(entry.page_file_id),
            "BUG: page table does not exist for page file: {:?}",
            entry.page_file_id
        );

        let lookup = self.lookup_table.pin();
        if let Some(old_lookup) = lookup.insert(group, entry) {
            let tables = self.page_tables.pin();
            match tables.get(&old_lookup.page_file_id) {
                None => {},
                Some(page_table) => {
                    page_table.unassign_pages_in_chain(old_lookup.first_page_id)
                },
            };
        }
    }

    /// Collects the pages of a given page group.
    ///
    /// The `data_range` is the range of bytes that need to be read from the group,
    /// the controller put all pages required to complete the read in the `results`
    /// output vector.
    ///
    /// Returns `true` if the page group exists, otherwise `false` is returned.
    pub fn collect_pages(
        &self,
        page_group_id: PageGroupId,
        data_range: Range<usize>,
        results: &mut Vec<PageMetadata>,
    ) -> bool {
        let lookup = match self.find_first_page(page_group_id) {
            None => return false,
            Some(lookup) => lookup,
        };

        let tables = self.page_tables.pin();
        let page_table = tables
            .get(&lookup.page_file_id)
            .expect("BUG: page file ID should exist");

        page_table.collect_pages(lookup.first_page_id, data_range, results);

        true
    }

    /// Assign a set of pages to a target [PageGroupId] in a given page file.
    ///
    /// If a group already has assigned pages, they will be unassigned.
    pub fn assign_pages_to_group(
        &self,
        page_file_id: PageFileId,
        group: PageGroupId,
        pages: &[PageMetadata],
    ) {
        // Debug assert because this should never get called in normal production use
        // with the page IDs being out of order, just because other things would break before.
        debug_assert!(
            pages.is_sorted_by_key(|p| p.id),
            "BUG: page metadata entries must be sorted by ID"
        );

        let first_page_id = pages.first().map(|p| p.id).unwrap_or(PageId::TERMINATOR);

        let tables = self.page_tables.pin();

        let new_page_table = tables
            .get(&page_file_id)
            .expect("BUG: page file ID should exist");

        // Ordering is important here, if, for some reason there is a panic, we need to
        // make sure the memory state stays in a semi-valid configuration (even if it means
        // we leak some disk space.)
        if let Some(existing_lookup) = self.find_first_page(group) {
            let old_page_table = tables
                .get(&existing_lookup.page_file_id)
                .expect("BUG: page file ID should exist");

            new_page_table.write_pages(pages);
            self.insert_page_group(
                group,
                LookupEntry {
                    page_file_id,
                    first_page_id,
                },
            );
            old_page_table.unassign_pages_in_chain(existing_lookup.first_page_id);
        } else {
            new_page_table.write_pages(pages);
            self.insert_page_group(
                group,
                LookupEntry {
                    page_file_id,
                    first_page_id,
                },
            );
        }
    }

    /// Unassign any pages assigned to the target [PageGroupId].
    pub fn unassign_pages_in_group(&self, group: PageGroupId) {
        let lookup = match self.remove_group_lookup(group) {
            None => return,
            Some(lookup) => lookup,
        };

        let tables = self.page_tables.pin();
        let page_table = tables
            .get(&lookup.page_file_id)
            .expect("BUG: page file ID should exist");
        page_table.unassign_pages_in_chain(lookup.first_page_id);
    }

    /// Reassign any pages that are currently linked to the `old_group`
    /// to a `new_group`.
    ///
    /// Returns `false` if the `old_group` does not exist.
    pub fn reassign_pages(
        &self,
        old_group: PageGroupId,
        new_group: PageGroupId,
    ) -> bool {
        let lookup_table = self.lookup_table.pin();

        let lookup = match lookup_table.get(&old_group).copied() {
            None => return false,
            Some(lookup) => lookup,
        };

        let tables = self.page_tables.pin();
        let page_table = tables
            .get(&lookup.page_file_id)
            .expect("BUG: page file ID should exist");
        page_table.reassign_pages_in_chain(lookup.first_page_id, new_group);

        lookup_table.insert(new_group, lookup);
        lookup_table.remove(&old_group);

        true
    }

    #[tracing::instrument(skip(self))]
    /// Checkpoint the current memory state.
    ///
    /// Any page table that has changed since the last checkpoint and creates a new checkpoint file.
    ///
    /// This operation is technically incremental, if a page table has not changed from the last
    /// checkpoint then a new checkpoint file is not created.
    pub async fn checkpoint(&self) -> Result<usize, WriteCheckpointError> {
        let mut num_checkpointed_files = 0;
        for (page_file_id, page_table) in self.page_tables.pin().iter() {
            if !page_table.has_changed() {
                continue;
            }

            let file_id = super::checkpoint::checkpoint_page_table(
                self.ctx.clone(),
                *page_file_id,
                page_table,
            )
            .await?;
            num_checkpointed_files += 1;

            let maybe_old_file_id = {
                let mut checkpoints = self.active_checkpoint_files.lock();
                checkpoints.insert(*page_file_id, file_id)
            };

            if let Some(old_file_id) = maybe_old_file_id {
                self.files_to_cleanup.lock().push(old_file_id);
            }
        }

        self.garbage_collect_checkpoints().await?;

        Ok(num_checkpointed_files)
    }

    /// Returns the number of files to be cleaned up.
    pub fn num_files_to_cleanup(&self) -> usize {
        self.files_to_cleanup.lock().len()
    }

    #[tracing::instrument(skip(self))]
    /// Cleanup any checkpoint files that are not outdated and not needed.
    pub async fn garbage_collect_checkpoints(&self) -> io::Result<()> {
        tracing::info!("cleaning up unused checkpoint files");

        #[cfg(test)]
        fail::fail_point!("metadata::garbage_collect_checkpoints", |_| Err(
            io::Error::other("garbage_collect_checkpoints fail point error")
        ));

        while let Some(file_id) = { self.files_to_cleanup.lock().pop() } {
            let result = self
                .ctx
                .directory()
                .remove_file(FileGroup::Metadata, file_id)
                .await;

            if result.is_err() {
                self.files_to_cleanup.lock().push(file_id);
            }
            result?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
/// A lookup entry contains the location of the first page that a page group
/// contains and the revision of the page group.
pub struct LookupEntry {
    pub page_file_id: PageFileId,
    pub first_page_id: PageId,
}

struct PageBlock {
    metadata: Box<[PageMetadata; NUM_PAGES_PER_BLOCK]>,
    num_allocated_pages: usize,
}

impl Default for PageBlock {
    fn default() -> Self {
        let mut uninit_pages = Box::new_uninit_slice(NUM_PAGES_PER_BLOCK);
        for page_id in 0..NUM_PAGES_PER_BLOCK {
            uninit_pages[page_id].write(PageMetadata::null());
        }

        // SAFETY: We have initialised each element in the array and the size of the array
        //         is aligned with the size of the array we're casting to.
        let pages = unsafe {
            let raw = Box::into_raw(uninit_pages);
            Box::from_raw(raw as *mut [PageMetadata; NUM_PAGES_PER_BLOCK])
        };

        Self {
            metadata: pages,
            num_allocated_pages: 0,
        }
    }
}

impl PageBlock {
    #[inline]
    fn write_page(&mut self, index: usize, metadata: PageMetadata) {
        self.mutate_page(
            index,
            #[inline(always)]
            |page| *page = metadata,
        );
    }

    fn mutate_page<F>(&mut self, index: usize, mut mutator: F)
    where
        F: FnMut(&mut PageMetadata),
    {
        let old_page_unassigned = self.metadata[index].is_unassigned();
        mutator(&mut self.metadata[index]);
        let new_page_unassigned = self.metadata[index].is_unassigned();

        if old_page_unassigned && !new_page_unassigned {
            self.num_allocated_pages += 1;
        } else if !old_page_unassigned && new_page_unassigned {
            self.num_allocated_pages -= 1;
        }
    }
}

/// A [PageTable] holds the individual [PageMetadata] entries for each page file,
/// tying the data stored in the data file with its metadata.
pub struct PageTable {
    /// A set of shards containing the page metadata.
    ///
    /// This is done in order to reduce lock contention under load.
    page_shards: [RwLock<PageBlock>; NUM_BLOCKS_PER_FILE],
    change_op_stamp: AtomicU64,
    last_checkpoint: AtomicU64,
}

impl Default for PageTable {
    fn default() -> Self {
        let mut uninit_shards: [MaybeUninit<RwLock<PageBlock>>; NUM_BLOCKS_PER_FILE] =
            [const { MaybeUninit::uninit() }; NUM_BLOCKS_PER_FILE];

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_BLOCKS_PER_FILE {
            uninit_shards[idx].write(RwLock::new(PageBlock::default()));
        }

        let init_shards = unsafe {
            mem::transmute::<
                [MaybeUninit<RwLock<PageBlock>>; NUM_BLOCKS_PER_FILE],
                [RwLock<PageBlock>; NUM_BLOCKS_PER_FILE],
            >(uninit_shards)
        };

        Self {
            page_shards: init_shards,
            change_op_stamp: AtomicU64::new(0),
            last_checkpoint: AtomicU64::new(0),
        }
    }
}

impl PageTable {
    /// Create a new [PageTable] using some existing set of pages.
    pub fn from_existing_state(pages: &[PageMetadata]) -> Self {
        let table = Self::default();
        table.write_pages(pages);
        table.checkpoint(table.get_current_op_stamp());
        table
    }

    /// Returns the number of assigned pages in the page table.
    pub(super) fn num_assigned_pages(&self) -> usize {
        let mut total = 0;
        for shard in &self.page_shards {
            let block = shard.read();
            total += block.num_allocated_pages;
        }
        total
    }

    /// Creates a new [disk_allocator::PageAllocator] configured to the current
    /// state of the page table.
    pub(super) fn create_allocator(&self) -> disk_allocator::PageAllocator {
        if self.num_assigned_pages() == 0 {
            return disk_allocator::PageAllocator::new(InitState::Free);
        }

        let mut allocator = disk_allocator::PageAllocator::new(InitState::Allocated);
        for (block_id, block) in self.page_shards.iter().enumerate() {
            let block = block.read();
            let page_start = (block_id * NUM_PAGES_PER_BLOCK) as u32;

            if block.num_allocated_pages == 0 {
                allocator.free(page_start, NUM_PAGES_PER_BLOCK as u16);
                continue;
            }

            // Tried to do some optimised complex thing, but it was harder to read and the
            // compiler just absolutely kicked my arse.
            for (offset, page) in block.metadata.iter().enumerate() {
                if page.is_unassigned() {
                    allocator.free(page_start + offset as u32, 1);
                }
            }
        }

        allocator
    }

    pub(super) fn unassign_pages_in_chain(&self, first_page_id: PageId) {
        self.mutate_pages_in_chain(
            first_page_id,
            #[inline(always)]
            |page| *page = PageMetadata::unassigned(page.id),
        )
    }

    pub(super) fn reassign_pages_in_chain(
        &self,
        first_page_id: PageId,
        new_group_id: PageGroupId,
    ) {
        self.mutate_pages_in_chain(
            first_page_id,
            #[inline(always)]
            |page| page.group = new_group_id,
        )
    }

    fn mutate_pages_in_chain<F>(&self, first_page_id: PageId, mut mutator: F)
    where
        F: FnMut(&mut PageMetadata),
    {
        if first_page_id.is_terminator() {
            return;
        }

        let mut current_page_id = first_page_id;
        let mut block_idx = (current_page_id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let mut page_idx = (current_page_id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        let mut block_shard = self.page_shards[block_idx].write();
        loop {
            let page_metadata = &block_shard.metadata[page_idx];
            let next_page_id = page_metadata.next_page_id;

            block_shard.mutate_page(page_idx, &mut mutator);

            if next_page_id.is_terminator() {
                break;
            }

            current_page_id = next_page_id;

            let old_block_idx = block_idx;
            block_idx = (current_page_id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
            page_idx = (current_page_id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

            if old_block_idx != block_idx {
                block_shard = self.page_shards[block_idx].write();
            }
        }
        drop(block_shard);
    }

    pub(super) fn write_pages(&self, to_update: &[PageMetadata]) {
        let first_page = match to_update.first() {
            None => return,
            Some(first_page) => first_page,
        };
        assert!(!first_page.is_null(), "BUG: provided page cannot be null");

        let mut block_idx = (first_page.id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let mut page_idx = (first_page.id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        let mut block_shard = self.page_shards[block_idx].write();
        block_shard.write_page(page_idx, *first_page);

        for metadata in to_update.iter().skip(1) {
            assert!(!first_page.is_null(), "BUG: provided page cannot be null");

            let old_block_idx = block_idx;
            block_idx = (metadata.id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
            page_idx = (metadata.id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

            if old_block_idx != block_idx {
                block_shard = self.page_shards[block_idx].write();
            }

            block_shard.write_page(page_idx, *metadata);
        }

        self.change_op_stamp.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn collect_pages(
        &self,
        start_page: PageId,
        data_range: Range<usize>,
        results: &mut Vec<PageMetadata>,
    ) {
        if start_page.is_terminator() {
            return;
        }

        assert!(
            start_page.0 < MAX_NUM_PAGES as u32,
            "page ID is beyond the bounds of the page table"
        );

        let n_pages_start = data_range.start / DISK_PAGE_SIZE;
        let n_pages_end = data_range.end.div_ceil(DISK_PAGE_SIZE);

        let n_pages_skip = n_pages_start;
        let n_pages_take = n_pages_end - n_pages_start;

        let mut block_idx = (start_page.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
        let mut page_idx = (start_page.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

        // We do a little bit of weirdness here, particularly to avoid requesting a lock
        // on every block in the page table if we already have it.
        // The high level view is we're just getting each page in the page group
        // until we've selected the range of pages we need.

        let mut num_pages_taken = 0;
        let mut num_pages_skipped = 0;
        let mut block_shard = self.page_shards[block_idx].read();
        while num_pages_taken < n_pages_take {
            let page_metadata = &block_shard.metadata[page_idx];
            if num_pages_skipped < n_pages_skip {
                num_pages_skipped += 1;
            } else {
                assert!(
                    !page_metadata.is_unassigned(),
                    "BUG: page being referenced is unassigned"
                );
                results.push(*page_metadata);
                num_pages_taken += 1;
            }

            let next_page_id = page_metadata.next_page_id;
            if next_page_id.is_terminator() {
                break;
            }

            let old_block_idx = block_idx;
            block_idx = (next_page_id.0 / NUM_PAGES_PER_BLOCK as u32) as usize;
            page_idx = (next_page_id.0 % NUM_PAGES_PER_BLOCK as u32) as usize;

            if old_block_idx != block_idx {
                block_shard = self.page_shards[block_idx].read();
            }
        }
        drop(block_shard);
    }

    /// Fills the vector with any pages that are not unassigned within the page table.
    pub(super) fn collect_assigned_pages(&self, pages: &mut Vec<PageMetadata>) {
        for shard in self.page_shards.iter() {
            for metadata in shard.read().metadata.iter() {
                if !metadata.is_unassigned() {
                    pages.push(*metadata);
                }
            }
        }
    }

    /// Returns if the page table has been modified since the last checkpoint.
    pub(super) fn has_changed(&self) -> bool {
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
    fn test_page_table_from_existing_state() {
        let table = PageTable::from_existing_state(&[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                id: PageId(4),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                id: PageId((NUM_PAGES_PER_BLOCK + 4) as u32),
                data_len: 0,
                context: [0; 40],
            },
        ]);
        assert!(!table.has_changed());

        let pages = table.page_shards[0].read();
        assert_eq!(pages.metadata[4].id, PageId(4));
        let pages = table.page_shards[1].read();
        assert_eq!(
            pages.metadata[4].id,
            PageId((NUM_PAGES_PER_BLOCK + 4) as u32)
        );
    }

    #[test]
    fn test_page_table_set_page() {
        let table = PageTable::default();

        {
            let pages = table.page_shards[0].read();
            assert!(pages.metadata[4].is_unassigned());
        }

        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };

        table.write_pages(&[metadata]);

        let pages = table.page_shards[0].read();
        assert_eq!(pages.metadata[4].id, PageId(4));
    }

    #[test]
    fn test_page_table_same_lock_update() {
        let table = PageTable::default();

        {
            let pages = table.page_shards[0].read();
            assert!(pages.metadata[5].is_unassigned());
            assert!(pages.metadata[6].is_unassigned());
        }

        // Test looping of metadata updates within same lock.
        let metadata1 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        };
        let metadata2 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(2),
            id: PageId(6),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata1, metadata2]);

        let pages = table.page_shards[0].read();
        assert_eq!(pages.metadata[5].id, PageId(5));
        assert_eq!(pages.metadata[6].id, PageId(6));
    }

    #[test]
    fn test_page_table_diff_lock_update() {
        let table = PageTable::default();
        {
            let pages = table.page_shards[0].read();
            assert!(pages.metadata[5].is_unassigned());
        }

        {
            let pages = table.page_shards[1].read();
            assert!(pages.metadata[4].is_unassigned());
        }

        // Test looping of metadata updates within same lock.
        let metadata1 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(5),
            data_len: 0,
            context: [0; 40],
        };
        let metadata2 = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(2),
            id: PageId((NUM_PAGES_PER_BLOCK + 4) as u32),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata1, metadata2]);

        let pages = table.page_shards[0].read();
        assert_eq!(pages.metadata[5].id, PageId(5));

        let pages = table.page_shards[1].read();
        assert_eq!(
            pages.metadata[4].id,
            PageId((NUM_PAGES_PER_BLOCK + 4) as u32)
        );
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
        table.write_pages(&[metadata]);

        let mut pages = Vec::new();
        table.collect_assigned_pages(&mut pages);
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
        table.write_pages(&[metadata]);

        assert!(table.has_changed());

        let op_stamp = table.get_current_op_stamp();
        assert_eq!(op_stamp, 1);

        table.checkpoint(1);
        assert!(!table.has_changed());
    }

    #[test]
    fn test_page_table_set_empty_page() {
        let table = PageTable::default();

        let metadata = PageMetadata {
            group: PageGroupId(1),
            revision: 0,
            next_page_id: PageId(1),
            id: PageId(4),
            data_len: 0,
            context: [0; 40],
        };
        table.write_pages(&[metadata]);

        let mut pages = Vec::new();
        table.collect_assigned_pages(&mut pages);
        assert_eq!(pages.len(), 1);

        let metadata = PageMetadata::unassigned(PageId(4));
        table.write_pages(&[metadata]);

        let mut pages = Vec::new();
        table.collect_assigned_pages(&mut pages);
        assert_eq!(pages.len(), 0);
    }

    #[should_panic(expected = "provided page cannot be null")]
    #[test]
    fn page_table_panics_on_null_page() {
        let table = PageTable::default();
        table.write_pages(&[PageMetadata::null()]);
    }

    #[rstest::rstest]
    #[case::write_empty_on_empty(
        PageMetadata::unassigned(PageId(4)),
        PageMetadata::unassigned(PageId(4)),
        0,
        0
    )]
    #[case::write_empty_on_full(
        PageMetadata { group: PageGroupId(0), revision: 0, next_page_id: PageId::TERMINATOR, id: PageId(4), data_len: 0, context: [0; 40]},
        PageMetadata::unassigned(PageId(4)),
        1,
        0,
    )]
    #[case::write_full_on_full(
        PageMetadata { group: PageGroupId(0), revision: 0, next_page_id: PageId::TERMINATOR, id: PageId(4), data_len: 0, context: [0; 40]},
        PageMetadata { group: PageGroupId(0), revision: 2, next_page_id: PageId::TERMINATOR, id: PageId(4), data_len: 0, context: [0; 40]},
        1,
        1,
    )]
    #[case::write_full_on_empty(
        PageMetadata::unassigned(PageId(4)),
        PageMetadata { group: PageGroupId(0), revision: 2, next_page_id: PageId::TERMINATOR, id: PageId(4), data_len: 0, context: [0; 40]},
        0,
        1,
    )]
    fn test_metadata_allocation_count(
        #[case] base_page: PageMetadata,
        #[case] write_page: PageMetadata,
        #[case] initial_count: usize,
        #[case] expected_count: usize,
    ) {
        let table = PageTable::from_existing_state(&[base_page]);
        assert_eq!(table.num_assigned_pages(), initial_count);

        table.write_pages(&[write_page]);
        assert_eq!(table.num_assigned_pages(), expected_count);
    }

    #[test]
    fn test_unassign_pages_in_chain() {
        let pid1 = PageId(4);
        let pid2 = PageId((NUM_PAGES_PER_BLOCK + 4) as u32);
        let pid3 = PageId((NUM_PAGES_PER_BLOCK * 2 + 50) as u32);

        let table = PageTable::from_existing_state(&[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: pid2,
                id: pid1,
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: pid3,
                id: pid2,
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: pid3,
                data_len: 0,
                context: [0; 40],
            },
        ]);

        {
            let pages = table.page_shards[0].read();
            assert!(!pages.metadata[4].is_unassigned());
            let pages = table.page_shards[1].read();
            assert!(!pages.metadata[4].is_unassigned());
            let pages = table.page_shards[2].read();
            assert!(!pages.metadata[50].is_unassigned());
        }

        table.unassign_pages_in_chain(pid1);

        let pages = table.page_shards[0].read();
        assert!(pages.metadata[4].is_unassigned());
        let pages = table.page_shards[1].read();
        assert!(pages.metadata[4].is_unassigned());
        let pages = table.page_shards[2].read();
        assert!(pages.metadata[50].is_unassigned());
    }

    #[test]
    fn test_unassign_pages_in_chain_undefined_behaviour_with_missing_terminator() {
        let pid1 = PageId(4);
        let pid2 = PageId((NUM_PAGES_PER_BLOCK + 4) as u32);
        let pid3 = PageId((NUM_PAGES_PER_BLOCK * 2 + 50) as u32);

        let table = PageTable::from_existing_state(&[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: pid2,
                id: pid1,
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: pid3,
                id: pid2,
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(2),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: pid3,
                data_len: 0,
                context: [0; 40],
            },
        ]);

        // The difference here is the final page for group `1` is pointing to a page
        // in group `2` which means the system will unassign a page it shouldn't
        // This is a sanity check to ensure this behaviour doesn't change without
        // us knowing about it.
        {
            let pages = table.page_shards[0].read();
            assert!(!pages.metadata[4].is_unassigned());
            let pages = table.page_shards[1].read();
            assert!(!pages.metadata[4].is_unassigned());
            let pages = table.page_shards[2].read();
            assert!(!pages.metadata[50].is_unassigned());
        }

        table.unassign_pages_in_chain(pid1);

        let pages = table.page_shards[0].read();
        assert!(pages.metadata[4].is_unassigned());
        let pages = table.page_shards[1].read();
        assert!(pages.metadata[4].is_unassigned());
        let pages = table.page_shards[2].read();
        assert!(pages.metadata[50].is_unassigned());
    }

    #[test]
    fn test_page_table_create_allocator() {
        let table = PageTable::from_existing_state(&[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: PageId(1),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(2),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: PageId(2),
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(3),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: PageId(3),
                data_len: 0,
                context: [0; 40],
            },
        ]);

        let mut allocator = table.create_allocator();
        assert_eq!(
            allocator.spare_capacity() as usize,
            NUM_PAGES_PER_BLOCK * 29
        ); // 29 blocks free, one marked full/sealed.

        allocator.free(1, 3);
        assert_eq!(
            allocator.spare_capacity() as usize,
            NUM_PAGES_PER_BLOCK * 30
        ); // 30 blocks free
    }

    #[test]
    fn test_page_table_create_allocator_sparse() {
        let pid1 = PageId(4);
        let pid2 = PageId((NUM_PAGES_PER_BLOCK + 4) as u32);
        let pid3 = PageId((NUM_PAGES_PER_BLOCK * 13 + 50) as u32);

        let table = PageTable::from_existing_state(&[
            PageMetadata {
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: pid1,
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(2),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: pid2,
                data_len: 0,
                context: [0; 40],
            },
            PageMetadata {
                group: PageGroupId(3),
                revision: 0,
                next_page_id: PageId::TERMINATOR,
                id: pid3,
                data_len: 0,
                context: [0; 40],
            },
        ]);

        let mut allocator = table.create_allocator();
        assert_eq!(
            allocator.spare_capacity() as usize,
            NUM_PAGES_PER_BLOCK * 27
        );

        allocator.free(pid1.0, 1);
        assert_eq!(
            allocator.spare_capacity() as usize,
            NUM_PAGES_PER_BLOCK * 28
        );
        allocator.free(pid2.0, 1);
        assert_eq!(
            allocator.spare_capacity() as usize,
            NUM_PAGES_PER_BLOCK * 29
        );
        allocator.free(pid3.0, 1);
        assert_eq!(
            allocator.spare_capacity() as usize,
            NUM_PAGES_PER_BLOCK * 30
        );
    }
}
