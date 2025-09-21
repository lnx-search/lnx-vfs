use std::cmp;

use smallvec::SmallVec;

use crate::page_data::{NUM_BLOCKS_PER_FILE, NUM_PAGES_PER_BLOCK};

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
/// An allocated span of pages.
pub struct AllocSpan {
    /// The starting page index.
    pub start_page: u32,
    /// The number of pages the allocation spans.
    pub span_len: u16,
}

#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The initial state to set all the blocks.
///
/// This is used to speedup startup.
pub enum InitState {
    /// All pages start as free.
    Free = 0,
    /// All pages start as allocated.
    Allocated = NUM_PAGES_PER_BLOCK as u16,
}

/// The [PageAllocator] determines how blocks of data are split up into pages
/// and laid out on the page file.
///
/// The design is currently quiet crude as I'm not exactly well-versed in allocator
/// or disk allocator design :)
pub(super) struct PageAllocator {
    /// The number of allocations present in each block of pages.
    block_allocations: [u16; NUM_BLOCKS_PER_FILE],
    /// The list of free blocks of page space.
    free_blocks: FreeBlockList,
}

impl PageAllocator {
    /// Create a new page allocator with [MAX_NUM_PAGES] number of pages and init state.
    pub(super) fn new(init_state: InitState) -> Self {
        let mut free_blocks = FreeBlockList::default();
        if init_state == InitState::Free {
            for idx in 0..NUM_BLOCKS_PER_FILE {
                let block = DiskBlock {
                    page_offset: (idx * NUM_PAGES_PER_BLOCK) as u32,
                    block_offset: 0,
                };
                free_blocks.push(block);
            }
        }

        Self {
            block_allocations: [init_state as u16; NUM_BLOCKS_PER_FILE],
            free_blocks,
        }
    }

    #[inline(always)]
    /// Free pages starting at `page_start` and continuing on for `len` pages.
    ///
    /// The page ranges must lay within the bounds of an allocation block.
    pub(super) fn free(&mut self, page_start: u32, len: u16) {
        let block_idx = page_start as usize / NUM_PAGES_PER_BLOCK;
        let page_end_block =
            (page_start + len as u32 - 1) as usize / NUM_PAGES_PER_BLOCK;

        assert_eq!(
            block_idx, page_end_block,
            "BUG: page range provided must stay within a single allocation block",
        );

        self.block_allocations[block_idx] -= len;
        if self.block_allocations[block_idx] == 0 {
            let block = DiskBlock {
                page_offset: (block_idx * NUM_PAGES_PER_BLOCK) as u32,
                block_offset: 0,
            };
            self.free_blocks.push(block);
        }
    }

    /// Returns the total number of spare pages within the allocator.
    pub(super) fn spare_capacity(&self) -> u32 {
        self.free_blocks.total_capacity
    }

    /// Allocate `num_pages` on the allocator.
    ///
    /// This will return an array of allocation spans if there was free space to hold
    /// the data, otherwise `None` is returned.
    pub(super) fn alloc(&mut self, num_pages: u32) -> Option<SmallVec<[AllocSpan; 8]>> {
        if self.spare_capacity() == 0 || self.spare_capacity() < num_pages {
            return None;
        } else if num_pages == 0 {
            return Some(SmallVec::new());
        }

        let mut allocated_pages = SmallVec::new();
        if num_pages <= 8 {
            self.small_alloc_best_fit(num_pages as u16, &mut allocated_pages);
        } else {
            self.large_alloc_best_fit(num_pages, &mut allocated_pages);
        }

        self.free_blocks.dec_free_pages(num_pages);
        self.free_blocks.prune();

        // Update total pages used per block.
        for span in allocated_pages.iter() {
            let block_idx = span.start_page as usize / NUM_PAGES_PER_BLOCK;
            self.block_allocations[block_idx] += span.span_len;
        }

        Some(allocated_pages)
    }

    /// A set of heuristics for allocations where the number of pages is less than
    /// or equal to a 128KB chunk.
    fn small_alloc_best_fit(
        &mut self,
        mut num_pages: u16,
        allocated_pages: &mut SmallVec<[AllocSpan; 8]>,
    ) {
        debug_assert!(num_pages <= 8);

        match num_pages {
            // if 256KB, 128KB, 64KB or 32KB, fit in one.
            8 | 6 | 4 | 2 | 1 => {
                let mut blocks = self.free_blocks.find_page_spans(num_pages);
                if let Some(block) = blocks.next() {
                    allocated_pages.push(block.alloc(num_pages));
                    return;
                }
            },
            // if the allocation size can be split into one of the aligned sizes and then some
            // remaining pages, we should do that.
            7 | 5 | 3 => {
                let num_aligned_pages = (num_pages / 2) * 2;
                self.small_alloc_best_fit(num_aligned_pages, allocated_pages);
                num_pages -= num_aligned_pages;
            },
            _ => {},
        }

        // everything else, use as filler.
        let mut blocks = self.free_blocks.find_page_spans(1);
        while num_pages > 0 {
            let Some(block) = blocks.next() else { break };
            let (span, n_left) = block.alloc_upto(num_pages);
            num_pages = n_left;
            allocated_pages.push(span);
        }

        // If this happens, it means we thought we had more spare capacity than we did.
        assert_eq!(
            num_pages, 0,
            "BUG: ran out of blocks despite remaining pages"
        );
    }

    /// A set of heuristics for allocations where the number of pages is larger than 128KB.
    fn large_alloc_best_fit(
        &mut self,
        mut num_pages: u32,
        allocated_pages: &mut SmallVec<[AllocSpan; 8]>,
    ) {
        // Attempt to consume entire blocks first.
        let mut blocks = self.free_blocks.blocks().iter_mut();
        'page_stepper: while num_pages >= NUM_PAGES_PER_BLOCK as u32 {
            for block in blocks.by_ref() {
                if block.spare_capacity() >= NUM_PAGES_PER_BLOCK as u16 {
                    let span = block.alloc(NUM_PAGES_PER_BLOCK as u16);
                    allocated_pages.push(span);
                    num_pages -= span.span_len as u32;
                    continue 'page_stepper;
                }
            }
            break;
        }

        if num_pages == 0 {
            return;
        }

        // For allocations where they are both large, and we cannot consume any more whole blocks,
        // we chunk into smaller sets of allocations.
        for block in self.free_blocks.find_page_spans(8) {
            let num_pages_to_alloc =
                cmp::min(num_pages, NUM_PAGES_PER_BLOCK as u32) as u16;
            let (span, n_left) = block.alloc_multiple_of(num_pages_to_alloc, 8);
            num_pages -= (num_pages_to_alloc - n_left) as u32;
            allocated_pages.push(span);

            if num_pages < 8 {
                break;
            }
        }

        while num_pages >= 8 {
            let page_chunk_size = cmp::min((num_pages / 2) * 2, 8);
            self.small_alloc_best_fit(page_chunk_size as u16, allocated_pages);
            num_pages -= page_chunk_size;
        }

        if num_pages > 0 {
            self.small_alloc_best_fit(num_pages as u16, allocated_pages);
        }
    }
}

/// The [FreeBlockList] is a stack of currently free blocks.
///
/// Due to the small number of blocks, the system allocates this
/// on the stack and uses a local cursor to track the currently
/// initialised blocks.
struct FreeBlockList {
    total_capacity: u32,
    blocks: [DiskBlock; NUM_BLOCKS_PER_FILE],
    len: usize,
}

impl Default for FreeBlockList {
    fn default() -> Self {
        Self {
            total_capacity: 0,
            blocks: [DiskBlock {
                page_offset: 0,
                block_offset: 0,
            }; NUM_BLOCKS_PER_FILE],
            len: 0,
        }
    }
}

impl FreeBlockList {
    /// Push a new [DiskBlock] onto the stack.
    fn push(&mut self, block: DiskBlock) {
        assert!(
            self.len <= NUM_BLOCKS_PER_FILE,
            "BUG: len has somehow become greater than number of blocks in file",
        );

        // If the block is already present, update in place.
        for existing_block in self.blocks.iter_mut().take(self.len) {
            if existing_block.page_offset == block.page_offset {
                existing_block.block_offset = block.block_offset;
                return;
            }
        }

        self.blocks[self.len] = block;
        self.len += 1;
        self.total_capacity += NUM_PAGES_PER_BLOCK as u32;
    }

    fn blocks(&mut self) -> &mut [DiskBlock] {
        &mut self.blocks[..self.len]
    }

    fn prune(&mut self) {
        let mut new_blocks = [DiskBlock {
            page_offset: 0,
            block_offset: 0,
        }; NUM_BLOCKS_PER_FILE];

        let mut len = 0;
        for block in self.blocks.iter().copied().take(self.len) {
            if block.spare_capacity() > 0 {
                new_blocks[len] = block;
                len += 1;
            }
        }

        self.blocks = new_blocks;
        self.len = len;
    }

    fn dec_free_pages(&mut self, pages: u32) {
        self.total_capacity -= pages;
    }

    fn find_page_spans(
        &mut self,
        num_pages: u16,
    ) -> impl Iterator<Item = &mut DiskBlock> + '_ {
        self.blocks
            .iter_mut()
            .take(self.len)
            .filter(move |block| block.spare_capacity() >= num_pages)
    }
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
/// A disk "block" is a collection of continuous pages.
///
/// This is used to manage allocation and freeing of pages on disk
/// incrementally without high fragmentation.
struct DiskBlock {
    /// The offset of the block in pages within the file.
    page_offset: u32,
    /// The offset within the block for allocations in pages.
    block_offset: u16,
}

impl DiskBlock {
    /// Returns the number of spare pages in the block
    fn spare_capacity(&self) -> u16 {
        NUM_PAGES_PER_BLOCK as u16 - self.block_offset
    }

    /// Increase the offset in the block by `num_pages` and produce a [AllocSpan]
    /// tracking the allocation.
    fn alloc(&mut self, num_pages: u16) -> AllocSpan {
        debug_assert!(
            self.spare_capacity() >= num_pages,
            "block has capacity {} but requested num pages is: {num_pages}",
            self.spare_capacity(),
        );
        let (span, _) = self.alloc_upto(num_pages);
        span
    }

    /// Allocate upto `num_pages` using the block or a number of pages
    /// that is a multiple of `multiple_of`.
    fn alloc_multiple_of(
        &mut self,
        num_pages: u16,
        multiple_of: u16,
    ) -> (AllocSpan, u16) {
        let start_page = self.page_offset + self.block_offset as u32;

        let aligned_num_pages = (num_pages / multiple_of) * multiple_of;
        let aligned_spare_capacity = (self.spare_capacity() / multiple_of) * multiple_of;

        let take_n = cmp::min(aligned_num_pages, aligned_spare_capacity);
        self.block_offset += take_n;

        let span = AllocSpan {
            start_page,
            span_len: take_n,
        };
        (span, num_pages - take_n)
    }

    /// Allocates upto `num_pages` on the block, returning the span
    /// and the remaining number of pages that could not be allocated.
    fn alloc_upto(&mut self, num_pages: u16) -> (AllocSpan, u16) {
        let start_page = self.page_offset + self.block_offset as u32;
        let take_n = cmp::min(num_pages, self.spare_capacity());
        self.block_offset += take_n;

        let span = AllocSpan {
            start_page,
            span_len: take_n,
        };
        (span, num_pages - take_n)
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[test]
    fn test_block_spare_capacity() {
        let mut block = DiskBlock {
            page_offset: 0,
            block_offset: 0,
        };

        let _alloc1 = block.alloc(5);
        assert_eq!(block.spare_capacity(), (NUM_PAGES_PER_BLOCK - 5) as u16);
        let _alloc2 = block.alloc(1);
        assert_eq!(block.spare_capacity(), (NUM_PAGES_PER_BLOCK - 6) as u16);
        let _alloc3 = block.alloc(999);
        assert_eq!(block.spare_capacity(), (NUM_PAGES_PER_BLOCK - 1005) as u16);
    }

    #[test]
    fn test_free_block_list() {
        let blocks = [
            DiskBlock {
                page_offset: 0,
                block_offset: 0,
            },
            DiskBlock {
                page_offset: 8,
                block_offset: 0,
            },
            DiskBlock {
                page_offset: 16,
                block_offset: 0,
            },
        ];

        let mut free_blocks = FreeBlockList::default();
        for block in blocks {
            free_blocks.push(block);
        }
        assert_eq!(free_blocks.blocks(), &blocks);

        let b = free_blocks.blocks();
        b[0].block_offset = NUM_PAGES_PER_BLOCK as u16;

        free_blocks.prune();
        assert_eq!(free_blocks.blocks(), &blocks[1..]);

        let b = free_blocks.blocks();
        b[1].block_offset = NUM_PAGES_PER_BLOCK as u16;

        free_blocks.prune();
        assert_eq!(free_blocks.blocks(), &blocks[1..2]);
    }

    #[test]
    fn test_free_block_list_push_dupe() {
        let mut free_blocks = FreeBlockList::default();

        free_blocks.push(DiskBlock {
            page_offset: 0,
            block_offset: 0,
        });
        assert_eq!(free_blocks.len, 1);

        free_blocks.push(DiskBlock {
            page_offset: 0,
            block_offset: 5,
        });
        assert_eq!(free_blocks.len, 1);
        assert_eq!(free_blocks.blocks[0].block_offset, 5);
    }
}
