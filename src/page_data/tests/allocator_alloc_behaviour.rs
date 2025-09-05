use smallvec::{SmallVec, smallvec};

use crate::page_data::allocator::{AllocSpan, InitState, PageAllocator};
use crate::page_data::{NUM_BLOCKS_PER_FILE, NUM_PAGES_PER_BLOCK};

#[rstest::rstest]
#[case(InitState::Free, (NUM_PAGES_PER_BLOCK * NUM_BLOCKS_PER_FILE) as u32)]
#[case(InitState::Allocated, 0)]
fn test_spare_capacity(#[case] init_state: InitState, #[case] expected_capacity: u32) {
    let capacity = PageAllocator::new(init_state);
    assert_eq!(capacity.spare_capacity(), expected_capacity);
}

#[test]
fn test_free_adds_capacity_on_allocated_init_space() {
    let mut capacity = PageAllocator::new(InitState::Allocated);
    assert_eq!(capacity.spare_capacity(), 0);

    capacity.free(0, 5);

    // Only updated when block is fully free.
    assert_eq!(capacity.spare_capacity(), 0);

    capacity.free(5, (NUM_PAGES_PER_BLOCK - 5) as u32);
    assert_eq!(capacity.spare_capacity(), NUM_PAGES_PER_BLOCK as u32);

    capacity.free(NUM_PAGES_PER_BLOCK as u32, 6);
    assert_eq!(capacity.spare_capacity(), NUM_PAGES_PER_BLOCK as u32);
}

#[rstest::rstest]
#[case(u32::MAX, None)]
#[case(0, Some(SmallVec::new()))]
#[trace]
fn test_alloc_edge_cases(
    #[case] num_pages: u32,
    #[case] expected_alloc: Option<SmallVec<[AllocSpan; 8]>>,
) {
    let mut allocator = PageAllocator::new(InitState::Free);
    let alloc = allocator.alloc(num_pages);
    assert_eq!(alloc, expected_alloc);
}

#[rstest::rstest]
#[case(u32::MAX)]
#[case(0)]
#[case(5)]
#[trace]
fn test_alloc_out_of_capacity(#[case] num_pages: u32) {
    let mut allocator = PageAllocator::new(InitState::Allocated);
    let alloc = allocator.alloc(num_pages);
    assert_eq!(alloc, None);
}

#[rstest::rstest]
#[case::single_page(1, smallvec![AllocSpan { start_page: 0, span_len: 1 }])]
#[case::span_128kb(16, smallvec![AllocSpan { start_page: 0, span_len: 16 }])]
#[case::span_96kb(12, smallvec![AllocSpan { start_page: 0, span_len: 12 }])]
#[case::span_64kb(8, smallvec![AllocSpan { start_page: 0, span_len: 8 }])]
#[case::span_32kb(4, smallvec![AllocSpan { start_page: 0, span_len: 4 }])]
#[case::span_unaligned_13(13, smallvec![AllocSpan { start_page: 0, span_len: 13 }])]
#[case::span_unaligned_14(14, smallvec![AllocSpan { start_page: 0, span_len: 14 }])]
#[case::span_unaligned_15(15, smallvec![AllocSpan { start_page: 0, span_len: 15 }])]
#[case::span_unaligned_9(9, smallvec![AllocSpan { start_page: 0, span_len: 9 }])]
#[case::span_unaligned_10(10, smallvec![AllocSpan { start_page: 0, span_len: 10 }])]
#[case::span_unaligned_11(11, smallvec![AllocSpan { start_page: 0, span_len: 11 }])]
#[case::span_unaligned_5(5, smallvec![AllocSpan { start_page: 0, span_len: 5 }])]
#[case::span_unaligned_6(6, smallvec![AllocSpan { start_page: 0, span_len: 6 }])]
#[case::span_unaligned_7(7, smallvec![AllocSpan { start_page: 0, span_len: 7 }])]
#[case::span_unaligned_2(2, smallvec![AllocSpan { start_page: 0, span_len: 2 }])]
#[case::span_unaligned_3(3, smallvec![AllocSpan { start_page: 0, span_len: 3 }])]
#[case::large_alloc_single_block(
    1000,
    smallvec![
        AllocSpan { start_page: 0, span_len: 992 },
        AllocSpan { start_page: 992, span_len: 8 },
    ]
)]
#[case::large_alloc_multi_block(
    (NUM_PAGES_PER_BLOCK + 2100) as u32,
    smallvec![
        AllocSpan { start_page: 0, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: 16_384, span_len: 2096 },
        AllocSpan { start_page: 18_480, span_len: 4 },
    ]
)]
#[case::large_alloc_multi_whole_blocks(
    (NUM_PAGES_PER_BLOCK * 4) as u32,
    smallvec![
        AllocSpan { start_page: 0, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: NUM_PAGES_PER_BLOCK as u32, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: (NUM_PAGES_PER_BLOCK * 2) as u32, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: (NUM_PAGES_PER_BLOCK * 3) as u32, span_len: NUM_PAGES_PER_BLOCK as u16 },
    ]
)]
fn test_page_single_alloc(
    #[case] num_pages: u32,
    #[case] expected_alloc: SmallVec<[AllocSpan; 8]>,
) {
    let mut allocator = PageAllocator::new(InitState::Free);
    let alloc = allocator.alloc(num_pages);
    assert_eq!(alloc, Some(expected_alloc));
}

#[rstest::rstest]
#[case::single_page(1, smallvec![AllocSpan { start_page: 150_000, span_len: 1 }])]
#[case::span_128kb(16, smallvec![AllocSpan { start_page: 150_000, span_len: 16 }])]
#[case::span_96kb(12, smallvec![AllocSpan { start_page: 150_000, span_len: 12 }])]
#[case::span_64kb(8, smallvec![AllocSpan { start_page: 150_000, span_len: 8 }])]
#[case::span_32kb(4, smallvec![AllocSpan { start_page: 150_000, span_len: 4 }])]
#[case::span_unaligned_13(13, smallvec![AllocSpan { start_page: 150_000, span_len: 13 }])]
#[case::span_unaligned_14(14, smallvec![AllocSpan { start_page: 150_000, span_len: 14 }])]
#[case::span_unaligned_15(15, smallvec![AllocSpan { start_page: 150_000, span_len: 15 }])]
#[case::span_unaligned_9(9, smallvec![AllocSpan { start_page: 150_000, span_len: 9 }])]
#[case::span_unaligned_10(10, smallvec![AllocSpan { start_page: 150_000, span_len: 10 }])]
#[case::span_unaligned_11(11, smallvec![AllocSpan { start_page: 150_000, span_len: 11 }])]
#[case::span_unaligned_5(5, smallvec![AllocSpan { start_page: 150_000, span_len: 5 }])]
#[case::span_unaligned_6(6, smallvec![AllocSpan { start_page: 150_000, span_len: 6 }])]
#[case::span_unaligned_7(7, smallvec![AllocSpan { start_page: 150_000, span_len: 7 }])]
#[case::span_unaligned_2(2, smallvec![AllocSpan { start_page: 150_000, span_len: 2 }])]
#[case::span_unaligned_3(3, smallvec![AllocSpan { start_page: 150_000, span_len: 3 }])]
#[case::large_alloc_single_block(
    1000,
    smallvec![
        AllocSpan { start_page: 150_000, span_len: 992 },
        AllocSpan { start_page: 150_992, span_len: 8 },
    ]
)]
#[case::large_alloc_multi_block(
    (NUM_PAGES_PER_BLOCK + 2100) as u32,
    smallvec![
        AllocSpan { start_page: 163_840, span_len: 16_384 },
        AllocSpan { start_page: 150_000, span_len: 2096 },
        AllocSpan { start_page: 152_096, span_len: 4 },
    ]
)]
#[case::large_alloc_multi_whole_blocks(
    (NUM_PAGES_PER_BLOCK * 4) as u32,
    smallvec![
        AllocSpan { start_page: 163_840, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: 180_224, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: 196_608, span_len: NUM_PAGES_PER_BLOCK as u16 },
        AllocSpan { start_page: 212_992, span_len: NUM_PAGES_PER_BLOCK as u16 },
    ]
)]
fn test_alloc_with_partial_filled_allocator(
    #[case] num_pages: u32,
    #[case] expected_alloc: SmallVec<[AllocSpan; 8]>,
) {
    let mut allocator = PageAllocator::new(InitState::Free);
    allocator.alloc(150_000);

    let alloc = allocator.alloc(num_pages);
    assert_eq!(alloc, Some(expected_alloc));
}

#[rstest::rstest]
#[case::span_unaligned_2(
    2,
    smallvec![AllocSpan { start_page: 16_382, span_len: 2 }],
)]
#[case::span_unaligned_3(
    3,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 1 },
    ],
)]
#[case::span_unaligned_5(
    5,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 1 },
    ],
)]
#[case::span_unaligned_6(
    6,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
    ],
)]
#[case::span_unaligned_7(
    7,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 1 },
    ],
)]
#[case::span_unaligned_9(
    9,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 2 },
        AllocSpan { start_page: 81_918, span_len: 1 },
    ],
)]
#[case::span_unaligned_10(
    10,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 2 },
        AllocSpan { start_page: 81_918, span_len: 2 },
    ],
)]
#[case::span_unaligned_11(
    11,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 2 },
        AllocSpan { start_page: 81_918, span_len: 2 },
        AllocSpan { start_page: 98_302, span_len: 1 },
    ],
)]
#[case::span_unaligned_13(
    13,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 2 },
        AllocSpan { start_page: 81_918, span_len: 2 },
        AllocSpan { start_page: 98_302, span_len: 2 },
        AllocSpan { start_page: 114_686, span_len: 1 },
    ],
)]
#[case::span_unaligned_14(
    14,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 2 },
        AllocSpan { start_page: 81_918, span_len: 2 },
        AllocSpan { start_page: 98_302, span_len: 2 },
        AllocSpan { start_page: 114_686, span_len: 2 },
    ],
)]
#[case::span_unaligned_15(
    15,
    smallvec![
        AllocSpan { start_page: 16_382, span_len: 2 },
        AllocSpan { start_page: 32_766, span_len: 2 },
        AllocSpan { start_page: 49_150, span_len: 2 },
        AllocSpan { start_page: 65_534, span_len: 2 },
        AllocSpan { start_page: 81_918, span_len: 2 },
        AllocSpan { start_page: 98_302, span_len: 2 },
        AllocSpan { start_page: 114_686, span_len: 2 },
        AllocSpan { start_page: 131_070, span_len: 1 },
    ],
)]
fn test_nearly_full_unaligned_page_chunking(
    #[case] num_pages: u32,
    #[case] expected_alloc: SmallVec<[AllocSpan; 8]>,
) {
    let mut allocator = PageAllocator::new(InitState::Free);
    for _ in 0..NUM_BLOCKS_PER_FILE {
        allocator.alloc((NUM_PAGES_PER_BLOCK - 2) as u32);
    }

    let alloc = allocator.alloc(num_pages);
    assert_eq!(alloc, Some(expected_alloc));
}

#[test]
fn test_infilling_aligned_pages_from_unaligned_page_requests() {
    let mut allocator = PageAllocator::new(InitState::Free);
    for _ in 0..NUM_BLOCKS_PER_FILE - 2 {
        allocator.alloc(NUM_PAGES_PER_BLOCK as u32);
    }
    allocator.alloc((NUM_PAGES_PER_BLOCK - 1) as u32);
    allocator.alloc((NUM_PAGES_PER_BLOCK - 12) as u32);

    let alloc = allocator.alloc(13);
    assert_eq!(
        alloc,
        Some(smallvec![
            AllocSpan {
                start_page: 491_508,
                span_len: 12
            },
            AllocSpan {
                start_page: 475_135,
                span_len: 1
            },
        ])
    );
}

#[test]
fn test_allocator_prevent_allocation_when_not_enough_pages_available() {
    let mut allocator = PageAllocator::new(InitState::Free);
    for _ in 0..NUM_BLOCKS_PER_FILE - 1 {
        allocator.alloc(NUM_PAGES_PER_BLOCK as u32);
    }
    allocator.alloc((NUM_PAGES_PER_BLOCK - 12) as u32);

    let alloc = allocator.alloc(13);
    assert_eq!(alloc, None);
}
