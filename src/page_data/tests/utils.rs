use std::ops::Range;

use crate::layout::PageId;
use crate::layout::page_metadata::PageMetadata;
use crate::page_data::utils::{
    copy_sparse_metadata_context,
    make_metadata_density_mask,
    metadata_is_contiguous,
};

#[rstest::rstest]
#[case::single_page(
    &[make_empty_metadata(0)],
    0b0000_0001,
)]
#[case::dense_pages(
    &[
        make_empty_metadata(5),
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    0b1111_1111,
)]
#[case::sparse_pages1(
    &[
        make_empty_metadata(5),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    0b1101_1101,
)]
#[case::sparse_pages2(
    &[
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    0b0111_1111,
)]
#[case::sparse_pages3(
    &[
        make_empty_metadata(5),
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
    ],
    0b0111_1111,
)]
#[trace]
fn test_make_metadata_density_mask(
    #[case] metadata: &[PageMetadata],
    #[case] expected_mask: u8,
) {
    let actual_mask = make_metadata_density_mask(metadata);
    assert_eq!(format!("{actual_mask:0b}"), format!("{expected_mask:0b}"));
}

#[rstest::rstest]
#[case::single_page(
    &[make_empty_metadata(1)],
    &[
        (0, 1),
        (40, 0),
        (80, 0),
        (120, 0),
        (160, 0),
        (200, 0),
        (240, 0),
        (280, 0),
    ],
)]
#[case::dense_pages(
    &[
        make_empty_metadata(5),
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    &[
        (0, 5),
        (40, 6),
        (80, 7),
        (120, 8),
        (160, 9),
        (200, 10),
        (240, 11),
        (280, 12),
    ],
)]
#[case::sparse_pages1(
    &[
        make_empty_metadata(5),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    &[
        (0, 5),
        (40, 0),
        (80, 7),
        (120, 8),
        (160, 9),
        (200, 0),
        (240, 11),
        (280, 12),
    ],
)]
#[case::sparse_pages2(
    &[
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    &[
        (0, 6),
        (40, 7),
        (80, 8),
        (120, 9),
        (160, 10),
        (200, 11),
        (240, 12),
        (280, 0),
    ],
)]
#[case::sparse_pages3(
    &[
        make_empty_metadata(5),
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
    ],
    &[
        (0, 5),
        (40, 6),
        (80, 7),
        (120, 8),
        (160, 9),
        (200, 10),
        (240, 11),
        (280, 0),
    ],
)]
#[trace]
fn test_copy_sparse_metadata_context(
    #[case] metadata: &[PageMetadata],
    #[case] offset_and_value: &[(usize, u8)],
) {
    let context = copy_sparse_metadata_context(metadata);
    for (offset, value) in offset_and_value {
        assert!(context[*offset..][..40].iter().all(|v| *v == *value));
    }
}

#[rstest::rstest]
#[case::single_page(
    &[make_empty_metadata(1)],
    true,
)]
#[case::dense_pages(
    &[
        make_empty_metadata(5),
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    true,
)]
#[case::sparse_pages(
    &[
        make_empty_metadata(5),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    false,
)]
#[case::missing_head(
    &[
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
        make_empty_metadata(12),
    ],
    true,
)]
#[case::missing_tail(
    &[
        make_empty_metadata(5),
        make_empty_metadata(6),
        make_empty_metadata(7),
        make_empty_metadata(8),
        make_empty_metadata(9),
        make_empty_metadata(10),
        make_empty_metadata(11),
    ],
    true,
)]
#[trace]
fn test_metadata_is_contiguous(
    #[case] metadata: &[PageMetadata],
    #[case] expect_contiguous: bool,
) {
    let is_contiguous = metadata_is_contiguous(metadata);
    assert_eq!(is_contiguous, expect_contiguous);
}

fn make_empty_metadata(page_id: u32) -> PageMetadata {
    let mut page_metadata = PageMetadata::unassigned(PageId(page_id));
    page_metadata.context.fill(page_id as u8);
    page_metadata
}
