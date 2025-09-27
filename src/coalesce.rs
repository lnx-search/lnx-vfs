//! Very basic IO coalesce algorithm to optimise IO on the pages.
//!
//! Both reads and writes are capable of being coalesced, however, writes
//! behave slightly differently and must always be dense, meaning we will not
//! write a bit more data for the sake of doing a single IOP.
//! This is because of our use of `O_DIRECT` and avoiding a read-before-write situation
//! potentially corrupting data.
//!

use std::cmp;
use std::ops::Range;

use smallvec::SmallVec;

const DEFAULT_AMPLIFICATION_FACTOR: f32 = 1.2;

/// Coalesce a set of read ranges (of pages) into more optimised IOPS where ever possible.
///
/// The provided ranges must be sorted by position.
///
/// The behaviour can be altered by adjusting the config parameters:
///
/// - `max_iop_page_spans` the maximum number of pages the final IOP is allowed to span.
/// - `amplification_factor` is the amount of amplification that is allowed when merging IOPS
///   a larger amplification factor allows mering of more sparse IOPS but at the cost of reading
///   more data overall which may end up causing a slow-down.
///   For example, an amplification factor of `1.2` would allow a reading upto 20% more data
///   in order to merge the IOPS.
pub fn coalesce_read(
    ranges: impl IntoIterator<Item = Range<u32>>,
    max_iop_page_spans: u32,
    amplification_factor: Option<f32>,
) -> SmallVec<[Range<u32>; 8]> {
    let amplification_factor =
        amplification_factor.unwrap_or(DEFAULT_AMPLIFICATION_FACTOR);
    assert!(
        amplification_factor >= 1.0,
        "amplification_factor must be at least 1.0"
    );

    let mut iops = SmallVec::<[Range<u32>; 8]>::new();

    // Perform a simple concat on ranges which are continuous to one another
    let mut ranges =
        SplitLargeIops::new(ranges.into_iter(), max_iop_page_spans).peekable();
    while let Some(range) = ranges.next() {
        let mut span = range.clone();

        while let Some(next_page) = ranges.peek() {
            let next_page = next_page.clone();
            let combined_len = span.len() + next_page.len();

            if span.end == next_page.start && combined_len <= max_iop_page_spans as usize
            {
                let _ = ranges.next();
                span.end = next_page.end;
            } else {
                break;
            }
        }

        iops.push(span);
    }

    if amplification_factor == 1.0 {
        return iops;
    }

    // Merge sparse page spans by the target amplification_factor
    let mut i = 0;
    while i < (iops.len() - 1) {
        let left_iop = &iops[i];
        let right_iop = &iops[i + 1];

        let combined_len = (left_iop.start..right_iop.end).len();
        if combined_len > max_iop_page_spans as usize {
            i += 1;
            continue;
        }

        let allowed_gap = (combined_len as f32 * (amplification_factor - 1.0)) as u32;
        let actual_gap = right_iop.start - left_iop.end;
        if actual_gap <= allowed_gap {
            iops[i].end = right_iop.end;
            iops.remove(i + 1);
        } else {
            i += 1;
        }
    }

    iops
}

/// Coalesce a set of write ranges (of pages) into more optimised IOPS where ever possible.
///
/// The behaviour can be altered by adjusting the config parameters:
///
/// - `max_iop_page_spans` the maximum number of pages the final IOP is allowed to span.
pub fn coalesce_write(
    ranges: impl IntoIterator<Item = Range<u32>>,
    max_iop_page_spans: u32,
) -> SmallVec<[Range<u32>; 8]> {
    coalesce_read(ranges, max_iop_page_spans, Some(1.0))
}

struct SplitLargeIops<I> {
    iter: I,
    max_iop_size: usize,
    temp_range: Range<u32>,
}

impl<I> SplitLargeIops<I> {
    fn new(iter: I, max_iop_size: u32) -> Self {
        Self {
            iter,
            max_iop_size: max_iop_size as usize,
            temp_range: 0..0,
        }
    }
}

impl<I> Iterator for SplitLargeIops<I>
where
    I: Iterator<Item = Range<u32>>,
{
    type Item = Range<u32>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.temp_range.is_empty() {
            self.temp_range = self.iter.next()?;
        }

        let take_n = cmp::min(self.temp_range.len(), self.max_iop_size);
        let start = self.temp_range.start;
        let end = start + take_n as u32;
        self.temp_range = end..self.temp_range.end;

        Some(start..end)
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::coalesce_contiguous_ranges(
        &[0..4, 4..5, 5..6, 8..9, 11..15, 15..17],
        6,
        None,
        &[0..6, 8..9, 11..17]
    )]
    #[case::all_sparse_ranges(
        &[0..2, 20..25, 30..31],
        6,
        None,
        &[0..2, 20..25, 30..31],
    )]
    #[case::contiguous_ranges_obey_limits(
        &[0..20, 20..25, 25..26, 30..35],
        6,
        None,
        &[0..6, 6..12, 12..18, 18..20, 20..26, 30..35],
    )]
    #[case::sparse_pages_eager_merge_with_factor(
        &[0..2, 6..8, 10..12, 16..20],
        8,
        Some(1.5),
        &[0..8, 10..12, 16..20],
    )]
    #[case::sparse_pages_contiguous_takes_priority(
        &[0..4, 10..12, 14..16, 16..20],
        8,
        Some(1.5),
        &[0..4, 10..12, 14..20],
    )]
    #[case::sparse_pages_merge(
        &[0..4, 10..12, 14..16],
        8,
        Some(1.5),
        &[0..4, 10..16],
    )]
    #[case::many_sparse_pages_merge(
        &[0..2, 4..6, 7..8, 13..16],
        8,
        Some(1.5),
        &[0..8, 13..16],
    )]
    #[should_panic(expected = "attempt to subtract with overflow")]
    #[case::out_of_order_spans(
        &[2..5, 0..7, 7..8, 13..16],
        8,
        Some(1.5),
        &[],
    )]
    #[case::previous_bug1(
        &[0..1, 4..5, 63000..63001],
        8,
        Some(1.3),
        &[0..1, 4..5, 63000..63001],
    )]
    fn test_coalesce_read(
        #[case] ranges: &[Range<u32>],
        #[case] max_iop_page_spans: u32,
        #[case] amplification_factor: Option<f32>,
        #[case] expected_ranges: &[Range<u32>],
    ) {
        let iops =
            coalesce_read(ranges.to_vec(), max_iop_page_spans, amplification_factor);
        assert_eq!(iops.as_slice(), expected_ranges);
    }

    #[rstest::rstest]
    #[case::coalesce_contiguous_ranges(
        &[0..4, 4..5, 5..6, 8..9, 11..15, 15..17],
        6,
        &[0..6, 8..9, 11..17]
    )]
    #[case::all_sparse_ranges(
        &[0..2, 20..25, 30..31],
        6,
        &[0..2, 20..25, 30..31],
    )]
    #[case::contiguous_ranges_obey_limits(
        &[0..20, 20..25, 25..26, 30..35],
        6,
        &[0..6, 6..12, 12..18, 18..20, 20..26, 30..35],
    )]
    #[case::sparse_pages_not_merged(
        &[0..2, 6..8, 10..12, 16..20],
        8,
        &[0..2, 6..8, 10..12, 16..20],
    )]
    #[case::sparse_pages_contiguous_takes_priority(
        &[0..4, 10..12, 14..16, 16..20],
        8,
        &[0..4, 10..12, 14..20],
    )]
    #[case::many_sparse_pages_merge(
        &[0..2, 4..6, 7..8, 13..16],
        8,
        &[0..2, 4..6, 7..8, 13..16],
    )]
    #[case::out_of_order_spans(
        &[2..5, 0..7, 7..8, 13..16],
        8,
        &[2..5, 0..8, 13..16],
    )]
    fn test_coalesce_write(
        #[case] ranges: &[Range<u32>],
        #[case] max_iop_page_spans: u32,
        #[case] expected_ranges: &[Range<u32>],
    ) {
        let iops = coalesce_write(ranges.to_vec(), max_iop_page_spans);
        assert_eq!(iops.as_slice(), expected_ranges);
    }
}
