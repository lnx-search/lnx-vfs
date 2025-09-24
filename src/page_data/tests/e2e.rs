use std::ops::Range;

use crate::buffer::ALLOC_PAGE_SIZE;
use crate::directory::FileGroup;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageId};
use crate::page_data::{DISK_PAGE_SIZE, MAX_NUM_PAGES, PageFile};
use crate::{ctx, file};

// TODO: Improve test cases
#[rstest::rstest]
#[case::one_page_start(
    vec![PageMetadata::unassigned(PageId(0))],
    vec![0..1],
    vec![vec![0]],
    vec![vec![0]],
)]
#[case::one_page_middle_of_file(
    vec![PageMetadata::unassigned(PageId(65_555))],
    vec![0..1],
    vec![vec![0]],
    vec![vec![0]],
)]
#[case::one_page_end_of_file(
    vec![PageMetadata::unassigned(PageId(MAX_NUM_PAGES as u32 - 1))],
    vec![0..1],
    vec![vec![0]],
    vec![vec![0]],
)]
#[case::many_pages_dense_read(
    vec![
        PageMetadata::unassigned(PageId(2)),
        PageMetadata::unassigned(PageId(3)),
        PageMetadata::unassigned(PageId(4)),
        PageMetadata::unassigned(PageId(5)),
        PageMetadata::unassigned(PageId(6)),
        PageMetadata::unassigned(PageId(7)),
    ],
    vec![0..6],
    vec![vec![0, 1, 2, 3, 4, 5]],
    vec![vec![0, 1, 2, 3, 4, 5]],
)]
#[case::many_pages_sparse_read(
    vec![
        PageMetadata::unassigned(PageId(2)),
        PageMetadata::unassigned(PageId(3)),
        PageMetadata::unassigned(PageId(4)),
        PageMetadata::unassigned(PageId(5)),
        PageMetadata::unassigned(PageId(6)),
        PageMetadata::unassigned(PageId(7)),
    ],
    vec![0..6],
    vec![vec![0, 1, 3, 5]],
    vec![vec![0, 1, 2, 3]],  // This is the index related to the `read_range`.
)]
#[tokio::test]
async fn test_reader_understands_writer(
    #[case] metadata: Vec<PageMetadata>,
    #[case] write_ranges: Vec<Range<usize>>,
    #[case] read_ranges: Vec<Vec<usize>>,
    #[case] validate_ranges: Vec<Vec<usize>>,
) {
    let mut metadata = metadata;
    let ctx = ctx::FileContext::for_test(false).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Pages).await;
    let page_file = PageFile::create(ctx.clone(), file.clone(), PageFileId(1))
        .await
        .unwrap();

    let expected_checksum = crc32fast::hash(&vec![4; DISK_PAGE_SIZE]);

    for select_range in write_ranges {
        let block = &mut metadata[select_range];

        let num_alloc_pages = (block.len() * DISK_PAGE_SIZE) / ALLOC_PAGE_SIZE;
        let mut buffer = ctx.alloc_pages(num_alloc_pages);
        buffer.fill(4);

        let expected_len = buffer.len();
        let reply = page_file
            .submit_write_at(block, buffer)
            .await
            .expect("write should be submitted successfully");
        let n = file::wait_for_reply(reply)
            .await
            .expect("write should complete successfully");
        assert_eq!(n, expected_len);
    }

    for (read_range, validate_range) in std::iter::zip(read_ranges, validate_ranges) {
        let mut block = Vec::with_capacity(read_range.len());
        let total_delta = (read_range.last().unwrap() - read_range.first().unwrap()) + 1;
        for idx in read_range {
            block.push(metadata[idx]);
        }
        let num_alloc_pages = (total_delta * DISK_PAGE_SIZE) / ALLOC_PAGE_SIZE;

        let buffer = ctx.alloc_pages(num_alloc_pages);
        let buffer = page_file.read_at(&block, buffer).await.unwrap();

        for validate_pos in validate_range {
            let start = validate_pos * DISK_PAGE_SIZE;
            let end = start + DISK_PAGE_SIZE;
            assert_eq!(crc32fast::hash(&buffer[start..end]), expected_checksum);
        }
    }
}
