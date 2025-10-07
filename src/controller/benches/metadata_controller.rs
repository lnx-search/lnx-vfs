extern crate test;

use crate::controller::metadata::MetadataController;
use crate::ctx;
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId};
use crate::page_data::MAX_NUM_PAGES;

#[bench]
fn metadata_write_pages_32kb(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_metadata_writes(bencher, 1)
}

#[bench]
fn metadata_write_pages_32mb(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_metadata_writes(bencher, 1024)
}

#[bench]
fn metadata_write_pages_3gb(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_metadata_writes(bencher, 98_304)
}

#[bench]
fn metadata_write_pages_10gb(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_metadata_writes(bencher, 327_680)
}

fn bench_metadata_writes(
    bencher: &mut test::Bencher,
    num_pages: usize,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let ctx = rt.block_on(ctx::Context::for_test(false));
    let controller = MetadataController::empty(ctx);
    controller.create_blank_page_table(PageFileId(1));

    let mut metadata = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        metadata.push(PageMetadata::null());
    }

    bencher.iter(|| {
        let page_group_id = PageGroupId(fastrand::u64(0..5_000_000));
        let mut last_page = PageId::TERMINATOR;
        for page in metadata.iter_mut() {
            page.id = PageId(fastrand::u32(0..MAX_NUM_PAGES as u32));
            page.group = page_group_id;
            page.next_page_id = last_page;
            last_page = page.id;
        }

        controller.assign_pages_to_group(
            std::hint::black_box(PageFileId(1)),
            std::hint::black_box(page_group_id),
            std::hint::black_box(&metadata),
        );
    });

    Ok(())
}
