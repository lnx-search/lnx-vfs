use divan::Bencher;
use lnx_vfs::bench::{InitState, PageAllocator};

fn main() {
    divan::main();
}

#[divan::bench(sample_size = 1000, sample_count = 1000)]
fn bench_disk_alloc(bencher: Bencher) {
    bencher
        .with_inputs(|| PageAllocator::new(InitState::Free))
        .bench_local_values(|mut allocator| {
            let n = fastrand::u32(0..400_000);
            allocator.alloc(std::hint::black_box(n))
        });
}
