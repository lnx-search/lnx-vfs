use std::cmp;
use std::time::{Duration, Instant};

use lnx_vfs::config::{CacheConfig, PageFileConfig, WalConfig};
use lnx_vfs::{ContextBuilder, VirtualFileSystem};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    tracing::info!("starting benchmark");

    for pre_allocate in [false, true] {
        run_bench(pre_allocate, 1000, 2 << 10).await?;
        tokio::time::sleep(Duration::from_secs(10)).await;

        run_bench(pre_allocate, 100, 2 << 20).await?;
        tokio::time::sleep(Duration::from_secs(10)).await;

        run_bench(pre_allocate, 10, 2 << 30).await?;
    }

    tracing::info!("complete");

    Ok(())
}

async fn run_bench(
    pre_allocate: bool,
    num_iters: u64,
    file_size: usize,
) -> anyhow::Result<()> {
    tracing::info!(
        "starting run (pre_allocate={pre_allocate}) - {num_iters} iters, {} per file",
        humanize(file_size as u64)
    );

    let tmp_dir = tempfile::tempdir_in("./test-data")?;
    let builder = ContextBuilder::new(tmp_dir.path());
    let ctx = builder.open().await?;
    ctx.set_config(WalConfig::default());
    ctx.set_config(CacheConfig {
        memory_allowance: 0,
        disable_gc_worker: true,
    });
    ctx.set_config(PageFileConfig {
        preallocate_file: pre_allocate,
    });
    let vfs = VirtualFileSystem::open(ctx).await?;

    let start = Instant::now();
    let mut commit_time = Duration::default();
    let mut write_time = Duration::default();

    for file_id in 0..num_iters {
        let mut txn = vfs.begin();
        let mut writer = vfs.create_writer(file_size as u64).await?;

        let write_start = Instant::now();
        let mut bytes_left: usize = file_size;
        let mut buffer = vec![0; 128 << 10];
        fastrand::fill(&mut buffer);
        while bytes_left > 0 {
            let take_n = cmp::min(bytes_left, buffer.len());
            writer.write(&buffer[..take_n]).await?;
            bytes_left -= take_n;
        }

        txn.add_writer(file_id, writer).await?;
        write_time += write_start.elapsed();

        let cmt_start = Instant::now();
        txn.commit().await?;
        commit_time += cmt_start.elapsed();
    }

    let elapsed = start.elapsed();
    let per_file = elapsed / num_iters as u32;
    let bytes_per_sec = (file_size as f32 / per_file.as_secs_f32()) as u64;

    commit_time /= num_iters as u32;
    write_time /= num_iters as u32;

    tracing::info!(
        "{num_iters} iters took {elapsed:?}, \
        {per_file:?}/iter {}/s \
        {commit_time:?} on commit, \
        {write_time:?} on write",
        humanize(bytes_per_sec),
    );

    Ok(())
}

fn humanize(size: u64) -> String {
    humansize::format_size(size, humansize::DECIMAL)
}
