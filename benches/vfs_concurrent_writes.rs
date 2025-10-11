use std::cmp;
use std::time::{Duration, Instant};

use lnx_vfs::config::{CacheConfig, PageFileConfig, StorageConfig, WalConfig};
use lnx_vfs::{ContextBuilder, VirtualFileSystem};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    tracing::info!("starting concurrent benchmark");

    for pre_allocate in [false, true] {
        for concurrency in [10, 50, 100] {
            run_bench(pre_allocate, 1000, 2 << 10, concurrency).await?;
            tokio::time::sleep(Duration::from_secs(10)).await;

            run_bench(pre_allocate, 100, 2 << 20, concurrency).await?;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    tracing::info!("complete");

    Ok(())
}

async fn run_bench(
    pre_allocate: bool,
    num_iters: u64,
    file_size: usize,
    concurrency: usize,
) -> anyhow::Result<()> {
    tracing::info!(
        "starting run (pre_allocate={pre_allocate}, concurrency={concurrency}) - {num_iters} iters, {} per file",
        humanize(file_size as u64)
    );

    let tmp_dir = tempfile::tempdir_in("./test-data")?;
    let builder = ContextBuilder::new(tmp_dir.path());
    let ctx = builder.open().await?;
    ctx.set_config(WalConfig {
        preallocate_file: pre_allocate,
        ..Default::default()
    });
    ctx.set_config(CacheConfig {
        memory_allowance: 0,
        disable_gc_worker: true,
    });
    ctx.set_config(StorageConfig {
        wal_checkpoint_interval: None,
    });
    ctx.set_config(PageFileConfig {
        preallocate_file: pre_allocate,
    });
    let vfs = VirtualFileSystem::open(ctx).await?;

    let start = Instant::now();

    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let handle = tokio::spawn(write_task(vfs.clone(), num_iters, file_size));
        tasks.push(handle);
    }

    let mut total_write_time = Duration::default();
    let mut total_commit_time = Duration::default();
    for handle in tasks {
        let (write_time, commit_time) = handle.await??;
        total_write_time += write_time;
        total_commit_time += commit_time;
    }

    let elapsed = start.elapsed();
    let per_file = elapsed / num_iters as u32;
    let bytes_per_sec = (file_size as f32 / per_file.as_secs_f32()) as u64;

    total_write_time /= concurrency as u32;
    total_commit_time /= concurrency as u32;

    total_write_time /= num_iters as u32;
    total_commit_time /= num_iters as u32;

    tracing::info!(
        "   {num_iters} iters took {elapsed:?}, \
        {per_file:?}/iter {}/s \
        {total_commit_time:?} on commit, \
        {total_write_time:?} on write",
        humanize(bytes_per_sec),
    );

    Ok(())
}

fn humanize(size: u64) -> String {
    humansize::format_size(size, humansize::DECIMAL)
}

async fn write_task(
    vfs: VirtualFileSystem,
    num_iters: u64,
    file_size: usize,
) -> anyhow::Result<(Duration, Duration)> {
    let mut write_time = Duration::default();
    let mut commit_time = Duration::default();

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

    Ok((write_time, commit_time))
}
