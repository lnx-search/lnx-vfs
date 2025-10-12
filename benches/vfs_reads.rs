use std::cmp;
use std::time::{Duration, Instant};

use anyhow::Context;
use lnx_vfs::config::{CacheConfig, PageFileConfig, StorageConfig, WalConfig};
use lnx_vfs::{ContextBuilder, VirtualFileSystem};

const NUM_READ_ITERS: usize = 10_000;
const IO_MEMORY_ALLOWANCE: usize = 512 << 20;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    fastrand::seed(239623572352352);

    tracing::info!("starting benchmark");

    for file_size in [5 << 30, 10 << 30] {
        tracing::info!("Running bench: {}", humanize(file_size));

        for read_concurrency in [1, 10, 25, 50] {
            tracing::info!("    Running concurrency={read_concurrency}");

            for cache_size in [0, 2 << 30, 6 << 30] {
                run_bench(file_size, read_concurrency, cache_size).await?;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    tracing::info!("complete");

    Ok(())
}

fn humanize(size: u64) -> String {
    humansize::format_size(size, humansize::DECIMAL)
}

async fn run_bench(
    file_size: u64,
    read_concurrency: usize,
    cache_size: usize,
) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir_in("./test-data")?;
    let builder =
        ContextBuilder::new(tmp_dir.path()).io_memory_arena_size(IO_MEMORY_ALLOWANCE);
    let ctx = builder.open().await?;
    ctx.set_config(WalConfig {
        preallocate_file: true,
        ..Default::default()
    });
    ctx.set_config(CacheConfig {
        memory_allowance: cache_size as u64,
        disable_gc_worker: false,
    });
    ctx.set_config(StorageConfig {
        wal_checkpoint_interval: None,
    });
    ctx.set_config(PageFileConfig {
        preallocate_file: true,
    });
    let vfs = VirtualFileSystem::open(ctx).await?;

    write_file(&vfs, file_size as usize)
        .await
        .context("write data")?;

    let mut handles = Vec::new();
    for _ in 0..read_concurrency {
        let handle = tokio::spawn(read_task(vfs.clone(), file_size as usize));
        handles.push(handle);
    }

    let mut total_elapsed = Duration::default();
    let mut total_bytes_read = 0;
    for handle in handles {
        let (elapsed, bytes) = handle.await??;
        total_bytes_read += bytes;
        total_elapsed += elapsed;
    }

    let per_reader = total_elapsed / read_concurrency as u32;
    let bytes_per_sec = (total_bytes_read as f64 / per_reader.as_secs_f64()) as u64;

    tracing::info!(
        "       cache size: {}, result: {per_reader:?} per reader {}/s",
        humanize(cache_size as u64),
        humanize(bytes_per_sec),
    );

    Ok(())
}

async fn read_task(
    vfs: VirtualFileSystem,
    file_size: usize,
) -> anyhow::Result<(Duration, u64)> {
    let start = Instant::now();
    let mut total_bytes_read = 0;

    for _ in 0..NUM_READ_ITERS {
        let start_pos = fastrand::usize(..file_size);
        let read_len = cmp::min(fastrand::usize(0..(20 << 20)), file_size - start_pos);
        let end_pos = start_pos + read_len;

        let read_ref = vfs.read_file(1, start_pos..end_pos).await?;
        total_bytes_read += read_ref.len() as u64;
    }

    Ok((start.elapsed(), total_bytes_read))
}

async fn write_file(vfs: &VirtualFileSystem, file_size: usize) -> anyhow::Result<()> {
    let mut txn = vfs.begin();
    let mut writer = vfs.create_writer(file_size as u64).await?;

    let mut bytes_left: usize = file_size;
    let mut buffer = vec![0; 128 << 10];
    fastrand::fill(&mut buffer);
    while bytes_left > 0 {
        let take_n = cmp::min(bytes_left, buffer.len());
        writer.write(&buffer[..take_n]).await?;
        bytes_left -= take_n;
    }

    txn.add_writer(1, writer).await?;
    txn.commit().await?;

    Ok(())
}
