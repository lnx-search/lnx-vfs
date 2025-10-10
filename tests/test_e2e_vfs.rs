use anyhow::Context;
use lnx_vfs::{ContextBuilder, VirtualFileSystem, config};

#[rstest::rstest]
#[tokio::test]
async fn test_e2e_run_vfs_read_write(
    #[values(false, true)] encryption: bool,
    #[values(0, 128 << 10, 30 << 20)] io_memory: usize,
) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let mut builder =
        ContextBuilder::new(tmp_dir.path()).io_memory_arena_size(io_memory);

    if encryption {
        builder = builder.with_encryption_key(Some("this is a test".to_string()));
    }

    let ctx = builder.open().await.context("open context")?;

    ctx.set_config(config::WalConfig::default());
    ctx.set_config(config::CacheConfig {
        memory_allowance: 0,
        disable_gc_worker: false,
    });

    let vfs = VirtualFileSystem::open(ctx).await.context("open VFS")?;

    let mut txn = vfs.begin();
    let mut writer = vfs.create_writer(13).await.context("create writer")?;
    writer.write(b"Hello, world!").await.context("write")?;
    txn.add_writer(1, writer).await.context("add writer")?;
    txn.commit().await.context("commit writer")?;

    let result = vfs.read_file(1, 0..).await.context("read all")?;
    assert_eq!(result.as_ref(), b"Hello, world!");

    Ok(())
}
