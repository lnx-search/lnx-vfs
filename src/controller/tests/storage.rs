use crate::controller::cache::CacheConfig;
use crate::controller::storage::StorageController;
use crate::controller::wal::WalConfig;
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::PageGroupId;

#[tokio::test]
async fn test_write_updates_memory() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");

    assert!(!controller.contains_page_group(PageGroupId(0)));
    let mut write_txn = controller.create_write_txn();
    assert_eq!(format!("{write_txn:?}"), "StorageWriteTx");

    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();

    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");
    assert!(!controller.contains_page_group(PageGroupId(0)));

    write_txn.commit().await.expect("commit transaction");
    assert!(controller.contains_page_group(PageGroupId(0)));

    let mut write_txn = controller.create_write_txn();
    write_txn
        .reassign_group(PageGroupId(0), PageGroupId(1))
        .unwrap();
    write_txn.commit().await.expect("commit transaction");
    assert!(!controller.contains_page_group(PageGroupId(0)));
    assert!(controller.contains_page_group(PageGroupId(1)));

    let mut write_txn = controller.create_write_txn();
    write_txn.unassign_group(PageGroupId(1)).unwrap();
    write_txn.commit().await.expect("commit transaction");
    assert!(!controller.contains_page_group(PageGroupId(1)));
}

#[tokio::test]
async fn test_write_rollback_does_not_set_memory() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");

    assert!(!controller.contains_page_group(PageGroupId(0)));
    let mut write_txn = controller.create_write_txn();
    assert_eq!(format!("{write_txn:?}"), "StorageWriteTx");

    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();

    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");
    assert!(!controller.contains_page_group(PageGroupId(0)));

    write_txn.rollback();
    assert!(!controller.contains_page_group(PageGroupId(0)));
}

#[tokio::test]
async fn test_add_writer_errors_on_writer_finish_error() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");

    assert!(!controller.contains_page_group(PageGroupId(0)));

    let mut write_txn = controller.create_write_txn();

    let writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");

    let err = write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect_err("add writer should error");
    assert_eq!(
        err.to_string(),
        "not all expected data has be submitted to the writer"
    );
}

#[tokio::test]
async fn test_open_already_existing_data() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx.clone())
        .await
        .expect("controller should open");

    let mut write_txn = controller.create_write_txn();
    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();
    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");
    write_txn.commit().await.expect("commit transaction");
    drop(controller);

    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
    assert!(controller.contains_page_group(PageGroupId(0)));
}

#[tokio::test]
async fn test_write_errors_on_concurrent_modification() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");

    let mut write_txn1 = controller.create_write_txn();
    let mut write_txn2 = controller.create_write_txn();
    write_txn1.unassign_group(PageGroupId(0)).unwrap();

    let err = write_txn2
        .reassign_group(PageGroupId(0), PageGroupId(1))
        .expect_err("operation should error because of concurrent modification");
    assert_eq!(
        err.to_string(),
        "concurrent mutation error on PageGroupId(0)"
    );

    let err = write_txn2
        .unassign_group(PageGroupId(0))
        .expect_err("operation should error because of concurrent modification");
    assert_eq!(
        err.to_string(),
        "concurrent mutation error on PageGroupId(0)"
    );

    // Ops within the same transaction are also required to follow this rule!
    let err = write_txn1
        .unassign_group(PageGroupId(0))
        .expect_err("operation should error because of concurrent modification");
    assert_eq!(
        err.to_string(),
        "concurrent mutation error on PageGroupId(0)"
    );

    let mut write_txn3 = controller.create_write_txn();
    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();
    let err = write_txn3
        .add_writer(PageGroupId(0), writer)
        .await
        .expect_err("operation should error because of concurrent modification");
    assert_eq!(
        err.to_string(),
        "concurrent mutation error on PageGroupId(0)"
    );
}

#[tokio::test]
async fn test_write_commit_rolls_back_on_drop() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
    assert!(!controller.contains_page_group(PageGroupId(0)));

    let mut write_txn = controller.create_write_txn();
    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();
    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");

    drop(write_txn);
    assert!(!controller.contains_page_group(PageGroupId(0)));

    // Doing a new txn should go through just fine.
    let mut write_txn = controller.create_write_txn();
    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();
    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");
    write_txn.commit().await.expect("commit transaction");
    assert!(controller.contains_page_group(PageGroupId(0)));
}

#[rstest::rstest]
#[case::submit_write_fail("file::rw::submit_write")]
#[case::wall_sync_fail("wal::sync")]
#[tokio::test]
async fn test_write_commit_rolls_back_on_page_file_error(
    #[case] component_failure: &str,
) {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");

    // Setup existing page group.
    let mut write_txn = controller.create_write_txn();
    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();
    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");
    write_txn.commit().await.expect("commit transaction");

    let mut write_txn = controller.create_write_txn();
    write_txn.unassign_group(PageGroupId(0)).unwrap();

    let scenario = fail::FailScenario::setup();
    fail::cfg(component_failure, "return(-5)").unwrap();

    write_txn
        .commit()
        .await
        .expect_err("transaction should fail");

    scenario.teardown();

    assert!(controller.contains_page_group(PageGroupId(0)));
}

#[rstest::rstest]
#[case::fsync_fail("file::rw::fsync", "1*return(-5)->off")]
#[case::ftruncate_fail("file::rw::truncate", "1*return(-5)->off")]
#[should_panic(
    expected = "ABORT CALL ACTIVATED: WAL file could not be reset to the last successful write, cause: Some(Os { code: 5, kind: Uncategorized, message: \"Input/output error\" })"
)]
#[case::fsync_fail_retry_exceeds_limit("file::rw::fsync", "4*return(-5)->off")]
#[should_panic(
    expected = "ABORT CALL ACTIVATED: WAL file could not be reset to the last successful write, cause: Some(Os { code: 5, kind: Uncategorized, message: \"Input/output error\" })"
)]
#[case::ftruncate_fail_retry_exceeds_limit("file::rw::truncate", "4*return(-5)->off")]
#[tokio::test]
async fn test_write_commit_wal_error_handling(
    #[case] reset_component_failure: &str,
    #[case] fail_cfg: &str,
) {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");

    let mut write_txn = controller.create_write_txn();
    write_txn.unassign_group(PageGroupId(0)).unwrap();

    let scenario = fail::FailScenario::setup();
    fail::cfg("wal::sync", "return(-5)").unwrap();
    fail::cfg(reset_component_failure, fail_cfg).unwrap();

    write_txn
        .commit()
        .await
        .expect_err("transaction should fail");

    scenario.teardown();
}

#[tokio::test]
async fn test_storage_does_not_recover_previously_failed_transaction() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx.clone())
        .await
        .expect("controller should open");

    let mut write_txn = controller.create_write_txn();
    let mut writer = controller
        .create_writer(13)
        .await
        .expect("create new writer");
    writer.write(b"Hello, world!").await.unwrap();
    write_txn
        .add_writer(PageGroupId(0), writer)
        .await
        .expect("add writer");

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", "1*return(-5)->off").unwrap();

    write_txn
        .commit()
        .await
        .expect_err("transaction should fail");

    scenario.teardown();
    assert!(!controller.contains_page_group(PageGroupId(0)));
    drop(controller);

    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
    assert!(!controller.contains_page_group(PageGroupId(0)));
}

#[tokio::test]
async fn test_storage_does_not_recover_truncated_log_entry() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(0));
    let controller = StorageController::open(ctx.clone())
        .await
        .expect("controller should open");

    let mut write_txn = controller.create_write_txn();
    let mut writer = controller.create_writer(13).await.unwrap();
    writer.write(b"Hello, world!").await.unwrap();
    write_txn.add_writer(PageGroupId(0), writer).await.unwrap();
    write_txn.commit().await.unwrap();
    drop(controller);

    let directory = ctx.directory();
    let file_ids = directory.list_dir(FileGroup::Wal).await;
    let wal_file = directory
        .get_rw_file(FileGroup::Wal, file_ids[0])
        .await
        .unwrap();

    // Truncate the WAL to include header + part of the log.
    wal_file.truncate(4096 + 250).await.unwrap();
    drop(wal_file);

    let controller = StorageController::open(ctx.clone())
        .await
        .expect("controller should open");
    assert!(!controller.contains_page_group(PageGroupId(0)));
}

#[rstest::rstest]
#[case::full_read_zero_cache(0, 13, 0, 13)]
#[case::full_read_one_page(32 << 10, 13, 0, 13)]
#[case::full_read_many_pages(512 << 10, 13, 0, 13)]
#[case::with_offset_zero_cache(0, 13, 5, 8)]
#[case::with_offset_one_page(32 << 10, 13, 5, 8)]
#[case::with_offset_many_pages(512 << 10, 13, 5, 8)]
#[case::with_truncation_zero_cache(0, 13, 0, 10)]
#[case::with_truncation_one_page(32 << 10, 13, 0, 10)]
#[case::with_truncation_many_pages(512 << 10, 13, 0, 10)]
#[case::with_offset_and_truncation_zero_cache(0, 13, 5, 5)]
#[case::with_offset_and_truncation_one_page(32 << 10, 13, 5, 5)]
#[case::with_offset_and_truncation_many_pages(512 << 10, 13, 5, 5)]
#[case::multi_page1(0, 64 << 10, 32 << 10, 13)]
#[case::multi_page2(0, 64 << 10, 20 << 10, 20)]
#[tokio::test]
async fn test_storage_read(
    #[case] cache_capacity: u64,
    #[case] buffer_size: u64,
    #[case] read_offset: usize,
    #[case] read_len: usize,
) {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    ctx.set_config(cache_config(cache_capacity));
    let controller = StorageController::open(ctx.clone())
        .await
        .expect("controller should open");

    write_group(&controller, PageGroupId(0), buffer_size).await;

    let buffer_offset = read_offset % 13;
    let mut expected_buffer = Vec::new();
    expected_buffer.extend_from_slice(b"Hello, world!");
    for _ in 0..read_len / 13 {
        expected_buffer.extend_from_slice(b"Hello, world!");
    }
    let expected_buffer = &expected_buffer[buffer_offset..][..read_len];

    let read_ref = controller
        .read_group(
            PageGroupId(0),
            Some(read_offset),
            Some(read_offset + read_len),
        )
        .await
        .expect("read page group");
    assert_eq!(read_ref.as_ref(), expected_buffer);
}

async fn write_group(controller: &StorageController, group: PageGroupId, size: u64) {
    let mut write_txn = controller.create_write_txn();
    let mut writer = controller.create_writer(size).await.unwrap();

    let mut remaining = size;
    while remaining > 0 {
        let take_n = std::cmp::min(remaining, 13);
        let buffer = b"Hello, world!";
        writer.write(&buffer[..take_n as usize]).await.unwrap();
        remaining -= take_n;
    }

    write_txn.add_writer(group, writer).await.unwrap();
    write_txn.commit().await.unwrap();
}

fn cache_config(capacity: u64) -> CacheConfig {
    CacheConfig {
        memory_allowance: capacity,
        disable_gc_worker: true,
    }
}
