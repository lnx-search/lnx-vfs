use std::sync::Arc;

use crate::controller::tests::create_sample_ops;
use crate::controller::wal::{WalConfig, WalController};
use crate::directory::FileGroup;
use crate::{ctx, page_op_log};

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_controller_write_entries(#[values(1, 4, 30, 120)] num_ops: usize) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let ops = create_sample_ops(num_ops);

    controller
        .write_updates(ops)
        .await
        .expect("controller should write entries");

    let file_id = controller.active_writer_id().await;
    let read_file = ctx
        .directory()
        .get_ro_file(FileGroup::Wal, file_id)
        .await
        .unwrap();

    let mut reader = page_op_log::LogFileReader::open(ctx.clone(), read_file)
        .await
        .expect("reader should be able to open WAL file");

    let mut retrieved_num_transactions = 0;
    let mut recovered_ops = Vec::new();
    while let Ok(Some(_transaction_id)) =
        reader.next_transaction(&mut recovered_ops).await
    {
        retrieved_num_transactions += 1;
    }
    assert_eq!(retrieved_num_transactions, 1);
    assert_eq!(recovered_ops.len(), num_ops);
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_controller_sync_op_stamp(#[values(1, 4, 30, 120)] num_ops: usize) {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    assert_eq!(controller.op_stamp(), 1);

    let ops = create_sample_ops(num_ops);

    controller
        .write_updates(ops)
        .await
        .expect("controller should write entries");

    // This is because the checkpoint op stamp should only be incremented
    // when a writer is rotated.
    assert_eq!(
        controller.op_stamp(),
        1 + controller.num_checkpoint_pending_writers() as u64,
        "op stamp should align with the number of pending writers",
    );
}

#[tokio::test]
async fn test_controller_rotate_writers() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let ops = create_sample_ops(10);

    controller
        .write_updates(ops)
        .await
        .expect("controller should write entries");

    let writer_id1 = controller.active_writer_id().await;
    controller
        .prepare_checkpoint()
        .await
        .expect("Failed to prepare checkpoint");
    let writer_id2 = controller.active_writer_id().await;
    assert_ne!(writer_id1, writer_id2, "writers should be rotated");

    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);

    let checkpoint_op_stamp = controller.op_stamp();
    controller
        .recycle_writers(checkpoint_op_stamp)
        .await
        .expect("writers should be recycled into free writers list");
    assert_eq!(controller.num_free_writers(), 1);
    assert_eq!(controller.num_checkpoint_pending_writers(), 0);
}

#[tokio::test]
async fn test_evict_free_writers() {
    let config = WalConfig {
        max_wal_file_pool_size: 1,
        ..Default::default()
    };

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(config);
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    controller
        .prepare_checkpoint()
        .await
        .expect("Failed to prepare checkpoint");

    controller
        .prepare_checkpoint()
        .await
        .expect("Failed to prepare checkpoint");

    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 2);

    let checkpoint_op_stamp = controller.op_stamp();
    controller
        .recycle_writers(checkpoint_op_stamp)
        .await
        .expect("writers should be recycled into free writers list");
    assert_eq!(controller.num_free_writers(), 1);
    assert_eq!(controller.num_checkpoint_pending_writers(), 0);
}

#[tokio::test]
async fn test_wal_file_rotation_due_to_size() {
    let config = WalConfig {
        soft_max_wal_size: 8 << 10,
        ..Default::default()
    };

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(config);
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    for _ in 0..8 {
        let ops = create_sample_ops(100);
        controller
            .write_updates(ops)
            .await
            .expect("controller should write entries");
    }

    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 2);
}

#[tokio::test]
async fn test_wal_file_dont_rotate_due_if_reset_ignored() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return(-4)").unwrap();
    fail::cfg("wal::reset_to_last_safe_point", "return").unwrap();

    let ops = create_sample_ops(10);

    let err = controller
        .write_updates(ops.clone())
        .await
        .expect_err("should error");
    assert_eq!(
        err.to_string(),
        "WAL Error: Interrupted system call (os error 4)"
    );
    assert_eq!(
        format!("{err:?}"),
        "Io(Os { code: 4, kind: Interrupted, message: \"Interrupted system call\" })"
    );

    fail::cfg("file::rw::submit_write", "off").unwrap();
    fail::cfg("wal::reset_to_last_safe_point", "return").unwrap();

    let err = controller
        .write_updates(ops.clone())
        .await
        .expect_err("controller should not rotate automatically");
    assert_eq!(
        err.to_string(),
        "WAL Error: writer is locked due to prior error"
    );

    scenario.teardown();
}

#[tokio::test]
async fn test_wal_file_reset_becomes_sealed_and_rotates() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return(-4)").unwrap();

    let ops = create_sample_ops(10);

    let err = controller
        .write_updates(ops.clone())
        .await
        .expect_err("should error and reset");
    assert_eq!(
        err.to_string(),
        "WAL Error: Interrupted system call (os error 4)"
    );
    scenario.teardown();

    controller
        .write_updates(ops)
        .await
        .expect("controller should rotate file after reset");
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);
    assert_eq!(controller.active_writer_id().await.as_u32(), 1001);
}

#[rstest::rstest]
#[case::fsync_fail("file::rw::sync")]
#[case::ftruncate_fail("file::rw::truncate")]
#[tokio::test]
async fn test_wal_file_abort_hook_triggered_after_reset_retry(
    #[case] reset_component_failure: &str,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return(-4)").unwrap();
    fail::cfg(reset_component_failure, "2*return(-2)->off").unwrap();

    let ops = create_sample_ops(10);

    assert_eq!(controller.active_writer_id().await.as_u32(), 1000);

    let err = controller
        .write_updates(ops.clone())
        .await
        .expect_err("should error and reset");
    assert_eq!(
        err.to_string(),
        "WAL Error: Interrupted system call (os error 4)"
    );

    scenario.teardown();

    controller
        .write_updates(ops)
        .await
        .expect("controller should rotate file after reset");
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);
    assert_eq!(controller.active_writer_id().await.as_u32(), 1001);
}

#[rstest::rstest]
#[tokio::test]
#[should_panic(
    expected = "ABORT CALL ACTIVATED: WAL file could not be reset to the last successful write, cause: Some(Os { code: 2, kind: NotFound, message: \"No such file or directory\" })"
)]
async fn test_wal_file_abort_hook_triggered_after_reset_retry_fail(
    #[values("file::rw::sync", "file::rw::truncate")] reset_component_failure: &str,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return(-4)").unwrap();
    fail::cfg(reset_component_failure, "3*return(-2)->off").unwrap();

    let ops = create_sample_ops(10);

    assert_eq!(controller.active_writer_id().await.as_u32(), 1000);

    controller.write_updates(ops.clone()).await.unwrap();

    scenario.teardown();
}

#[tokio::test]
async fn test_rotated_file_not_recycled_on_lower_op_stamp() {
    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .expect("Failed to create WalController");

    let ops = create_sample_ops(10);

    controller
        .write_updates(ops)
        .await
        .expect("controller should write entries");

    controller
        .prepare_checkpoint()
        .await
        .expect("Failed to prepare checkpoint");
    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);

    controller
        .prepare_checkpoint()
        .await
        .expect("Failed to prepare checkpoint");
    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 2);

    controller.recycle_writers(1).await.unwrap();
    assert_eq!(controller.num_free_writers(), 1);
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_flaky_write_coalesce_updates() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::Context::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = WalController::create(ctx.clone())
        .await
        .map(Arc::new)
        .expect("Failed to create WalController");

    let ops = create_sample_ops(10);

    let scenario = fail::FailScenario::setup();
    fail::cfg("wal::write_log", "sleep(1)").unwrap();
    fail::cfg("wal::sync", "sleep(200)").unwrap();

    let task1 = tokio::spawn({
        let controller = controller.clone();
        let entries = ops.clone();
        async move {
            controller
                .write_updates(entries)
                .await
                .expect("controller should write entries");
        }
    });

    let task2 = tokio::spawn({
        let controller = controller.clone();
        let entries = ops.clone();
        async move {
            controller
                .write_updates(entries)
                .await
                .expect("controller should write entries");
        }
    });

    let (_r1, _r2) = tokio::join!(task1, task2);
    scenario.teardown();

    assert_eq!(controller.total_write_operations(), 2);
    assert_eq!(controller.total_coalesced_write_operations(), 1);
    assert_eq!(controller.total_coalesced_failures(), 0);
}
