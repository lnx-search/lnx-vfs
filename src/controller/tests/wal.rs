use std::sync::Arc;

use crate::controller::wal::{WalConfig, WalController, WalError};
use crate::directory::FileGroup;
use crate::layout::log::{LogEntry, LogOp};
use crate::layout::{PageFileId, PageId};
use crate::{ctx, page_op_log};

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_controller_write_entries(#[values(1, 4, 30, 120)] num_entries: usize) {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), WalConfig::default())
        .await
        .expect("Failed to create WalController");

    let mut entries = Vec::with_capacity(num_entries);
    for id in 0..num_entries {
        let entry = LogEntry {
            transaction_id: 1,
            transaction_n_entries: 1,
            sequence_id: 1,
            page_file_id: PageFileId(1),
            page_id: PageId(id as u32),
            op: LogOp::Write,
        };
        entries.push((entry, None));
    }

    controller
        .write_updates(entries)
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

    let mut retrieved_num_entries = 0;
    while let Some(block) = reader.next_block().await.unwrap() {
        retrieved_num_entries += block.num_entries();
    }
    assert_eq!(retrieved_num_entries, num_entries);
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_controller_sync_op_stamp(#[values(1, 4, 30, 120)] num_entries: usize) {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), WalConfig::default())
        .await
        .expect("Failed to create WalController");

    assert_eq!(controller.next_checkpoint_op_stamp(), 0);

    let mut entries = Vec::with_capacity(num_entries);
    for id in 0..num_entries {
        let entry = LogEntry {
            transaction_id: id as u64,
            transaction_n_entries: 1,
            sequence_id: 1,
            page_file_id: PageFileId(1),
            page_id: PageId(id as u32),
            op: LogOp::Write,
        };
        entries.push((entry, None));
    }

    controller
        .write_updates(entries)
        .await
        .expect("controller should write entries");

    // This is because the checkpoint op stamp should only be incremented
    // when a writer is rotated.
    assert_eq!(
        controller.next_checkpoint_op_stamp(),
        1 + controller.num_checkpoint_pending_writers() as u64,
        "op stamp should align with the number of pending writers",
    );
}

#[tokio::test]
async fn test_controller_rotate_writers() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), WalConfig::default())
        .await
        .expect("Failed to create WalController");

    let entry = LogEntry {
        transaction_id: 0,
        transaction_n_entries: 1,
        sequence_id: 1,
        page_file_id: PageFileId(1),
        page_id: PageId(1),
        op: LogOp::Write,
    };
    let mut entries = Vec::with_capacity(10);
    for _ in 0..10 {
        entries.push((entry, None));
    }

    controller
        .write_updates(entries)
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

    let checkpoint_op_stamp = controller.next_checkpoint_op_stamp();
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

    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), config)
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

    let checkpoint_op_stamp = controller.next_checkpoint_op_stamp();
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

    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), config)
        .await
        .expect("Failed to create WalController");

    let entry = LogEntry {
        transaction_id: 0,
        transaction_n_entries: 1,
        sequence_id: 1,
        page_file_id: PageFileId(1),
        page_id: PageId(1),
        op: LogOp::Write,
    };

    for _ in 0..3 {
        let mut entries = Vec::with_capacity(10);
        for _ in 0..100 {
            entries.push((entry, None));
        }
        controller
            .write_updates(entries)
            .await
            .expect("controller should write entries");
    }

    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 2);
}

#[tokio::test]
async fn test_wal_file_rotate_due_to_error() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), WalConfig::default())
        .await
        .expect("Failed to create WalController");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return(-4)").unwrap();

    let entry = LogEntry {
        transaction_id: 0,
        transaction_n_entries: 1,
        sequence_id: 1,
        page_file_id: PageFileId(1),
        page_id: PageId(1),
        op: LogOp::Write,
    };
    let mut entries = Vec::with_capacity(10);
    for _ in 0..10 {
        entries.push((entry, None));
    }

    let err = controller
        .write_updates(entries.clone())
        .await
        .expect_err("should error");
    assert!(matches!(err, WalError::Io(_)));
    scenario.teardown();

    controller
        .write_updates(entries.clone())
        .await
        .expect("controller should rotate and write updates");
    assert_eq!(controller.num_free_writers(), 0);
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);
}

#[tokio::test]
async fn test_rotated_file_not_recycled_on_lower_op_stamp() {
    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), WalConfig::default())
        .await
        .expect("Failed to create WalController");

    let entry = LogEntry {
        transaction_id: 0,
        transaction_n_entries: 1,
        sequence_id: 1,
        page_file_id: PageFileId(1),
        page_id: PageId(1),
        op: LogOp::Write,
    };
    let mut entries = Vec::with_capacity(10);
    for _ in 0..10 {
        entries.push((entry, None));
    }

    controller
        .write_updates(entries)
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

    controller.recycle_writers(0).await.unwrap();
    assert_eq!(controller.num_free_writers(), 1);
    assert_eq!(controller.num_checkpoint_pending_writers(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_coalesce_updates() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(false).await;
    let controller = WalController::create(ctx.clone(), WalConfig::default())
        .await
        .map(Arc::new)
        .expect("Failed to create WalController");

    let entry = LogEntry {
        transaction_id: 0,
        transaction_n_entries: 1,
        sequence_id: 1,
        page_file_id: PageFileId(1),
        page_id: PageId(1),
        op: LogOp::Write,
    };
    let mut entries = Vec::with_capacity(3);
    for _ in 0..3 {
        entries.push((entry, None));
    }

    let scenario = fail::FailScenario::setup();
    fail::cfg("wal::write_log", "sleep(1)").unwrap();
    fail::cfg("wal::sync", "sleep(200)").unwrap();

    let task1 = tokio::spawn({
        let controller = controller.clone();
        let entries = entries.clone();
        async move {
            controller
                .write_updates(entries)
                .await
                .expect("controller should write entries");
        }
    });

    let task2 = tokio::spawn({
        let controller = controller.clone();
        let entries = entries.clone();
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
