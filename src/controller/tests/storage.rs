use crate::controller::storage::StorageController;
use crate::controller::wal::WalConfig;
use crate::ctx;
use crate::layout::PageGroupId;

#[tokio::test]
async fn test_write_updates_memory() {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
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
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
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
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
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

    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
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
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
}

#[tokio::test]
async fn test_write_commit_rolls_back_on_drop() {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
}

#[rstest::rstest]
#[case::submit_write_fail("file::rw::submit_write")]
#[case::wall_sync_fail("wal::sync")]
#[tokio::test]
async fn test_write_commit_rolls_back_on_page_file_error(
    #[case] component_failure: &str,
) {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
}

#[rstest::rstest]
#[case::fsync_fail("file::rw::fsync")]
#[case::ftruncate_fail("file::rw::truncate")]
#[tokio::test]
async fn test_write_commit_wal_error_handling(#[case] reset_component_failure: &str) {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(WalConfig::default());
    let controller = StorageController::open(ctx)
        .await
        .expect("controller should open");
}
