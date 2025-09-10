use std::io::ErrorKind;

use super::{list_files, tempdir};
use crate::directory::{FileGroup, SystemDirectory};

#[rstest::rstest]
#[tokio::test]
async fn test_dir_sync_fail_on_file_creation(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::dir::sync", "return").unwrap();

    let err = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect_err("create_new_file should fail due to fail point");
    assert_eq!(err.kind(), ErrorKind::Other);
    assert_eq!(err.to_string(), "standard fail point error");

    // created file should be deleted.
    let modified_path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = list_files(&modified_path).unwrap();
    assert_eq!(files.len(), 0);

    scenario.teardown();
}

#[rstest::rstest]
#[trace]
#[case::scheduler_closed("return(scheduler_closed)", "scheduler has closed")]
#[case::cancelled("return(cancelled)", "task cancelled")]
#[case::out_of_capacity("return(out_of_capacity)", "out of capacity")]
#[tokio::test]
async fn test_ring_register_error(
    tempdir: tempfile::TempDir,
    #[case] return_str: &str,
    #[case] expected_io_error: &str,
) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::register_file_async", return_str).unwrap();

    let err = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect_err("create_new_file should fail due to fail point");
    assert_eq!(err.kind(), ErrorKind::Other);
    assert_eq!(err.to_string(), expected_io_error);

    scenario.teardown()
}

#[rstest::rstest]
#[trace]
#[case::scheduler_closed("return(scheduler_closed)", "scheduler has closed")]
#[case::cancelled("return(cancelled)", "task cancelled")]
#[case::out_of_capacity("return(out_of_capacity)", "out of capacity")]
#[tokio::test]
async fn test_removal_error_on_unregister(
    tempdir: tempfile::TempDir,
    #[case] return_str: &str,
    #[case] expected_io_error: &str,
) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file");

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::unregister_file_async", return_str).unwrap();

    let err = directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect_err("create_new_file should fail due to fail point");
    assert_eq!(err.kind(), ErrorKind::Other);
    assert_eq!(err.to_string(), expected_io_error);

    // The file should not be removed on an error when unregistering from the ring.
    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 1);

    fail::cfg("i2o2::fail::unregister_file_async", "off").unwrap();
    fail::cfg("directory::remove_file", "return").unwrap();

    let err = directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect_err("create_new_file should fail due to fail point");
    assert_eq!(err.kind(), ErrorKind::Other);
    assert_eq!(err.to_string(), "remove_file fail point error");

    // The file should be removed because it has been unregistered from the ring.
    // If the removal of the file errors... Then we just have to live with it.
    // NOTE: Basically any of these error are somewhat fatal, failure here would
    //       be very weird to see without the device failing.
    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 0);

    scenario.teardown();
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_create_file_does_not_alter_next_id(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let scenario = fail::FailScenario::setup();
    fail::cfg("directory::create_file", "return").unwrap();

    let err = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect_err("create_new_file should fail due to fail point");
    assert_eq!(err.kind(), ErrorKind::Other);
    assert_eq!(err.to_string(), "create_file fail point error");

    scenario.teardown();

    // The error should not result in the ID it _was_ going to use, still being allocated.
    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file");
    assert_eq!(file_id.as_u32(), 1000);
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_atomic_file_not_removed_before_rename_complete(
    tempdir: tempfile::TempDir,
) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_atomic_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);

    let path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = list_files(&path).unwrap();
    assert_eq!(files, &["0000001000-1000.dat.lnx.atomic"]);

    let scenario = fail::FailScenario::setup();
    fail::cfg("directory::rename_file", "return").unwrap();

    let err = directory
        .persist_atomic_file(FileGroup::Pages, file_id)
        .await
        .expect_err("persist should fail due to fail point");
    assert_eq!(err.kind(), ErrorKind::Other);

    // Files should be untouched.
    let path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = list_files(&path).unwrap();
    assert_eq!(files, &["0000001000-1000.dat.lnx.atomic"]);

    scenario.teardown();

    // Persist retry should work.
    directory
        .persist_atomic_file(FileGroup::Pages, file_id)
        .await
        .expect("atomic file should be persisted");

    let path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = list_files(&path).unwrap();
    assert_eq!(files, &["0000001000-1000.dat.lnx"]);
}
