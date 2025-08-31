use std::io;

use super::tempdir;
use crate::buffer::DmaBuffer;
use crate::directory::{FileGroup, SystemDirectory};

#[rstest::rstest]
#[tokio::test]
async fn test_file_write_lockout(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory.create_new_file(FileGroup::Pages).await.unwrap();

    let rw_file = directory
        .get_rw_file(FileGroup::Pages, file_id)
        .await
        .expect("get rw file");

    let scenario = fail::FailScenario::setup();
    fail::cfg("file::rw::submit_write", "return(-4)").unwrap();

    let mut buffer = DmaBuffer::alloc_sys(1);
    buffer[..13].copy_from_slice(b"Hello, world!");
    let err = rw_file
        .write_buffer(&mut buffer, 0)
        .await
        .expect_err("read buffer should error");
    assert_eq!(err.kind(), io::ErrorKind::Interrupted);

    // A normal write error is not cause for issue.
    assert!(rw_file.is_writeable());

    fail::cfg("file::rw::submit_write", "off").unwrap();
    fail::cfg("i2o2::fail::try_get_result", "return(-4)").unwrap();

    let err = rw_file.fdatasync().await.expect_err("fsync should error");
    assert_eq!(err.kind(), io::ErrorKind::Interrupted);
    // fsync failed! We treat this as the end of the world.
    assert!(!rw_file.is_writeable());

    scenario.teardown();
}
