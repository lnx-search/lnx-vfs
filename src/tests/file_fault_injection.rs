use super::tempdir;
use crate::directory::{FileGroup, SystemDirectory};
use crate::file;
use crate::file::DISK_ALIGN;

#[rstest::rstest]
#[case("return(50)", "Ok(50)")]
#[case(
    "return(-4)",
    "Err(Os { code: 4, kind: Interrupted, message: \"Interrupted system call\" })"
)]
#[case("return(pending)", "Err(Kind(WouldBlock))")]
#[case(
    "return(cancelled)",
    "Err(Custom { kind: BrokenPipe, error: Cancelled })"
)]
#[tokio::test]
async fn test_try_get_reply(
    tempdir: tempfile::TempDir,
    #[case] fail_op: &str,
    #[case] expected_result: &str,
) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory.create_new_file(FileGroup::Pages).await.unwrap();
    let file = directory
        .get_rw_file(FileGroup::Pages, file_id)
        .await
        .unwrap();

    static SAMPLE: &[u8] = &[4; DISK_ALIGN];
    let reply = unsafe {
        file.submit_write(SAMPLE.as_ptr(), SAMPLE.len(), 0, None)
            .await
            .unwrap()
    };

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", fail_op).unwrap();

    let result = file::try_get_reply(&reply);
    assert_eq!(format!("{result:?}"), expected_result);

    scenario.teardown();
}

#[rstest::rstest]
#[case("return(50)", "Ok(50)")]
#[case(
    "return(-4)",
    "Err(Os { code: 4, kind: Interrupted, message: \"Interrupted system call\" })"
)]
#[case(
    "return(cancelled)",
    "Err(Custom { kind: BrokenPipe, error: Cancelled })"
)]
#[tokio::test]
async fn test_wait_for_reply(
    tempdir: tempfile::TempDir,
    #[case] fail_op: &str,
    #[case] expected_result: &str,
) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory.create_new_file(FileGroup::Pages).await.unwrap();
    let file = directory
        .get_rw_file(FileGroup::Pages, file_id)
        .await
        .unwrap();

    static SAMPLE: &[u8] = &[4; DISK_ALIGN];
    let reply = unsafe {
        file.submit_write(SAMPLE.as_ptr(), SAMPLE.len(), 0, None)
            .await
            .unwrap()
    };

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::poll_reply_future", fail_op).unwrap();
    fail::cfg("i2o2::fail::try_get_result", "return(pending)").unwrap();

    let result = file::wait_for_reply(reply).await;
    assert_eq!(format!("{result:?}"), expected_result);

    scenario.teardown();
}
