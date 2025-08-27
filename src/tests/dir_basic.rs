use crate::directory::SystemDirectory;

#[rstest::rstest]
#[tokio::test]
async fn test_creation() {
    let _ = tracing_subscriber::fmt::try_init();

    let tempdir = tempfile::tempdir().expect("create temp dir");

    let _directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let path = tempdir.path();
    assert!(path.exists());
    assert!(path.join("data").exists());
    assert!(path.join("metadata").exists());
    assert!(path.join("wal").exists());
}