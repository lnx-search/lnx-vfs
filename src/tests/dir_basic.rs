use crate::directory::{FileGroup, FileId, SystemDirectory};

#[rstest::fixture]
fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create temp dir")
}

#[rstest::rstest]
#[tokio::test]
async fn test_creation(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let path = tempdir.path();
    assert!(path.exists());
    assert!(path.join("pages").exists());
    assert!(path.join("metadata").exists());
    assert!(path.join("wal").exists());

    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 0);
    assert_eq!(directory.num_open_files(FileGroup::Metadata).await, 0);
    assert_eq!(directory.num_open_files(FileGroup::Wal).await, 0);
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_new_file(
    tempdir: tempfile::TempDir,
    #[values(FileGroup::Pages, FileGroup::Metadata, FileGroup::Wal)] group: FileGroup,
) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(group)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);

    // A complete hack to get the assertions on each group.
    assert_eq!(
        directory.num_open_files(FileGroup::Pages).await,
        (group == FileGroup::Pages) as usize
    );
    assert_eq!(
        directory.num_open_files(FileGroup::Metadata).await,
        (group == FileGroup::Metadata) as usize
    );
    assert_eq!(
        directory.num_open_files(FileGroup::Wal).await,
        (group == FileGroup::Wal) as usize
    );

    let _file = directory
        .get_ro_file(group, file_id)
        .await
        .expect("file should exist and be available");

    let _file = directory
        .get_rw_file(group, file_id)
        .await
        .expect("file should exist and be available");

    let path = tempdir.path().join(group.folder_name());

    let files = super::list_files(&path).unwrap();
    if group == FileGroup::Pages {
        assert_eq!(&files, &["0000001000-1000.dat.lnx"]);
    } else if group == FileGroup::Metadata {
        assert_eq!(&files, &["0000001000-1000.pts.lnx"]);
    } else if group == FileGroup::Wal {
        assert_eq!(&files, &["0000001000-1000.wal.lnx"]);
    }
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_remove_file(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");
}

#[rstest::rstest]
#[tokio::test]
async fn test_open_existing_files(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");
}
