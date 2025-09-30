use std::io::ErrorKind;

use super::tempdir;
use crate::directory::{FileGroup, SystemDirectory};

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
async fn test_creation_not_a_directory_err() {
    let file = tempfile::NamedTempFile::new().unwrap();

    let err = SystemDirectory::open(file.path())
        .await
        .expect_err("directory should not be created");
    assert_eq!(err.kind(), ErrorKind::NotADirectory);
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_new_file_group_behaviour(
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

    let file = directory
        .get_ro_file(group, file_id)
        .await
        .expect("file should exist and be available");
    assert_eq!(file.id().as_u32(), 1000);

    let file = directory
        .get_rw_file(group, file_id)
        .await
        .expect("file should exist and be available");
    assert_eq!(file.id().as_u32(), 1000);

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
async fn test_create_new_file_incrementing_id(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);
    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1001);
    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1002);
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_new_file_already_exists_error(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let sample_file_path = tempdir
        .path()
        .join(FileGroup::Pages.folder_name())
        .join("0000001000-1000.dat.lnx");
    std::fs::write(&sample_file_path, b"Hello, world!").unwrap();

    let err = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect_err("create new file in group");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_remove_file(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);

    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 1);

    let group_path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = super::list_files(&group_path).unwrap();
    assert_eq!(files.len(), 1);

    directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect("remove file in group");
    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 0);

    let group_path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = super::list_files(&group_path).unwrap();
    assert_eq!(files.len(), 0);
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_remove_non_existing_files(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);
    directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect("remove file in group");
    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 0);

    directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect("ignore missing file");
}

#[rstest::rstest]
#[tokio::test]
async fn test_create_remove_file_still_in_use(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);

    let file = directory
        .get_ro_file(FileGroup::Pages, file_id)
        .await
        .expect("file should exist and be available");

    let err = directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect_err("file removal should error as file still in use");
    assert_eq!(err.kind(), ErrorKind::ResourceBusy);

    drop(file);

    directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect("remove file in group");
    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 0);
}

#[rstest::rstest]
#[tokio::test]
async fn test_open_existing_files(tempdir: tempfile::TempDir) {
    for group in [FileGroup::Pages, FileGroup::Metadata, FileGroup::Wal] {
        let group_path = tempdir.path().join(group.folder_name());
        std::fs::create_dir(group_path).unwrap();
    }

    let group_path = tempdir.path().join(FileGroup::Pages.folder_name());
    let sample_file_path = group_path.join("0000001000-1000.dat.lnx");
    std::fs::write(&sample_file_path, b"Hello, world!").unwrap();
    let sample_file_path = group_path.join("0000002000-2000.dat.lnx");
    std::fs::write(&sample_file_path, b"Hello, world!").unwrap();
    let sample_file_path = group_path.join("0000002000-3000.dat.lnx");
    std::fs::write(&sample_file_path, b"Hello, world!").unwrap();

    let group_path = tempdir.path().join(FileGroup::Wal.folder_name());
    let sample_file_path = group_path.join("0000001000-1000.wal.lnx");
    std::fs::write(&sample_file_path, b"Hello, world!").unwrap();

    let group_path = tempdir.path().join(FileGroup::Metadata.folder_name());
    let sample_file_path = group_path.join("0000001000-1000.pts.lnx");
    std::fs::write(&sample_file_path, b"Hello, world!").unwrap();

    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    assert_eq!(directory.num_open_files(FileGroup::Pages).await, 3);
    assert_eq!(directory.num_open_files(FileGroup::Metadata).await, 1);
    assert_eq!(directory.num_open_files(FileGroup::Wal).await, 1);
}

#[rstest::rstest]
#[tokio::test]
async fn test_list_dir(tempdir: tempfile::TempDir, #[values(0, 1, 13)] num_files: u32) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    for id in 0..num_files {
        let file_id = directory
            .create_new_file(FileGroup::Pages)
            .await
            .expect("create new file in group");
        assert_eq!(file_id.as_u32(), 1000 + id);
    }

    let files = directory.list_dir(FileGroup::Pages).await;
    assert_eq!(files.len(), num_files as usize);
}

#[rstest::rstest]
#[tokio::test]
async fn test_file_not_found(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);
    directory
        .remove_file(FileGroup::Pages, file_id)
        .await
        .expect("remove file in group");

    let error = directory
        .get_ro_file(FileGroup::Pages, file_id)
        .await
        .expect_err("file does not exist and should error");
    assert_eq!(error.kind(), ErrorKind::NotFound);

    let error = directory
        .get_rw_file(FileGroup::Pages, file_id)
        .await
        .expect_err("file does not exist and should error");
    assert_eq!(error.kind(), ErrorKind::NotFound);
}

#[rstest::rstest]
#[tokio::test]
async fn test_atomic_file(tempdir: tempfile::TempDir) {
    let _ = tracing_subscriber::fmt::try_init();

    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory
        .create_new_atomic_file(FileGroup::Pages)
        .await
        .expect("create new file in group");
    assert_eq!(file_id.as_u32(), 1000);

    let file = directory
        .get_ro_file(FileGroup::Pages, file_id)
        .await
        .expect("file should exist and be available");
    assert_eq!(file.id().as_u32(), 1000);

    let path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = super::list_files(&path).unwrap();
    assert_eq!(files, &["0000001000-1000.dat.lnx.atomic"]);

    directory
        .persist_atomic_file(FileGroup::Pages, file_id)
        .await
        .expect("atomic file should be persisted");

    let path = tempdir.path().join(FileGroup::Pages.folder_name());
    let files = super::list_files(&path).unwrap();
    assert_eq!(files, &["0000001000-1000.dat.lnx"]);
}

#[rstest::rstest]
#[tokio::test]
async fn test_directory_fmt_debug(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");
    assert_eq!(
        format!("{directory:?}"),
        format!("SystemDirectory(path={})", tempdir.path().display())
    );
}
