use super::tempdir;
use crate::buffer::DmaBuffer;
use crate::directory::{FileGroup, SystemDirectory};

#[rstest::rstest]
#[tokio::test]
async fn test_file_read(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory.create_new_file(FileGroup::Pages).await.unwrap();

    let file_path = directory.resolve_file_path(FileGroup::Pages, file_id).await;
    std::fs::write(file_path, b"Hello, world!").unwrap();

    let ro_file = directory
        .get_ro_file(FileGroup::Pages, file_id)
        .await
        .expect("get ro file");

    let mut buffer = DmaBuffer::alloc_sys(1);
    let len = buffer.len();
    let n = ro_file
        .read_buffer(&mut buffer, len, 0)
        .await
        .expect("read buffer");
    assert_eq!(n, 13);
    assert_eq!(&buffer[..13], b"Hello, world!");
}

#[rstest::rstest]
#[tokio::test]
async fn test_file_write(tempdir: tempfile::TempDir) {
    let directory = SystemDirectory::open(tempdir.path())
        .await
        .expect("directory should be created");

    let file_id = directory.create_new_file(FileGroup::Pages).await.unwrap();

    let rw_file = directory
        .get_rw_file(FileGroup::Pages, file_id)
        .await
        .expect("get rw file");

    let mut buffer = DmaBuffer::alloc_sys(1);
    buffer[..13].copy_from_slice(b"Hello, world!");
    let n = rw_file
        .write_buffer(&mut buffer, 0)
        .await
        .expect("read buffer");
    assert_eq!(n, 4096);

    let file_path = directory.resolve_file_path(FileGroup::Pages, file_id).await;
    let buffer = std::fs::read(file_path).unwrap();
    assert_eq!(&buffer[..13], b"Hello, world!");
}
