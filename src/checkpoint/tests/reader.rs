use std::io::Write;

use crate::checkpoint::tests::fill_updates;
use crate::checkpoint::{MetadataHeader, ckpt_associated_data};
use crate::directory::FileGroup;
use crate::layout::{PageFileId, file_metadata, page_metadata};
use crate::{ctx, file};

#[rstest::rstest]
#[tokio::test]
async fn test_checkpoint_reader(
    #[values(false, true)] encryption: bool,
    #[values(0, 1, 10, 400, 7423)] num_updates: usize,
) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file: file::ROFile = ctx.make_tmp_rw_file(FileGroup::Metadata).await.into();

    let updates = fill_updates(num_updates);
    let associated_data = ckpt_associated_data(
        file.id(),
        PageFileId(1),
        num_updates as u32,
        file_metadata::HEADER_SIZE as u64,
    );
    let ckpt_buffer = page_metadata::encode_page_metadata_changes(
        ctx.cipher(),
        &associated_data,
        &updates,
    )
    .unwrap();

    let header_associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Metadata);
    let header = MetadataHeader {
        file_id: file.id(),
        parent_page_file_id: PageFileId(1),
        encryption: ctx.get_encryption_status(),
        checkpoint_buffer_size: ckpt_buffer.len(),
        checkpoint_num_changes: num_updates as u32,
    };

    let mut header_buffer = vec![0; file_metadata::HEADER_SIZE];
    file_metadata::encode_metadata(
        ctx.cipher(),
        &header_associated_data,
        &header,
        &mut header_buffer[..],
    )
    .unwrap();

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Metadata, file.id())
        .await;

    let mut raw_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    raw_file.write_all(&header_buffer).unwrap();
    raw_file.write_all(&ckpt_buffer).unwrap();
    raw_file.sync_all().unwrap();

    let checkpoint = crate::checkpoint::read_checkpoint(&ctx, &file)
        .await
        .expect("could not write checkpoint");
    assert_eq!(checkpoint.len(), num_updates);
}
