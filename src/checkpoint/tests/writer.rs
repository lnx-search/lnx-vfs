use crate::checkpoint::tests::fill_updates;
use crate::checkpoint::{MetadataHeader, ckpt_associated_data};
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::{PageFileId, file_metadata, page_metadata};

#[rstest::rstest]
#[tokio::test]
async fn test_checkpoint_writer(
    #[values(false, true)] encryption: bool,
    #[values(0, 1, 10, 400, 7423)] num_updates: usize,
) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Metadata).await;

    let updates = fill_updates(num_updates);
    crate::checkpoint::write_checkpoint(&ctx, &file, PageFileId(1), updates)
        .await
        .expect("could not write checkpoint");

    let file_path = ctx
        .directory()
        .resolve_file_path(FileGroup::Metadata, file.id())
        .await;

    let mut written_data = std::fs::read(file_path).expect("could not read checkpoint");

    let header_associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Metadata);
    let header: MetadataHeader = file_metadata::decode_metadata(
        ctx.cipher(),
        &header_associated_data,
        &mut written_data[..file_metadata::HEADER_SIZE],
    )
    .expect("valid header should be written");

    assert_eq!(header.file_id, file.id());
    assert_eq!(header.parent_page_file_id, PageFileId(1));
    assert_eq!(header.encryption, ctx.get_encryption_status());

    let ckpt_associated_data = ckpt_associated_data(
        file.id(),
        header.parent_page_file_id,
        header.checkpoint_num_changes,
        file_metadata::HEADER_SIZE as u64,
    );

    let checkpoint = page_metadata::decode_page_metadata_changes(
        ctx.cipher(),
        &ckpt_associated_data,
        &mut written_data[file_metadata::HEADER_SIZE..][..header.checkpoint_buffer_size],
    )
    .expect("checkpoint should be able to be decoded");
    assert_eq!(checkpoint.len() as u32, header.checkpoint_num_changes);
}
