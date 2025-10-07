use crate::checkpoint::tests::fill_updates;
use crate::ctx;
use crate::directory::FileGroup;
use crate::layout::PageFileId;

#[rstest::rstest]
#[tokio::test]
async fn test_reader_can_read_writer_output(
    #[values(false, true)] encryption: bool,
    #[values(0, 1, 10, 400, 7423)] num_updates: usize,
) {
    let ctx = ctx::Context::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Metadata).await;

    let updates = fill_updates(num_updates);

    crate::checkpoint::write_checkpoint(&ctx, &file, PageFileId(1), updates.clone())
        .await
        .expect("could not write checkpoint");

    let checkpoint = crate::checkpoint::read_checkpoint(&ctx, &file.into())
        .await
        .expect("could not read checkpoint");
    assert_eq!(checkpoint.updates, updates);
    assert_eq!(checkpoint.page_file_id, PageFileId(1));
}
