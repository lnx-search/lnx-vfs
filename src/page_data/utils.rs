use crate::layout::page_metadata::PageMetadata;

pub(super) fn make_metadata_density_mask(metadata: &[PageMetadata]) -> u8 {
    assert!(metadata.len() <= 8);

    let mut mask = 0;

    let mut offset = 0;
    let mut expected_next_page = metadata[0].id.0;
    for metadata in metadata {
        let current_page_id = metadata.id.0;
        let delta_from_expected = current_page_id - expected_next_page;
        offset += delta_from_expected;
        mask |= 1 << offset;

        offset += 1;
        expected_next_page = current_page_id + 1;
    }

    mask
}

pub(super) fn copy_sparse_metadata_context(
    metadata: &[PageMetadata],
) -> [u8; super::CONTEXT_BUFFER_SIZE] {
    assert!(metadata.len() <= 8);

    let mut context = [0u8; super::CONTEXT_BUFFER_SIZE];

    let mut offset = 0;
    let mut expected_next_page = metadata.first().unwrap().id.0;
    for metadata in metadata {
        let current_page_id = metadata.id.0;
        let delta_from_expected = current_page_id - expected_next_page;
        offset += delta_from_expected;

        let ctx_start = offset as usize * 40;
        let ctx_end = ctx_start + 40;
        context[ctx_start..ctx_end].copy_from_slice(&metadata.context);

        offset += 1;
        expected_next_page = current_page_id + 1;
    }

    context
}

pub(super) fn metadata_is_contiguous(metadata: &[PageMetadata]) -> bool {
    let mut next_page_id = metadata[0].id.0;
    for metadata in metadata {
        if metadata.id.0 != next_page_id {
            return false;
        }
        next_page_id += 1;
    }
    true
}
