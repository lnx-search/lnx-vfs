use crate::layout::PageId;
use crate::layout::page_metadata::PageMetadata;

pub(super) fn make_metadata_density_mask(metadata: &[PageMetadata]) -> u8 {
    assert!(metadata.len() <= 8);

    let mut mask = 0;
    mask |= 1 << 0;

    let mut offset = 1;
    let mut last_page_id = metadata[0].id.0;
    for metadata in metadata.iter().skip(1) {
        let current_page_id = metadata.id.0;
        let delta_from_expected = current_page_id - (last_page_id + 1);
        offset += delta_from_expected;
        mask |= 1 << offset;
        last_page_id = current_page_id;
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

        expected_next_page = current_page_id + 1;
    }

    context
}

pub(super) fn metadata_is_contiguous(metadata: &[PageMetadata]) -> bool {
    let mut last_page_id = PageId::TERMINATOR;
    for metadata in metadata {
        if last_page_id.is_terminator() {
            last_page_id = metadata.id;
            continue;
        }

        let expected_page_id = last_page_id.0 + 1;
        let actual_page_id = metadata.id.0;
        if actual_page_id != expected_page_id {
            return false;
        }
    }
    true
}
