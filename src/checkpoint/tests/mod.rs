use crate::layout::page_metadata::{PageChangeCheckpoint, PageMetadata};
use crate::layout::{PageGroupId, PageId};

mod e2e;
mod reader;
mod writer;

fn fill_updates(num_updates: usize) -> PageChangeCheckpoint {
    let mut updates = PageChangeCheckpoint::default();
    for update_id in 0..num_updates {
        let metadata = PageMetadata {
            group: PageGroupId(999),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            id: PageId(update_id as u32),
            data_len: 0,
            context: [0; 40],
        };
        updates.push(metadata);
    }
    updates
}
