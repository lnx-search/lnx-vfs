pub use self::page_file::{CreatePageFileError, OpenPageFileError, PageFile};
use crate::directory::FileId;
use crate::layout::file_metadata::Encryption;
use crate::layout::{PageFileId, PageId};
mod page_file;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod utils;

/// The default size of individual pages of data.
pub const DISK_PAGE_SIZE: usize = 32 << 10;
/// The number of page "blocks" that make up a page file.
pub const NUM_BLOCKS_PER_FILE: usize = 30;
/// The number of 32KB pages that make up a block.
pub const NUM_PAGES_PER_BLOCK: usize = 16_384;
/// The maximum number of pages currently supported by the system.
///
/// This value plays into the storage allocator design.
pub const MAX_NUM_PAGES: usize = NUM_BLOCKS_PER_FILE * NUM_PAGES_PER_BLOCK;

/// The maximum number of pages a single IOP can affect.
pub const MAX_SINGLE_IOP_NUM_PAGES: usize = 8;
const CONTEXT_BUFFER_SIZE: usize = 40 * MAX_SINGLE_IOP_NUM_PAGES;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub(super) struct MetadataHeader {
    /// The unique ID of the file itself (as matches the file name.)
    pub(super) file_id: FileId,
    /// The unique ID of the page file.
    pub(super) page_file_id: PageFileId,
    /// Signals if the data in the log is encrypted or not.
    pub(super) encryption: Encryption,
    /// The number of maximum pages in the file.
    pub(super) max_num_pages: usize,
}

/// Computes the associated data to tag page file data with.
///
/// This method is used on all files and is used to prevent replay attacks
/// and a bad actor gaining information about the system by taking and swapping
/// around data in the files.
fn page_associated_data(
    file_id: FileId,
    page_file_id: PageFileId,
    page_id: PageId,
) -> [u8; 16] {
    let mut buffer = [0; 16];
    buffer[0..4].copy_from_slice(&file_id.as_u32().to_le_bytes());
    buffer[4..12].copy_from_slice(&page_id.0.to_le_bytes());
    buffer[12..16].copy_from_slice(&page_file_id.0.to_le_bytes());
    buffer
}
