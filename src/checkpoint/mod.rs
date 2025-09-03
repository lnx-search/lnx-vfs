mod reader;
mod tests;
mod writer;

pub use self::reader::{ReadCheckpointError, read_checkpoint};
pub use self::writer::{WriteCheckpointError, write_checkpoint};
use crate::directory::FileId;
use crate::layout::PageFileId;
use crate::layout::file_metadata::Encryption;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub(super) struct MetadataHeader {
    /// The unique ID of the file itself (as matches the file name.)
    pub(super) file_id: FileId,
    /// The unique ID of the log file.
    ///
    /// NOTE: This ID changes every time the WAL is flushed, although the disk
    /// allocation stays the same, the file itself is seen as 'new'.
    pub(super) parent_page_file_id: PageFileId,
    /// Signals if the data in the log is encrypted or not.
    pub(super) encryption: Encryption,
    /// The size of the checkpoint buffer.
    pub(super) checkpoint_buffer_size: usize,
}

/// Computes the associated data to tag checkpoint file data with.
///
/// This method is used on all files and is used to prevent replay attacks
/// and a bad actor gaining information about the system by taking and swapping
/// around data in the files.
pub(super) fn ckpt_associated_data(
    file_id: FileId,
    target_page_file_id: PageFileId,
    start_pos: u64,
) -> [u8; 16] {
    let mut buffer = [0; 16];
    buffer[0..4].copy_from_slice(&file_id.as_u32().to_le_bytes());
    buffer[4..12].copy_from_slice(&start_pos.to_le_bytes());
    buffer[12..16].copy_from_slice(&target_page_file_id.0.to_le_bytes());
    buffer
}
