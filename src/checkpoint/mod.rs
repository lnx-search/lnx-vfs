use crate::directory::FileId;
use crate::layout::file_metadata::Encryption;
use crate::layout::PageFileId;

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
}