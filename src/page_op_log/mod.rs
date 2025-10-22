//! The operations log tracks changes to pages in the data files.

#[cfg(all(test, not(miri), feature = "bench-lib-unstable"))]
mod benches;
mod reader;
#[cfg(all(test, not(miri)))]
mod tests;
mod writer;

use rand::RngCore;

pub use self::reader::{LogFileReader, LogOpenReadError};
pub use self::writer::{LogFileWriter, LogOpenWriteError};
use crate::directory::FileId;
use crate::layout::file_metadata::Encryption;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub struct MetadataHeader {
    /// The unique ID of the log file.
    ///
    /// NOTE: This ID changes every time the WAL is flushed, although the disk
    /// allocation stays the same, the file itself is seen as 'new'.
    pub(super) log_file_id: u64,
    /// The key used for ordering of the WAL files.
    ///
    /// NOTE: This key is only valid for sorting between files created within the same
    /// process and same runtime (i.e. restarts will break this guarantee.)
    pub(super) order_key: u64,
    /// Signals if the data in the log is encrypted or not.
    pub(super) encryption: Encryption,
}

/// Computes the associated data to tag file data with.
///
/// This method is used on all files and is used to prevent replay attacks
/// and a bad actor gaining information about the system by taking and swapping
/// around data in the files.
pub(super) fn op_log_associated_data(
    file_id: FileId,
    log_file_id: u64,
    sequence_id: u32,
    start_pos: u64,
) -> [u8; 24] {
    let mut buffer = [0; 24];
    buffer[0..4].copy_from_slice(&file_id.as_u32().to_le_bytes());
    buffer[4..12].copy_from_slice(&start_pos.to_le_bytes());
    buffer[12..20].copy_from_slice(&log_file_id.to_le_bytes());
    buffer[20..24].copy_from_slice(&sequence_id.to_le_bytes());
    buffer
}

pub(super) fn generate_random_log_id() -> u64 {
    let mut rng = rand::rng();
    rng.next_u64()
}
