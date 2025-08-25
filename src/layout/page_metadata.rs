use std::io::{Cursor, Write};
use std::ops::{Deref, DerefMut};

use rkyv::rancor;
use rkyv::ser::writer::IoWriter;
use rkyv::util::AlignedVec;

use crate::layout::{PageFileId, PageGroupId, PageId, encrypt};

const ENTRIES_PER_BLOCK: usize = 63;
const EXPECTED_BUFFER_SIZE: usize = 4 << 10;
pub const PAGE_BLOCK_SIZE: usize = 4096;

#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[rkyv(derive(Debug))]
/// Metadata about the page and the ata stored within it when serialized on disk.
pub struct PageMetadata {
    /// The block this page contains data for.
    pub(crate) group: PageGroupId,
    /// The [PageFileId] that the next page is located at.
    pub(crate) next_page_file_id: PageFileId,
    /// The [PageId] of the next that is part of the group.
    pub(crate) next_page_id: PageId,
    /// The ID of the page.
    pub(crate) id: PageId,
    /// The length of the buffer within the page.
    pub(crate) data_len: u32,
    /// Context bytes used for decrypting the page data.
    pub(crate) context: [u8; 40],
}

impl PageMetadata {
    pub(crate) fn is_empty(&self) -> bool {
        self.id.is_terminator() && self.group == PageGroupId(u64::MAX)
    }

    /// Creates a new [PageMetadata] entry representing an empty page.
    pub(crate) const fn empty() -> Self {
        Self {
            id: PageId::TERMINATOR,
            group: PageGroupId(u64::MAX),
            next_page_file_id: PageFileId(0),
            next_page_id: PageId::TERMINATOR,
            data_len: 0,
            context: [0; 40],
        }
    }
}

#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[rkyv(derive(Debug))]
/// The page metadata updates, this may be partial or a complete
/// rewrite of the table state.
pub struct PageMetadataUpdates(#[rkyv(with = rkyv::with::AsBox)] Vec<PageMetadata>);

impl Deref for PageMetadataUpdates {
    type Target = Vec<PageMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PageMetadataUpdates {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, thiserror::Error)]
/// The provided buffer is too small to serialize the block.
pub enum EncodeError {
    #[error("failed to compress data: {0}")]
    /// The data could not be compressed with LZ4.
    CompressError(#[from] lz4_flex::frame::Error),
    #[error("failed to encrypt data")]
    /// The data could not be encrypted.
    EncryptionFail,
}

/// Encode a set of page metadata updates.
///
/// Metadata is automatically compressed with LZ4 compression.
pub fn encode_page_metadata_updates(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    entries: &PageMetadataUpdates,
) -> Result<Vec<u8>, EncodeError> {
    const HEADER_SIZE: usize = 44;

    let mut buffer =
        Vec::with_capacity(HEADER_SIZE + entries.len() * size_of::<PageMetadata>());
    buffer.extend_from_slice(&[0; HEADER_SIZE]);

    let mut compressor = lz4_flex::frame::FrameEncoder::new(buffer);
    let mut writer = IoWriter::new(compressor);
    writer = rkyv::api::high::to_bytes_in::<_, rancor::Panic>(entries, writer).unwrap();
    compressor = writer.into_inner();
    compressor.flush().unwrap();
    buffer = compressor.finish()?;

    let indices = [0..40, 40..44, 44..buffer.len()];
    let [context, checksum_bytes, data] = buffer.get_disjoint_mut(indices).unwrap();

    let checksum = crc32fast::hash(data);
    checksum_bytes.copy_from_slice(&checksum.to_le_bytes());

    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, associated_data, data, context)
            .map_err(|_| EncodeError::EncryptionFail)?;
    }

    Ok(buffer)
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a block of metadata pages.
pub enum DecodeError {
    #[error("provided buffer length is incorrect")]
    /// The provided buffer is too small.
    IncorrectBufferSize,
    #[error("failed to decrypt data")]
    /// The data could not be decrypted.
    DecryptionFailed,
    #[error("failed to decompress data: {0}")]
    /// The data could not be decompressed.
    DecompressionFailed(String),
    #[error("metadata corrupted")]
    /// The data was corrupted
    Corrupted,
    #[error("{0}")]
    /// The payload data is malformed and could not be deserialized.
    Deserialize(rancor::Error),
}

/// Decode the set of page metadata updates encoded within the buffer.
///
/// The update data should be located at the start of the buffer.
///
/// The provided buffer should be 4KB in size.
pub fn decode_page_metadata_updates(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
) -> Result<PageMetadataUpdates, DecodeError> {
    const HEADER_SIZE: usize = 48;

    if buffer.len() < HEADER_SIZE {
        return Err(DecodeError::IncorrectBufferSize);
    }

    let indices = [0..40, 40..44, 44..buffer.len()];
    let [context, checksum_bytes, data] = buffer.get_disjoint_mut(indices).unwrap();

    if let Some(cipher) = cipher {
        encrypt::decrypt_in_place(cipher, associated_data, data, context)
            .map_err(|_| DecodeError::DecryptionFailed)?;
    }

    let actual_checksum = crc32fast::hash(data);
    let expected_checksum = u32::from_le_bytes(checksum_bytes.try_into().unwrap());

    if actual_checksum != expected_checksum {
        return Err(DecodeError::Corrupted);
    }

    let size_hint = data.len();
    let mut decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(data));
    let mut buffer: AlignedVec<{ align_of::<PageMetadataUpdates>() }> =
        AlignedVec::with_capacity(size_hint);
    buffer
        .extend_from_reader(&mut decoder)
        .map_err(|e| DecodeError::DecompressionFailed(e.to_string()))?;

    let view: &rkyv::Archived<PageMetadataUpdates> =
        rkyv::access(&buffer).map_err(DecodeError::Deserialize)?;

    rkyv::deserialize(view).map_err(DecodeError::Deserialize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_metadata_size() {
        assert_eq!(size_of::<PageMetadata>(), 64);
        assert_eq!(size_of::<ArchivedPageMetadata>(), 64);
    }
}
