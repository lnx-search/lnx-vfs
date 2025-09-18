use std::ops::{Deref, DerefMut};

use rkyv::rancor;
use rkyv::util::AlignedVec;

use crate::layout::file_metadata::Encryption;
use crate::layout::{PageGroupId, PageId, encrypt, integrity};

const HEADER_SIZE: usize = 44;
type AlignedBuffer = AlignedVec<{ align_of::<PageChangeCheckpoint>() }>;

#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[rkyv(derive(Debug))]
/// Metadata about the page and the data stored within it when serialized on disk.
pub struct PageMetadata {
    /// The block this page contains data for.
    pub(crate) group: PageGroupId,
    /// Used to determine if a page is newer than another when assigned the same group ID.
    pub(crate) revision: u32,
    /// The [PageId] of  next that is part of the group.
    ///
    /// This ID must be a higher logical ID than the page's own ID.
    pub(crate) next_page_id: PageId,
    /// The ID of the page.
    pub(crate) id: PageId,
    /// The length of the buffer within the page.
    pub(crate) data_len: u32,
    /// Context bytes used for decrypting the page data.
    ///
    /// Alternatively, if encryption is disabled, this contains the crc32 checksum.
    pub(crate) context: [u8; 40],
}

impl PageMetadata {
    /// Returns if the page is considered empty/unassigned or not.
    pub(crate) const fn is_unassigned(&self) -> bool {
        self.group.is_null()
    }

    /// Returns if the page is considered null.
    pub(crate) const fn is_null(&self) -> bool {
        self.id.is_terminator()
    }

    /// Creates a new [PageMetadata] entry representing an empty page with no assigned
    /// information.
    pub(crate) const fn null() -> Self {
        Self {
            id: PageId::TERMINATOR,
            group: PageGroupId::NULL,
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            data_len: 0,
            context: [0; 40],
        }
    }

    /// Creates a new [PageMetadata] entry representing an empty page with an assigned
    /// [PageId].
    ///
    /// This differs from [PageMetadata::null] as the page ID is assigned and is effectively
    /// a "valid" page to be used within the system metadata tables.
    pub(crate) const fn empty(page_id: PageId) -> Self {
        Self {
            id: page_id,
            group: PageGroupId(u64::MAX),
            revision: 0,
            next_page_id: PageId::TERMINATOR,
            data_len: 0,
            context: [0; 40],
        }
    }
}

#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq, Clone))]
#[rkyv(derive(Debug))]
/// The page metadata updates, this may be partial or a complete
/// rewrite of the table state.
pub struct PageChangeCheckpoint(#[rkyv(with = rkyv::with::AsBox)] Vec<PageMetadata>);

impl PageChangeCheckpoint {
    /// Creates a new [PageChangeCheckpoint] with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
}

impl Deref for PageChangeCheckpoint {
    type Target = Vec<PageMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PageChangeCheckpoint {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, thiserror::Error)]
/// The provided buffer is too small to serialize the block.
pub enum EncodeError {
    #[error("failed to compress data: {0}")]
    /// The data could not be compressed with LZ4.
    CompressError(#[from] lz4_flex::block::CompressError),
    #[error("failed to encrypt data")]
    /// The data could not be encrypted.
    EncryptionFail,
}

/// Encode a set of page metadata updates.
///
/// Metadata is automatically compressed with LZ4 compression.
pub fn encode_page_metadata_changes(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    entries: &PageChangeCheckpoint,
) -> Result<Vec<u8>, EncodeError> {
    let serialized = rkyv::api::high::to_bytes::<rancor::Panic>(entries).unwrap();
    let original_size = serialized.len() as u32;

    let max_compressed_size = lz4_flex::block::get_maximum_output_size(serialized.len());
    let mut output_buffer = vec![0; max_compressed_size + HEADER_SIZE];

    let compression_buffer = &mut output_buffer[40..];
    compression_buffer[..4].copy_from_slice(&original_size.to_le_bytes());
    let n = lz4_flex::block::compress_into(&serialized, &mut compression_buffer[4..])?;
    output_buffer.truncate(HEADER_SIZE + n);

    let indices = [0..40, 40..output_buffer.len()];
    let [context, data] = output_buffer.get_disjoint_mut(indices).unwrap();

    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, associated_data, data, context)
            .map_err(|_| EncodeError::EncryptionFail)?;
    } else {
        integrity::write_check_bytes(None, associated_data, data, context);
    }

    Ok(output_buffer)
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a block of metadata pages.
pub enum DecodeError {
    #[error("provided buffer length is incorrect")]
    /// The provided buffer is too small.
    IncorrectBufferSize,
    #[error("buffer decryption failed")]
    /// The buffer could not be decrypted
    DecryptionFail,
    #[error("failed to decompress data: {0}")]
    /// The data could not be decompressed.
    DecompressionFailed(#[from] lz4_flex::block::DecompressError),
    #[error("buffer verification failed")]
    /// The buffer could not be verified for integrity
    VerificationFail,
    #[error("{0}")]
    /// The payload data is malformed and could not be deserialized.
    Deserialize(rancor::Error),
}

/// Decode the set of page metadata updates encoded within the buffer.
///
/// The update data should be located at the start of the buffer.
///
/// The provided buffer should be 4KB in size.
pub fn decode_page_metadata_changes(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
) -> Result<PageChangeCheckpoint, DecodeError> {
    if buffer.len() < HEADER_SIZE {
        return Err(DecodeError::IncorrectBufferSize);
    }

    let indices = [0..40, 40..buffer.len()];
    let [context, data] = buffer.get_disjoint_mut(indices).unwrap();

    if let Some(cipher) = cipher {
        encrypt::decrypt_in_place(cipher, associated_data, data, context)
            .map_err(|_| DecodeError::DecryptionFail)?;
    } else {
        let verified = integrity::verify(
            Encryption::Disabled,
            None,
            associated_data,
            data,
            context,
        );
        if !verified {
            return Err(DecodeError::VerificationFail);
        }
    }

    let compressed_buffer = &buffer[40..];
    let uncompressed_size =
        u32::from_le_bytes(compressed_buffer[..4].try_into().unwrap());

    let mut aligned = AlignedBuffer::with_capacity(uncompressed_size as usize);
    aligned.resize(uncompressed_size as usize, 0);
    lz4_flex::decompress_into(&compressed_buffer[4..], &mut aligned)?;

    let view: &rkyv::Archived<PageChangeCheckpoint> =
        rkyv::access(&aligned).map_err(DecodeError::Deserialize)?;

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
