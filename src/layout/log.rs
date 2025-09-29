//! The page operation log is a WAL-like system which holds the last
//! operations that occurred to the page file since the last checkpoint.

use lz4_flex::frame::{BlockMode, BlockSize};
use rkyv::rancor;
use rkyv::util::AlignedVec;

use super::encrypt::EncryptError;
use super::file_metadata::Encryption;
use super::page_metadata::PageMetadata;
use super::{PageFileId, PageGroupId, encrypt, integrity};
use crate::utils;

/// The required header size of the log block buffer.
pub const HEADER_SIZE: usize =
    size_of::<u64>() + size_of::<u64>() + size_of::<[u8; 40]>();
const BUFFER_ALIGN: usize = align_of::<Vec<LogOp>>();
const DISK_ALIGN: usize = 512;

/// Try to decode a set of transaction operations from the provided buffer.
///
/// This will decrypt the  buffer if `cipher` is provided or attempt to
/// check the CRC32 checksum check depending on if not.
pub fn decode_log_block(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
    ops: &mut Vec<LogOp>,
) -> Result<(), DecodeLogBlockError> {
    if buffer.len() < 40 {
        return Err(DecodeLogBlockError::BufferTooSmall);
    }

    let slice = [0..40, 40..buffer.len()];
    let [data_ctx, data_slice] = buffer.get_disjoint_mut(slice).unwrap();
    decrypt_or_integrity_check(cipher, associated_data, data_slice, data_ctx)?;

    let mut decrypted_buffer: &[u8] = data_slice;
    let mut data_buffer =
        AlignedVec::<BUFFER_ALIGN>::with_capacity(decrypted_buffer.len());
    let mut decoder = lz4_flex::frame::FrameDecoder::new(&mut decrypted_buffer);
    data_buffer
        .extend_from_reader(&mut decoder)
        .map_err(|e| DecodeLogBlockError::Decompress(e.to_string()))?;

    let view: &rkyv::Archived<Vec<LogOp>> =
        rkyv::access::<_, rancor::Error>(&data_buffer)
            .map_err(DecodeLogBlockError::Deserialize)?;

    for op in view.iter() {
        let op = rkyv::deserialize::<_, rancor::Error>(op)
            .map_err(DecodeLogBlockError::Deserialize)?;
        ops.push(op);
    }

    Ok(())
}

/// Decode the header of the log block.
pub fn decode_log_header(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
) -> Result<(u64, usize), DecodeLogBlockError> {
    if buffer.len() < HEADER_SIZE {
        return Err(DecodeLogBlockError::BufferTooSmall);
    }

    let header_bytes = &mut buffer[..HEADER_SIZE];
    let [header_slice, header_ctx] = header_bytes
        .get_disjoint_mut([0..16, 16..HEADER_SIZE])
        .unwrap();
    decrypt_or_integrity_check(cipher, associated_data, header_slice, header_ctx)?;

    let transaction_id = u64::from_le_bytes(header_slice[0..8].try_into().unwrap());
    let buffer_len =
        u64::from_le_bytes(header_slice[8..16].try_into().unwrap()) as usize;

    Ok((transaction_id, buffer_len))
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a log entry.
pub enum DecodeLogBlockError {
    #[error("buffer too small")]
    /// The provided buffer is too small to have a valid block in it.
    BufferTooSmall,
    #[error("buffer decryption failed")]
    /// The buffer could not be decrypted
    DecryptionFail,
    #[error("buffer verification failed")]
    /// The buffer could not be verified for integrity
    VerificationFail,
    #[error("decompress error: {0}")]
    /// The system could not decompress the log entry.
    Decompress(String),
    #[error("deserialize error: {0}")]
    /// The system could not parse and deserialize the log entry.
    Deserialize(rancor::Error),
}

/// Serializes, compressed and writes a set of transaction operations into the provided buffer.
pub fn encode_log_block(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    transaction_id: u64,
    ops: &Vec<LogOp>,
    mut buffer: &mut Vec<u8>,
) -> Result<usize, EncodeLogBlockError> {
    use rkyv::api::high;
    use rkyv::ser::writer::IoWriter;

    assert!(buffer.is_empty(), "buffer must be empty");

    // Add the header bytes
    buffer.resize(HEADER_SIZE + 40, 0);

    let frame_info = lz4_flex::frame::FrameInfo::new()
        .block_mode(BlockMode::Linked)
        .block_size(BlockSize::Max1MB)
        .content_checksum(false)
        .block_checksums(false);

    let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(frame_info, buffer);
    let mut writer = IoWriter::new(encoder);
    writer = high::to_bytes_in::<_, rancor::Error>(ops, writer)
        .map_err(EncodeLogBlockError::Serialize)?;
    encoder = writer.into_inner();
    buffer = encoder.finish().map_err(EncodeLogBlockError::Compression)?;

    if buffer.len() % 512 != 0 {
        let aligned_len = utils::align_up(buffer.len(), 512);
        buffer.resize(aligned_len, 0);
    }

    let buffer_len = buffer.len() - HEADER_SIZE;
    buffer[0..8].copy_from_slice(&transaction_id.to_le_bytes());
    buffer[8..16].copy_from_slice(&buffer_len.to_le_bytes());

    let [header_slice, header_ctx] =
        buffer.get_disjoint_mut([0..16, 16..HEADER_SIZE]).unwrap();
    encrypt_or_integrity_encode(cipher, associated_data, header_slice, header_ctx)
        .map_err(EncodeLogBlockError::EncryptionFail)?;

    let data_buffer = &mut buffer[HEADER_SIZE..];
    let slice = [0..40, 40..data_buffer.len()];
    let [data_ctx, data_slice] = data_buffer.get_disjoint_mut(slice).unwrap();
    encrypt_or_integrity_encode(cipher, associated_data, data_slice, data_ctx)
        .map_err(EncodeLogBlockError::EncryptionFail)?;

    Ok(buffer.len())
}

fn encrypt_or_integrity_encode(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
    context: &mut [u8],
) -> Result<(), EncryptError> {
    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, associated_data, buffer, context)
    } else {
        integrity::write_check_bytes(None, associated_data, buffer, context);
        Ok(())
    }
}

fn decrypt_or_integrity_check(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
    context: &mut [u8],
) -> Result<(), DecodeLogBlockError> {
    if let Some(cipher) = cipher {
        encrypt::decrypt_in_place(cipher, associated_data, buffer, context)
            .map_err(|_| DecodeLogBlockError::DecryptionFail)
    } else {
        let is_ok = integrity::verify(
            Encryption::Disabled,
            None,
            associated_data,
            buffer,
            context,
        );
        if is_ok {
            Ok(())
        } else {
            Err(DecodeLogBlockError::VerificationFail)
        }
    }
}

#[derive(Debug, thiserror::Error)]
/// The log entry could not be encoded and written to the buffer.
pub enum EncodeLogBlockError {
    #[error("serialize error: {0}")]
    /// Rkyv failed to serialize the entry.
    ///
    /// This should always be infallible, but we avoid panicking.
    Serialize(rancor::Error),
    #[error("compression error: {0}")]
    /// The serialized buffer could not be compressed.
    Compression(lz4_flex::frame::Error),
    #[error("failed to encrypt data: {0}")]
    /// The data could not be encrypted.
    EncryptionFail(EncryptError),
}

#[repr(u32)]
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug), compare(PartialEq)))]
pub enum LogOp {
    /// A new write performed on the page.
    Write(WriteOp),
    /// The page has been freed and can be reused.
    Free(FreeOp),
    /// The collection of existing pages have be reassigned from one-page group
    /// to another.
    Reassign(ReassignOp),
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug), compare(PartialEq)))]
/// An update to a targe page group.
pub struct WriteOp {
    /// The page file ID that the change was applied to.
    pub page_file_id: PageFileId,
    /// The page group that is assigned the altered pages.
    ///
    /// This must match the group IDs attached to the pages.
    pub page_group_id: PageGroupId,
    /// The pages altered by the write operation.
    pub altered_pages: Vec<PageMetadata>,
}

#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug), compare(PartialEq)))]
/// A free op removes/unassigns a set of pages associated with a given
/// page group ID.
pub struct FreeOp {
    /// The page group that was freed.
    pub page_group_id: PageGroupId,
}

#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug), compare(PartialEq)))]
/// Reassign a set pages from one group to another.
pub struct ReassignOp {
    /// The old page group ID.
    pub old_page_group_id: PageGroupId,
    /// The new page group ID.
    pub new_page_group_id: PageGroupId,
}
