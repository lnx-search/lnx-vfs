//! Attach integrity bytes to the start of a buffer and verify those bytes
//! when decoding a buffer.
//!
//! This system provides both HMAC & CRC32 integrity checks for encryption at REST being
//! enabled/disabled respectively.

use crate::layout::file_metadata::Encryption;

const INTEGRITY_HASH_SIZE: usize = 32;
const KEY_SIZE: usize = 32;

/// Verify the integrity of the buffer.
pub fn verify(
    mode: Encryption,
    authentication_key: Option<&[u8; KEY_SIZE]>,
    associated_data: &[u8],
    buffer: &[u8],
    context: &[u8],
) -> bool {
    match mode {
        Encryption::Disabled => verify_crc32_buffer(associated_data, buffer, context),
        Encryption::Enabled => verify_blake3_buffer(
            authentication_key.expect(
                "blake3 key should be provided when blake3 verification is enabled",
            ),
            associated_data,
            buffer,
            context,
        ),
    }
}

/// Prefix the given output buffer with a set of check bytes
/// containing either a blake3 keyed hash or CRC32 digest depending on
/// if a key is provided or not.
pub fn write_check_bytes(
    authentication_key: Option<&[u8; KEY_SIZE]>,
    associated_data: &[u8],
    input_buffer: &[u8],
    context: &mut [u8],
) {
    if let Some(key) = authentication_key {
        let computed_hash = blake3_hash_components(key, associated_data, input_buffer);
        context[..INTEGRITY_HASH_SIZE].copy_from_slice(computed_hash.as_bytes());
    } else {
        let result = crc32_hash_components(associated_data, input_buffer);
        context[..4].copy_from_slice(&result.to_le_bytes());
    }
}

/// Check the blake3 keyed hash at the start of the buffer aligns with the calculated keyed hash.
/// with the provided key.
///
/// Returns `false` if the HMAC could not be verified.
fn verify_blake3_buffer(
    hmac_key: &[u8; KEY_SIZE],
    associated_data: &[u8],
    buffer: &[u8],
    context: &[u8],
) -> bool {
    if context.len() < INTEGRITY_HASH_SIZE {
        return false;
    }

    let provided_hash = match context[..INTEGRITY_HASH_SIZE].try_into() {
        Ok(bytes) => blake3::Hash::from_bytes(bytes),
        Err(_) => return false,
    };

    let calculated_hash = blake3_hash_components(hmac_key, associated_data, buffer);

    #[cfg(debug_assertions)]
    {
        if provided_hash != calculated_hash {
            tracing::warn!(
                provided_hash = ?provided_hash,
                calculated_hash = ?calculated_hash,
                "buffer verification missmatch",
            );
        }
    }

    provided_hash == calculated_hash
}

/// Check the CRC32 checksum encoded in `context` with the provided `buffer` and
/// `associated_data`.
///
/// Returns `false` if the checksums did not match
fn verify_crc32_buffer(associated_data: &[u8], buffer: &[u8], context: &[u8]) -> bool {
    if context.len() < INTEGRITY_HASH_SIZE {
        return false;
    }

    let expected_checksum = u32::from_le_bytes(context[..4].try_into().unwrap());
    let actual_checksum = crc32_hash_components(associated_data, buffer);

    #[cfg(debug_assertions)]
    {
        if expected_checksum != actual_checksum {
            tracing::warn!(
                expected_checksum = ?expected_checksum,
                actual_checksum = ?actual_checksum,
                "buffer checksum missmatch",
            );
        }
    }

    actual_checksum == expected_checksum
}

fn blake3_hash_components(
    hmac_key: &[u8; KEY_SIZE],
    associated_data: &[u8],
    buffer: &[u8],
) -> blake3::Hash {
    let mut mac = blake3::Hasher::new_keyed(hmac_key);
    mac.update(associated_data);
    mac.update(buffer);
    mac.finalize()
}

fn crc32_hash_components(associated_data: &[u8], buffer: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(associated_data);
    hasher.update(buffer);
    hasher.finalize()
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::no_hmac(
        b"hello, world!".as_ref(), 
        None,
        &[
            19, 141, 152, 88,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        b"",
    )]
    #[case::hmac(
        b"hello, world!".as_ref(),
        Some(key1()),
        &[
            30, 55, 29, 223, 75, 225, 205, 232, 133, 29, 235, 172,
            17, 41, 186, 12, 159, 17, 136, 69, 183, 59, 235, 133, 95,
            77, 184, 206, 187, 164, 123, 78, 0, 0, 0, 0, 0, 0, 0, 0
        ],
        b"",
    )]
    fn test_write_check_bytes(
        #[case] input_buffer: &[u8],
        #[case] hmac_key: Option<[u8; KEY_SIZE]>,
        #[case] expected_ctx: &[u8],
        #[case] associated_data: &[u8],
    ) {
        let mut context = [0; 40];
        write_check_bytes(
            hmac_key.as_ref(),
            associated_data,
            input_buffer,
            &mut context,
        );
        assert_eq!(context, expected_ctx);
    }

    #[rstest::rstest]
    #[case::simple_matching_hmac(b"hello, world!", key1(), key1(), true)]
    #[case::simple_different_hmac(b"hello, world!", key1(), key2(), false)]
    #[case::empty_payload_matching_hmac(b"", key1(), key1(), true)]
    #[case::empty_payload_different_hmac(b"", key2(), key1(), false)]
    fn test_verify_hmac_buffer(
        #[case] input_buffer: &[u8],
        #[case] sign_hmac: [u8; KEY_SIZE],
        #[case] verify_hmac: [u8; KEY_SIZE],
        #[case] should_be_valid: bool,
        #[values(b"", b"sample-associated-data")] associated_data: &[u8],
    ) {
        let mut context = [0; 40];
        write_check_bytes(
            Some(&sign_hmac),
            associated_data,
            input_buffer,
            &mut context,
        );

        let verified =
            verify_blake3_buffer(&verify_hmac, associated_data, input_buffer, &context);
        assert_eq!(verified, should_be_valid);
    }

    #[rstest::rstest]
    #[case::simple_matching_crc(b"hello, world!", None, true)]
    #[case::simple_different_crc(b"hello, world!", Some(b"other".as_ref()), false)]
    #[case::empty_payload_matching_crc(b"", None, true)]
    #[case::empty_payload_different_crc(b"", Some(b"other".as_ref()), false)]
    fn test_verify_crc32_buffer(
        #[case] input_buffer: &[u8],
        #[case] overwrite_digest: Option<&[u8]>,
        #[case] should_be_valid: bool,
        #[values(b"", b"sample-associated-data")] associated_data: &[u8],
    ) {
        let mut context = [0; 40];
        write_check_bytes(None, associated_data, input_buffer, &mut context);

        if let Some(overwrite) = overwrite_digest {
            context[..overwrite.len()].copy_from_slice(overwrite);
        }

        let verified = verify_crc32_buffer(associated_data, input_buffer, &context);
        assert_eq!(verified, should_be_valid);
    }

    #[rstest::rstest]
    #[case::matching_associated_data(b"foo", b"foo", true)]
    #[case::different_associated_data(b"foo", b"bar", false)]
    fn test_associated_data_checked(
        #[case] encode_assoc_data: &[u8],
        #[case] decode_assoc_data: &[u8],
        #[case] expected_is_valid: bool,
        #[values(None, Some(key1()))] key: Option<[u8; KEY_SIZE]>,
    ) {
        let input_payload = b"example payload";
        let mut context = [0; 40];
        write_check_bytes(key.as_ref(), encode_assoc_data, input_payload, &mut context);

        let mode = if key.is_some() {
            Encryption::Enabled
        } else {
            Encryption::Disabled
        };

        let is_valid = verify(
            mode,
            key.as_ref(),
            decode_assoc_data,
            input_payload,
            &context,
        );
        assert_eq!(is_valid, expected_is_valid);
    }

    fn key1() -> [u8; KEY_SIZE] {
        blake3::derive_key("test1", &[1; 32])
    }

    fn key2() -> [u8; KEY_SIZE] {
        blake3::derive_key("test2", &[1; 32])
    }
}
