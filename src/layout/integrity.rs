//! Attach integrity bytes to the start of a buffer and verify those bytes
//! when decoding a buffer.
//!
//! This system provides both HMAC & CRC32 integrity checks for encryption at REST being
//! enabled/disabled respectively.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::layout::file_metadata::Encryption;

type HmacSha256 = Hmac<Sha256>;
const INTEGRITY_PREFIX_SIZE: usize = 32;

/// Verify the integrity of the buffer.
pub fn verify(
    mode: Encryption,
    hmac_key: Option<&[u8]>,
    associated_data: &[u8],
    buffer: &[u8],
    context: &[u8],
) -> bool {
    match mode {
        Encryption::Disabled => verify_crc32_buffer(associated_data, buffer, context),
        Encryption::Enabled => verify_hmac_buffer(
            hmac_key
                .expect("HMAC key should be provided when HMAC verification is enabled"),
            associated_data,
            buffer,
            context,
        ),
    }
}

/// Prefix the given output buffer with a set of check bytes
/// containing either a HMAC or CRC32 digest depending on
/// if a HMAC key is provided or not.
pub fn write_check_bytes(
    hmac_key: Option<&[u8]>,
    associated_data: &[u8],
    input_buffer: &[u8],
    context: &mut [u8],
) {
    if let Some(hmac_key) = hmac_key {
        let mac = hmac_hash_components(hmac_key, associated_data, input_buffer);
        let result = mac.finalize().into_bytes();
        context[..32].copy_from_slice(result.as_slice());
    } else {
        let result = crc32_hash_components(associated_data, input_buffer);
        context[..4].copy_from_slice(&result.to_le_bytes());
    }
}

/// Check the HMAC at the start of the buffer aligns with the calculated HMAC
/// with the provided key.
///
/// Returns `false` if the HMAC could not be verified.
fn verify_hmac_buffer(
    hmac_key: &[u8],
    associated_data: &[u8],
    buffer: &[u8],
    context: &[u8],
) -> bool {
    if context.len() < INTEGRITY_PREFIX_SIZE {
        return false;
    }

    let provided_hmac = &context[..INTEGRITY_PREFIX_SIZE];
    let mac = hmac_hash_components(hmac_key, associated_data, buffer);
    mac.verify_slice(provided_hmac).is_ok()
}

/// Check the CRC32 checksum encoded in `context` with the provided `buffer` and
/// `associated_data`.
///
/// Returns `false` if the checksums did not match
fn verify_crc32_buffer(associated_data: &[u8], buffer: &[u8], context: &[u8]) -> bool {
    if context.len() < INTEGRITY_PREFIX_SIZE {
        return false;
    }

    let expected_checksum = u32::from_le_bytes(context[..4].try_into().unwrap());
    let actual_checksum = crc32_hash_components(associated_data, buffer);
    actual_checksum == expected_checksum
}

fn hmac_hash_components(
    hmac_key: &[u8],
    associated_data: &[u8],
    buffer: &[u8],
) -> HmacSha256 {
    let mut mac =
        HmacSha256::new_from_slice(hmac_key).expect("HMAC can take key of any size");
    mac.update(associated_data);
    mac.update(buffer);
    mac
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
        Some(b"test".as_ref()),
        &[
            16, 104, 181, 61, 182, 2, 187, 163, 56, 84, 65,
            49, 243, 82, 216, 201, 1, 241, 168, 210, 233, 120,
            115, 132, 23, 211, 2, 112, 135, 87, 134, 90,
            0, 0, 0, 0, 0, 0, 0, 0,
        ],
        b"",
    )]
    fn test_write_check_bytes(
        #[case] input_buffer: &[u8],
        #[case] hmac_key: Option<&[u8]>,
        #[case] expected_ctx: &[u8],
        #[case] associated_data: &[u8],
    ) {
        let mut context = [0; 40];
        write_check_bytes(hmac_key, associated_data, input_buffer, &mut context);
        assert_eq!(context, expected_ctx);
    }

    #[rstest::rstest]
    #[case::simple_matching_hmac(b"hello, world!", b"test", b"test", true)]
    #[case::simple_different_hmac(b"hello, world!", b"test", b"other", false)]
    #[case::empty_payload_matching_hmac(b"", b"test", b"test", true)]
    #[case::empty_payload_different_hmac(b"", b"test", b"other", false)]
    fn test_verify_hmac_buffer(
        #[case] input_buffer: &[u8],
        #[case] sign_hmac: &[u8],
        #[case] verify_hmac: &[u8],
        #[case] should_be_valid: bool,
        #[values(b"", b"sample-associated-data")] associated_data: &[u8],
    ) {
        let mut context = [0; 40];
        write_check_bytes(Some(sign_hmac), associated_data, input_buffer, &mut context);

        let verified =
            verify_hmac_buffer(verify_hmac, associated_data, input_buffer, &context);
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
        #[values(None, Some(b"examplekey".as_ref()))] key: Option<&[u8]>,
    ) {
        let input_payload = b"example payload";
        let mut context = [0; 40];
        write_check_bytes(key, encode_assoc_data, input_payload, &mut context);

        let mode = if key.is_some() {
            Encryption::Enabled
        } else {
            Encryption::Disabled
        };

        let is_valid = verify(mode, key, decode_assoc_data, input_payload, &context);
        assert_eq!(is_valid, expected_is_valid);
    }
}
