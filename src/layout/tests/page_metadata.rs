use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305};

use crate::layout::page_metadata::{
    DecodeError,
    PageChangeCheckpoint,
    PageMetadata,
    decode_page_metadata_changes,
    encode_page_metadata_changes,
};
use crate::layout::{PageGroupId, PageId, encrypt};

const SAMPLE_PAGE_METADATA: PageMetadata = PageMetadata {
    id: PageId(1),
    group: PageGroupId(1),
    revision: 0,
    next_page_id: PageId(1),
    data_len: 124124,
    context: [1; 40],
};

#[rstest::rstest]
#[case::encrypt_empty(true, &[])]
#[case::encrypt_one_page(true, &[SAMPLE_PAGE_METADATA])]
#[case::encrypt_many_pages(true, &[SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA])]
#[case::decrypt_empty(false, &[])]
#[case::decrypt_one_page(false, &[SAMPLE_PAGE_METADATA])]
#[case::decrypt_many_pages(false, &[SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA])]
fn test_encode_pages_metadata(#[case] encrypt: bool, #[case] pages: &[PageMetadata]) {
    let cipher = if encrypt { Some(cipher_1()) } else { None };

    let mut updates = PageChangeCheckpoint::default();
    for page in pages {
        updates.push(*page);
    }

    encode_page_metadata_changes(cipher.as_ref(), b"", &updates)
        .expect("page metadata encoding failed");
}

#[rstest::rstest]
#[case::encrypt_empty(true, &[])]
#[case::encrypt_one_page(true, &[SAMPLE_PAGE_METADATA])]
#[case::encrypt_many_pages(true, &[SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA])]
#[case::decrypt_empty(false, &[])]
#[case::decrypt_one_page(false, &[SAMPLE_PAGE_METADATA])]
#[case::decrypt_many_pages(false, &[SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA, SAMPLE_PAGE_METADATA])]
fn test_encode_decode_pages_metadata(
    #[case] encrypt: bool,
    #[case] pages: &[PageMetadata],
) {
    let cipher = if encrypt { Some(cipher_1()) } else { None };

    let mut updates = PageChangeCheckpoint::default();
    for page in pages {
        updates.push(*page);
    }

    let mut buffer = encode_page_metadata_changes(cipher.as_ref(), b"", &updates)
        .expect("page metadata encoding failed");

    let decoded_block = decode_page_metadata_changes(cipher.as_ref(), b"", &mut buffer)
        .expect("page metadata decode");
    assert_eq!(decoded_block, updates);
}

#[rstest::rstest]
fn test_decode_err_incorrect_buffer_size(#[values(true, false)] encrypt: bool) {
    let cipher = if encrypt { Some(cipher_1()) } else { None };
    let updates = PageChangeCheckpoint::default();

    let mut buffer =
        encode_page_metadata_changes(cipher.as_ref(), b"", &updates).unwrap();

    let err = decode_page_metadata_changes(cipher.as_ref(), b"", &mut buffer[..20])
        .expect_err("page metadata decode should fail");
    assert_eq!(err.to_string(), "provided buffer length is incorrect");
}

#[rstest::rstest]
#[case::decrypt_non_encrypted_data(None, Some(cipher_1()), DecodeError::DecryptionFail)]
#[case::dencrypt_missmatch_keys(
    Some(cipher_1()),
    Some(cipher_2()),
    DecodeError::DecryptionFail
)]
#[case::checksum_missmatch(Some(cipher_1()), None, DecodeError::VerificationFail)]
fn test_decode_err_most_errors(
    #[case] encode_cipher: Option<encrypt::Cipher>,
    #[case] decode_cipher: Option<encrypt::Cipher>,
    #[case] expected_error: DecodeError,
) {
    let updates = PageChangeCheckpoint::default();
    let mut buffer =
        encode_page_metadata_changes(encode_cipher.as_ref(), b"", &updates).unwrap();

    let err = decode_page_metadata_changes(decode_cipher.as_ref(), b"", &mut buffer)
        .expect_err("page metadata decode should fail");
    assert_eq!(err.to_string(), expected_error.to_string());
}

#[test]
fn test_empty_page_check() {
    let metadata = PageMetadata::null();
    assert!(metadata.is_unassigned());
}

fn cipher_1() -> encrypt::Cipher {
    let key = Key::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    XChaCha20Poly1305::new(key)
}

fn cipher_2() -> encrypt::Cipher {
    let key = Key::from_slice(b"8f4935bDBd0A771bA20fda47f44bf2bf");
    XChaCha20Poly1305::new(key)
}
