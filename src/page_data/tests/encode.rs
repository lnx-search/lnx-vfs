use chacha20poly1305::aead::Key;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

use crate::layout::encrypt;
use crate::page_data::DISK_PAGE_SIZE;
use crate::page_data::encode::{decode_page_data, encode_page_data};

#[rstest::rstest]
#[case::no_assoc_both_ciphers_match(Some(cipher_1()), b"", Some(cipher_1()), b"")]
#[should_panic(expected = "called `Result::unwrap()` on an `Err` value: DecryptionFail")]
#[case::no_assoc_ciphers_mismatch(Some(cipher_1()), b"", Some(cipher_2()), b"")]
#[case::assoc_both_ciphers_match(
    Some(cipher_1()),
    b"hello world",
    Some(cipher_1()),
    b"hello world"
)]
#[should_panic(expected = "called `Result::unwrap()` on an `Err` value: DecryptionFail")]
#[case::assoc_ciphers_mismatch(
    Some(cipher_1()),
    b"hello world",
    Some(cipher_2()),
    b"hello world"
)]
#[should_panic(expected = "called `Result::unwrap()` on an `Err` value: DecryptionFail")]
#[case::assoc_data_mismatch(
    Some(cipher_1()),
    b"hello world 1",
    Some(cipher_1()),
    b"hello world 2"
)]
#[case::no_assoc_no_encrypt(None, b"", None, b"")]
#[case::assoc_both_no_encrypt(None, b"hello world", None, b"hello world")]
#[should_panic(
    expected = "called `Result::unwrap()` on an `Err` value: VerificationFail"
)]
#[case::assoc_data_mismatch_no_encrypt(None, b"hello world 1", None, b"hello world 2")]
fn test_encode_decode_page_data(
    #[case] encrypt_cipher: Option<encrypt::Cipher>,
    #[case] encrypt_associated_data: &[u8],
    #[case] decrypt_cipher: Option<encrypt::Cipher>,
    #[case] decrypt_associated_data: &[u8],
) {
    let mut page_data = vec![4; DISK_PAGE_SIZE];
    let original_page_data = page_data.clone();
    let mut context = [0; 40];

    encode_page_data(
        encrypt_cipher.as_ref(),
        encrypt_associated_data,
        &mut page_data,
        &mut context,
    )
    .unwrap();

    decode_page_data(
        decrypt_cipher.as_ref(),
        decrypt_associated_data,
        &mut page_data,
        &context,
    )
    .unwrap();

    assert_eq!(page_data, original_page_data, "page data does not match");
}

fn cipher_1() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbface1");
    XChaCha20Poly1305::new(key)
}

fn cipher_2() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbface2");
    XChaCha20Poly1305::new(key)
}
