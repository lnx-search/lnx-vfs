use crate::encryption_key::{
    decode_encryption_keys,
    generate_encryption_keys,
    rencode_encryption_keys,
};

#[test]
fn test_generate_keys_and_decode_keys() {
    let buffer =
        generate_encryption_keys("example1").expect("encryption key generation failed");

    let _keys = decode_encryption_keys("example1", &buffer).expect("decode keys failed");
}

#[test]
fn test_decode_encryption_keys_wrong_password() {
    let buffer =
        generate_encryption_keys("example1").expect("encryption key generation failed");

    let err = decode_encryption_keys("example2", &buffer)
        .expect_err("decode keys should fail failed");
    assert_eq!(err.to_string(), "failed to decrypt data");
}

#[test]
fn test_rencode_encryption_keys() {
    let mut buffer =
        generate_encryption_keys("example1").expect("encryption key generation failed");

    let keys1 = decode_encryption_keys("example1", &buffer).expect("decode keys failed");

    buffer = rencode_encryption_keys("example1", "example2", &buffer)
        .expect("rencode keys failed");

    let err = decode_encryption_keys("example1", &buffer)
        .expect_err("decode keys should fail failed");
    assert_eq!(err.to_string(), "failed to decrypt data");

    let keys2 = decode_encryption_keys("example2", &buffer).expect("decode keys failed");
    assert_eq!(keys1.authentication_key, keys2.authentication_key);
}

#[test]
fn test_encryption_keys_debug_does_not_leak_info() {
    let buffer =
        generate_encryption_keys("example1").expect("encryption key generation failed");

    let keys = decode_encryption_keys("example1", &buffer).expect("decode keys failed");
    assert_eq!(format!("{keys:?}"), "EncryptionKeys");
}
