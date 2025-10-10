use super::tempdir;
use crate::encryption_file::*;

#[rstest::rstest]
#[tokio::test]
async fn test_generation_and_load_keys_file(tempdir: tempfile::TempDir) {
    let keys1 = load_encryption_keys(tempdir.path(), "helloworld")
        .await
        .expect("generate new keys");

    let keys2 = load_encryption_keys(tempdir.path(), "helloworld")
        .await
        .expect("generate new keys");

    assert_eq!(keys1.authentication_key, keys2.authentication_key);
}

#[rstest::rstest]
#[tokio::test]
async fn test_change_password_keys_file(tempdir: tempfile::TempDir) {
    let keys1 = load_encryption_keys(tempdir.path(), "helloworld")
        .await
        .expect("generate new keys");

    change_password(tempdir.path(), "helloworld", "helloworld2")
        .await
        .expect("change password");

    let err = load_encryption_keys(tempdir.path(), "helloworld")
        .await
        .expect_err("system should have changed password");
    assert_eq!(
        err.to_string(),
        "failed to decode encryption keys: decryption error, is the password correct?"
    );

    let keys2 = load_encryption_keys(tempdir.path(), "helloworld2")
        .await
        .expect("generate new keys");

    assert_eq!(keys1.authentication_key, keys2.authentication_key);
}
