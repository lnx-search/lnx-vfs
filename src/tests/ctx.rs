use super::tempdir;
use crate::ctx;
use crate::layout::file_metadata::Encryption;

#[rstest::rstest]
#[case::encryption_on(None, Encryption::Disabled)]
#[case::encryption_off(Some("testing"), Encryption::Enabled)]
#[tokio::test]
async fn test_ctx_builder_open(
    tempdir: tempfile::TempDir,
    #[case] encryption_key: Option<&str>,
    #[case] expected_encryption_status: Encryption,
) {
    let ctx = ctx::ContextBuilder::new(tempdir.path())
        .with_encryption_key(encryption_key.map(|s| s.to_string()))
        .open()
        .await
        .expect("ctx open should succeed");
    assert_eq!(ctx.get_encryption_status(), expected_encryption_status);
}

#[rstest::rstest]
#[tokio::test]
async fn test_ctx_builder_errors_with_miss_matched_encryption_keys(
    tempdir: tempfile::TempDir,
) {
    let ctx = ctx::ContextBuilder::new(tempdir.path())
        .with_encryption_key(Some("testing".to_string()))
        .open()
        .await
        .expect("ctx open should succeed");
    assert_eq!(ctx.get_encryption_status(), Encryption::Enabled);
    drop(ctx);

    let err = ctx::ContextBuilder::new(tempdir.path())
        .with_encryption_key(Some("other".to_string()))
        .open()
        .await
        .expect_err("ctx open should not open");
    assert_eq!(
        err.to_string(),
        "failed to decode encryption keys: decryption error, is the password correct?"
    );
}
