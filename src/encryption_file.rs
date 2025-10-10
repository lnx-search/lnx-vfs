use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};

use super::encryption_key::{
    DecodeKeysError,
    EncodeKeysError,
    EncryptionKeys,
    ReEncodeKeysError,
    decode_encryption_keys,
    generate_encryption_keys,
    rencode_encryption_keys,
};

static ENCRYPTION_KEY_FILE: &str = "keys.lnx";
static ENCRYPTION_KEY_FILE_INIT: &str = "keys.lnx.atomic";

#[derive(Debug, thiserror::Error)]
/// An error preventing the system from loading or generating encryption
/// keys for the VFS.
pub enum LoadError {
    #[error("IO error prevented encryption key loading: {0}")]
    /// An IO error occurred.
    Io(#[from] io::Error),
    #[error("failed to encode encryption keys: {0}")]
    /// The system could not encode the generated keys.
    EncodeKeys(#[from] EncodeKeysError),
    #[error("failed to decode encryption keys: {0}")]
    /// The system could not decode the generated keys.
    DecodeKeys(#[from] DecodeKeysError),
}

#[derive(Debug, thiserror::Error)]
pub enum ChangePasswordError {
    #[error("IO error prevented the operation: {0}")]
    /// An IO error occurred.
    Io(#[from] io::Error),
    #[error("No encryption keys were found")]
    NoEncryptionKeysFound,
    #[error("Failed to re-encode encryption keys: {0}")]
    /// The system could not re-encode the keys.
    ReEncode(#[from] ReEncodeKeysError),
}

/// Load or generate a set of encryption keys with the given password.
pub async fn load_encryption_keys(
    base_path: &Path,
    password: &str,
) -> Result<EncryptionKeys, LoadError> {
    let base_path = base_path.to_path_buf();
    let password = password.to_string();
    tokio::task::spawn_blocking(move || load_encryption_keys_inner(base_path, &password))
        .await
        .expect("spawn blocking task")
}

/// Change the password protecting the encryption keys.
pub async fn change_password(
    base_path: &Path,
    old_password: &str,
    new_password: &str,
) -> Result<(), ChangePasswordError> {
    let base_path = base_path.to_path_buf();
    let old_password = old_password.to_string();
    let new_password = new_password.to_string();
    tokio::task::spawn_blocking(move || {
        change_password_inner(base_path, &old_password, &new_password)
    })
    .await
    .expect("spawn blocking task")
}

fn load_encryption_keys_inner(
    base_path: PathBuf,
    password: &str,
) -> Result<EncryptionKeys, LoadError> {
    let path = base_path.join(ENCRYPTION_KEY_FILE);
    let key_data = match std::fs::read(&path) {
        Ok(content) => content,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return generate_new_keys(&base_path, password);
        },
        Err(e) => return Err(e.into()),
    };

    decode_encryption_keys(password, &key_data).map_err(LoadError::DecodeKeys)
}

fn change_password_inner(
    base_path: PathBuf,
    old_password: &str,
    new_password: &str,
) -> Result<(), ChangePasswordError> {
    let path = base_path.join(ENCRYPTION_KEY_FILE);
    let key_data = match std::fs::read(&path) {
        Ok(content) => content,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err(ChangePasswordError::NoEncryptionKeysFound);
        },
        Err(e) => return Err(e.into()),
    };

    let new_key_data = rencode_encryption_keys(old_password, new_password, &key_data)?;

    write_key_data(&base_path, &new_key_data)?;

    Ok(())
}

fn generate_new_keys(
    base_path: &Path,
    password: &str,
) -> Result<EncryptionKeys, LoadError> {
    let key_data = generate_encryption_keys(password)?;
    write_key_data(base_path, &key_data)?;
    decode_encryption_keys(password, &key_data).map_err(LoadError::DecodeKeys)
}

fn write_key_data(base_path: &Path, key_data: &[u8]) -> io::Result<()> {
    let init_path = base_path.join(ENCRYPTION_KEY_FILE_INIT);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(&init_path)?;

    file.write_all(key_data)?;
    file.sync_all()?;

    let final_path = base_path.join(ENCRYPTION_KEY_FILE);
    std::fs::rename(init_path, &final_path)?;

    let dir = std::fs::OpenOptions::new().read(true).open(base_path)?;
    dir.sync_all()?;

    Ok(())
}
