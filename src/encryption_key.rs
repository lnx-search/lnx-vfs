use std::time::SystemTime;

use argon2::PasswordHasher;
use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::{OsRng, RngCore};
use chacha20poly1305::KeyInit;

use crate::layout::encrypt;

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding [EncryptionKeys]
/// from the input buffer.
pub enum DecodeKeysError {
    #[error("password hash error: {0}")]
    /// Argon2 was unable to hash the provided user password.
    PasswordHashFailed(argon2::password_hash::Error),
    #[error(transparent)]
    /// A failure to deserialize the key struct data to bytes.
    Deserialize(#[from] rmp_serde::decode::Error),
    #[error(transparent)]
    /// The serialized buffer was unable to be decrypted.
    DecryptionFailed(#[from] encrypt::DecryptError),
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from encoding the [EncryptionKeys]
/// to a serialized output buffer.
pub enum EncodeKeysError {
    #[error("password hash error: {0}")]
    /// Argon2 was unable to hash the provided user password.
    PasswordHashFailed(argon2::password_hash::Error),
    #[error(transparent)]
    /// The serialized buffer was unable to be encrypted.
    EncryptionFailed(#[from] encrypt::EncryptError),
    #[error(transparent)]
    /// A failure to serialize the key struct data to bytes.
    Serialize(#[from] rmp_serde::encode::Error),
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from re-encoding the encryption keys
/// provided by the input buffer.
pub enum ReEncodeKeysError {
    #[error(transparent)]
    /// An encoder error occurred.
    Encode(#[from] EncodeKeysError),
    #[error(transparent)]
    /// An decoder error occurred.
    Decode(#[from] DecodeKeysError),
}

/// Manage system generated encryption and authentication keys protected
/// by a user-provided password/key.
pub struct EncryptionKeys {
    /// The encryption cipher key.
    pub encryption_cipher: encrypt::Cipher,
    /// The authentication key.
    pub authentication_key: [u8; 32],
}

impl std::fmt::Debug for EncryptionKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptionKeys")
    }
}

/// Decode the [EncryptionKeys] from the provided input buffer and password
/// used for encrypting & wrapping the keys.
pub fn decode_encryption_keys(
    password: &str,
    buffer: &[u8],
) -> Result<EncryptionKeys, DecodeKeysError> {
    let key_data = decode_key_data_inner(password, buffer)?;
    let cipher_key = encrypt::CipherKey::from_slice(&key_data.encryption_key);
    let cipher = encrypt::Cipher::new(cipher_key);

    Ok(EncryptionKeys {
        encryption_cipher: cipher,
        authentication_key: key_data.authentication_key,
    })
}

/// Generate a new set of random [EncryptionKeys] then encode and encrypt them
/// outputting the wrapped keys in the buffer.
pub fn generate_encryption_keys(password: &str) -> Result<Vec<u8>, EncodeKeysError> {
    let key_data = PackagedSystemKeyData::random();
    encode_key_data(password, &key_data)
}

/// Re-encode the encryption keys within the buffer, changing it from being
/// encrypted with `old_password` to `new_password`.
pub fn rencode_encryption_keys(
    old_password: &str,
    new_password: &str,
    buffer: &[u8],
) -> Result<Vec<u8>, ReEncodeKeysError> {
    let key_data = decode_key_data_inner(old_password, buffer)?;
    encode_key_data(new_password, &key_data).map_err(ReEncodeKeysError::Encode)
}

fn decode_key_data_inner(
    password: &str,
    buffer: &[u8],
) -> Result<PackagedSystemKeyData, DecodeKeysError> {
    let argon2 = get_argon2();

    let data: Data = rmp_serde::from_slice(buffer)?;
    let salt = data.salt();

    let wrapping_encryption_key =
        derive_wrapping_encryption_key(&argon2, password, &salt)
            .map_err(DecodeKeysError::PasswordHashFailed)?;

    let Data::V1 {
        context,
        mut packaged,
        ..
    } = data;

    encrypt::decrypt_in_place(&wrapping_encryption_key, b"", &mut packaged, &context)?;

    rmp_serde::from_slice(&packaged).map_err(DecodeKeysError::Deserialize)
}

fn encode_key_data(
    password: &str,
    key_data: &PackagedSystemKeyData,
) -> Result<Vec<u8>, EncodeKeysError> {
    let argon2 = get_argon2();
    let salt = SaltString::generate(&mut OsRng);
    let wrapping_encryption_key =
        derive_wrapping_encryption_key(&argon2, password, &salt)
            .map_err(EncodeKeysError::PasswordHashFailed)?;

    let mut packaged = rmp_serde::to_vec_named(&key_data)?;
    let mut context = [0; 40];

    encrypt::encrypt_in_place(
        &wrapping_encryption_key,
        b"",
        &mut packaged,
        &mut context,
    )?;

    let data = Data::V1 {
        salt: salt.to_string(),
        context: context.to_vec(),
        packaged,
    };

    rmp_serde::to_vec_named(&data).map_err(EncodeKeysError::Serialize)
}

fn get_argon2() -> argon2::Argon2<'static> {
    const DEFAULT_MEM_COST: u32 = 65_536;
    const DEFAULT_TIME_COST: u32 = if cfg!(test) { 1 } else { 5 };
    const DEFAULT_PARALLELISM: u32 = 4;

    let params = argon2::Params::new(
        DEFAULT_MEM_COST,
        DEFAULT_TIME_COST,
        DEFAULT_PARALLELISM,
        None,
    )
    .expect("argon2 parameters should be valid");

    argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params)
}

fn derive_wrapping_encryption_key(
    argon2: &argon2::Argon2<'_>,
    password: &str,
    salt: &SaltString,
) -> Result<encrypt::Cipher, argon2::password_hash::Error> {
    let hash_output = argon2
        .hash_password(password.as_ref(), salt)?
        .hash
        .expect("password hash should be present");

    let cipher_key = encrypt::CipherKey::from_slice(hash_output.as_ref());
    let cipher = encrypt::Cipher::new(cipher_key);
    Ok(cipher)
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "version", content = "data")]
enum Data {
    /// Version 1 of the encryption key system.
    V1 {
        /// Salt for Argon2ID
        salt: String,
        /// The encryption context of the packaged data.
        ///
        /// This is 40 bytes in length but serde does not support `[T; 40]`.
        context: Vec<u8>,
        /// The encrypted data holding the inner system keys & metadata.
        packaged: Vec<u8>,
    },
}

impl Data {
    fn salt(&self) -> SaltString {
        match self {
            Data::V1 { salt, .. } => {
                SaltString::from_b64(salt).expect("salt should be valid")
            },
        }
    }
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct PackagedSystemKeyData {
    encryption_key: [u8; 32],
    authentication_key: [u8; 32],
    creation_ts: u64,
    creation_id: String,
}

impl PackagedSystemKeyData {
    fn random() -> Self {
        let mut encryption_key = [0u8; 32];
        OsRng.fill_bytes(encryption_key.as_mut());

        let mut authentication_key = [0u8; 32];
        OsRng.fill_bytes(authentication_key.as_mut());

        let creation_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut creation_id_bytes = [0; 64];
        OsRng.fill_bytes(creation_id_bytes.as_mut());
        let creation_id = hex::encode(creation_id_bytes);

        Self {
            encryption_key,
            authentication_key,
            creation_ts,
            creation_id,
        }
    }
}
