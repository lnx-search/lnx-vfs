use crate::layout::file_metadata::Encryption;
use crate::layout::{encrypt, integrity};

/// Encode the provided page data and write the context to the page metadata.
///
/// If encryption at rest is enabled, this will encrypt the buffer, otherwise an integrity
/// checksum will be calculated.
pub(super) fn encode_page_data(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    page_data: &mut [u8],
    context: &mut [u8],
) -> Result<(), encrypt::EncryptError> {
    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, associated_data, page_data, context)?;
    } else {
        integrity::write_check_bytes(None, associated_data, page_data, context);
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
/// An error preventing the decoder from processing the data.
pub enum DecodePageError {
    #[error("buffer decryption failed")]
    /// The buffer could not be decrypted.
    DecryptionFail,
    #[error("buffer verification failed")]
    /// The buffer could not be verified for integrity.
    VerificationFail,
}

/// Decode the provided page data using the context provided.
///
/// If encryption at rest is enabled, this will decrypt the buffer, otherwise an integrity
/// checksum will be calculated and verified against the stored checksum in the context.
pub(super) fn decode_page_data(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    page_data: &mut [u8],
    context: &[u8],
) -> Result<(), DecodePageError> {
    if let Some(cipher) = cipher {
        encrypt::decrypt_in_place(cipher, associated_data, page_data, context)
            .map_err(|_| DecodePageError::DecryptionFail)?;
    } else {
        let is_valid = integrity::verify(
            Encryption::Disabled,
            None,
            associated_data,
            page_data,
            context,
        );
        if !is_valid {
            return Err(DecodePageError::VerificationFail);
        }
    }

    Ok(())
}
