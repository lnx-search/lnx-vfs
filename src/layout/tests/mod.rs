use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305};
use crate::layout::encrypt;

mod file_metadata;
mod page_metadata;
mod log;

fn cipher_1() -> encrypt::Cipher {
    let key = Key::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    XChaCha20Poly1305::new(key)
}

fn cipher_2() -> encrypt::Cipher {
    let key = Key::from_slice(b"8f4935bDBd0A771bA20fda47f44bf2bf");
    XChaCha20Poly1305::new(key)
}
