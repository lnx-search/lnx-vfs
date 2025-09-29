use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305};

use crate::layout::{PageFileId, PageGroupId, PageId, encrypt};

mod file_metadata;
mod log;
mod page_metadata;

fn cipher_1() -> encrypt::Cipher {
    let key = Key::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    XChaCha20Poly1305::new(key)
}

fn cipher_2() -> encrypt::Cipher {
    let key = Key::from_slice(b"8f4935bDBd0A771bA20fda47f44bf2bf");
    XChaCha20Poly1305::new(key)
}

#[test]
fn test_id_displays() {
    assert_eq!(format!("{:?}", PageId(1)), "PageId(1)");
    assert_eq!(format!("{:?}", PageGroupId(2)), "PageGroupId(2)");
    assert_eq!(format!("{:?}", PageFileId(3)), "PageFileId(3)");
}
