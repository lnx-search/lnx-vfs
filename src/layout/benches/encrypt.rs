extern crate test;

use std::hint::black_box;

use chacha20poly1305::XChaCha20Poly1305;

use crate::layout::encrypt;

#[bench]
fn encrypt_512b_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_encrypt::<512>(bencher)
}

#[bench]
fn encrypt_8kb_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_encrypt::<{ 8 << 10 }>(bencher)
}

#[bench]
fn encrypt_32kb_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_encrypt::<{ 32 << 10 }>(bencher)
}

#[bench]
fn encrypt_128kb_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_encrypt::<{ 128 << 10 }>(bencher)
}

#[bench]
fn decrypt_512b_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_decrypt::<512>(bencher)
}

#[bench]
fn decrypt_8kb_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_decrypt::<{ 8 << 10 }>(bencher)
}

#[bench]
fn decrypt_32kb_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_decrypt::<{ 32 << 10 }>(bencher)
}

#[bench]
fn decrypt_128kb_blocks(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    bench_decrypt::<{ 128 << 10 }>(bencher)
}

fn bench_encrypt<const SIZE: usize>(b: &mut test::Bencher) -> anyhow::Result<()> {
    fastrand::seed(3634634636346);

    let key = encrypt::CipherKey::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    let cipher = encrypt::Cipher::new(key);

    let mut buffer = vec![0u8; 20 << 20];
    fastrand::fill(&mut buffer);
    let num_blocks = buffer.len() / SIZE;

    b.iter(|| {
        let pos = fastrand::usize(0..num_blocks);
        let start = pos * SIZE;
        let end = start + SIZE;

        let buffer = black_box(&mut buffer[start..end]);
        let mut context = [0; 40];
        encrypt::encrypt_in_place(
            black_box(&cipher),
            black_box(b"demo buffer data"),
            black_box(buffer),
            black_box(&mut context),
        )
        .unwrap();
    });

    Ok(())
}

fn bench_decrypt<const SIZE: usize>(b: &mut test::Bencher) -> anyhow::Result<()> {
    fastrand::seed(978657384653);

    let key = encrypt::CipherKey::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    let cipher = encrypt::Cipher::new(key);

    let mut buffer = vec![0; 20 << 20];
    fastrand::fill(&mut buffer);
    let num_blocks = buffer.len() / SIZE;

    let mut context = vec![0; 40 * SIZE];

    for pos in 0..num_blocks {
        let start = pos * SIZE;
        let end = start + SIZE;

        let ctx_start = pos * 40;
        let ctx_end = ctx_start + 40;

        encrypt::encrypt_in_place(
            &cipher,
            b"demo buffer data",
            buffer,
            &mut context[ctx_start..ctx_end],
        )
        .unwrap();
    }

    b.iter(|| {
        let pos = fastrand::usize(0..num_blocks);
        let start = pos * SIZE;
        let end = start + SIZE;

        let ctx_start = pos * 40;
        let ctx_end = ctx_start + 40;

        let mut buffer = [0; SIZE];
        buffer.copy_from_slice(&buffer[start..end]);

        encrypt::decrypt_in_place(
            black_box(&cipher),
            black_box(b"demo buffer data"),
            black_box(&buffer),
            black_box(&mut context[ctx_start..ctx_end]),
        )
        .unwrap();
    });

    Ok(())
}
