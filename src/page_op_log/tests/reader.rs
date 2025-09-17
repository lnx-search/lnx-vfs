use std::collections::BTreeSet;
use std::io;

use crate::buffer::DmaBuffer;
use crate::directory::FileGroup;
use crate::layout::log::{LogEntry, LogOp};
use crate::layout::{PageFileId, PageId, file_metadata, log};
use crate::page_op_log::reader::{LogDecodeError, LogFileReader};
use crate::page_op_log::{MetadataHeader, op_log_associated_data};
use crate::{ctx, file};

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_log_reader(
    #[values(true, false)] encryption: bool,
    #[values(0, 4096)] offset: u64,
    #[values(0, 5, 18)] num_blocks: usize,
) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let mut file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    make_sample_file(&ctx, &mut file, num_blocks, offset).await;

    let mut reader = LogFileReader::new(ctx, file.into(), 1, offset);

    let mut blocks = Vec::new();
    while let Some(block) = reader.next_block().await.expect("Failed to read block") {
        blocks.push(block);
    }
    assert_eq!(blocks.len(), num_blocks);

    for (block_id, block) in blocks.into_iter().enumerate() {
        assert_eq!(block.num_entries(), 7);
        let expected_first_sequence_id = block_id * 7;

        let entry = &block.entries()[0];
        assert_eq!(entry.log.sequence_id, expected_first_sequence_id as u32);
    }
}

async fn make_sample_file(
    ctx: &ctx::FileContext,
    file: &mut file::RWFile,
    num_blocks: usize,
    offset: u64,
) {
    use std::io::{Seek, Write};

    let path = ctx
        .directory()
        .resolve_file_path(FileGroup::Wal, file.id())
        .await;
    let mut raw_file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .unwrap();
    raw_file.set_len(offset).unwrap();
    raw_file.seek(io::SeekFrom::Start(offset)).unwrap();

    let mut seq_id = 0;
    for block_id in 0..num_blocks {
        let mut block = log::LogBlock::default();
        for page_id in 0..7 {
            let entry = LogEntry {
                sequence_id: seq_id,
                transaction_id: 0,
                transaction_n_entries: 1,
                page_id: PageId(((block_id * 7) + page_id) as u32),
                page_file_id: PageFileId(1),
                op: LogOp::Write,
            };
            block.push_entry(entry, None).unwrap();

            seq_id += 1;
        }

        let mut buffer = [0; log::LOG_BLOCK_SIZE];
        log::encode_log_block(
            ctx.cipher(),
            &op_log_associated_data(
                file.id(),
                1,
                offset + (block_id * log::LOG_BLOCK_SIZE) as u64,
            ),
            &block,
            &mut buffer,
        )
        .unwrap();

        raw_file.write_all(&buffer).unwrap();
        raw_file.sync_all().unwrap();
    }
}

#[rstest::rstest]
#[tokio::test]
async fn test_missing_header_processing(#[values(false, true)] encryption: bool) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let err = LogFileReader::open(ctx, file.into())
        .await
        .expect_err("log reader should not open due to missing metadata header");
    assert_eq!(err.to_string(), "missing metadata header");
}

#[rstest::rstest]
#[tokio::test]
async fn test_header_processing(#[values(false, true)] encryption: bool) {
    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let header = MetadataHeader {
        log_file_id: 1,
        encryption: ctx.get_encryption_status(),
    };
    setup_file_with_header(&ctx, &file, header).await;

    let reader = LogFileReader::open(ctx, file.into())
        .await
        .expect("log reader should process header");
    assert_eq!(
        format!("{reader:?}"),
        "LogFileReader(file_id=FileId(1000), log_file_id=1)",
    );
}

#[rstest::rstest]
#[tokio::test]
async fn test_header_encryption_missmatch() {
    let encoding_ctx = ctx::FileContext::for_test(false).await;
    let file = encoding_ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let header = MetadataHeader {
        log_file_id: 1,
        encryption: encoding_ctx.get_encryption_status(),
    };
    setup_file_with_header(&encoding_ctx, &file, header).await;

    let tmp_dir = encoding_ctx.tmp_dir();
    let decoding_ctx = ctx::FileContext::for_test_in_dir(true, tmp_dir).await;
    let file_ids = decoding_ctx.directory().list_dir(FileGroup::Wal).await;

    let file = decoding_ctx
        .directory()
        .get_rw_file(FileGroup::Wal, file_ids[0])
        .await
        .unwrap();

    let err = LogFileReader::open(decoding_ctx, file.into())
        .await
        .expect_err("log reader should not open due to encryption mismatch");
    assert_eq!(
        err.to_string(),
        "file is not encrypted but system has encryption enabled"
    );
}

#[rstest::rstest]
#[case::corrupted_tail(Corruptor::Tail)]
#[case::corrupted_head(Corruptor::Head)]
#[case::corrupted_middle(Corruptor::Middle)]
#[case::corrupted_rng1(Corruptor::Rng(345634634678))]
#[case::corrupted_rng2(Corruptor::Rng(987563437))]
#[case::corrupted_rng3(Corruptor::Rng(1276523967453))]
#[trace]
#[tokio::test]
async fn test_skip_corrupted_or_previous_blocks(
    #[values(false, true)] encryption: bool,
    #[values(1, 15, 437)] num_blocks: usize,
    #[case] corruptor: Corruptor,
) {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = ctx::FileContext::for_test(encryption).await;
    let file = ctx.make_tmp_rw_file(FileGroup::Wal).await;

    let mut buffer = ctx.alloc::<{ 256 << 10 }>();
    fill_buffer_with_blocks(&ctx, &file, &mut buffer, num_blocks);
    let num_blocks_corrupted = corruptor.corrupt(num_blocks, &mut buffer);
    assert_ne!(num_blocks_corrupted, 0);

    file.write_buffer(&mut buffer, 0).await.unwrap();

    let mut reader = LogFileReader::new(ctx, file.into(), 1, 0);

    let mut blocks_recovered = 0;
    loop {
        match reader.next_block().await {
            Err(LogDecodeError::Decode(_)) => continue,
            Err(other) => panic!("got IO error: {other}"),
            Ok(Some(block)) => {
                if block.num_entries() > 0 {
                    blocks_recovered += 1;
                }
            },
            Ok(None) => break,
        }
    }

    assert_eq!(blocks_recovered, num_blocks - num_blocks_corrupted);
}

#[derive(Debug)]
enum Corruptor {
    /// Corrupt the last 10% of data
    Tail,
    /// Corrupt the first 10% of data
    Head,
    /// Corrupt 10% of the data in the center
    Middle,
    /// Corrupt 10% of blocks randomly.
    Rng(u64),
}

impl Corruptor {
    fn corrupt(&self, num_blocks: usize, buffer: &mut DmaBuffer) -> usize {
        match self {
            Corruptor::Tail => {
                let n = num_blocks.div_ceil(10);
                let buffer_data_start = num_blocks * log::LOG_BLOCK_SIZE;
                let corrupt_from = buffer_data_start - (n * log::LOG_BLOCK_SIZE);
                fastrand::fill(&mut buffer[corrupt_from..buffer_data_start]);
                n
            },
            Corruptor::Head => {
                let n = num_blocks.div_ceil(10);
                let corrupt_to = n * log::LOG_BLOCK_SIZE;
                fastrand::fill(&mut buffer[0..corrupt_to]);
                n
            },
            Corruptor::Middle => {
                let n = num_blocks.div_ceil(10);
                let corrupt_from = (num_blocks / 2) * log::LOG_BLOCK_SIZE;
                let corrupt_to = corrupt_from + (n * log::LOG_BLOCK_SIZE);
                fastrand::fill(&mut buffer[corrupt_from..corrupt_to]);
                n
            },
            Corruptor::Rng(seed) => {
                fastrand::seed(*seed);
                let n = num_blocks.div_ceil(10);

                let mut corrupted = BTreeSet::new();
                for _ in 0..n {
                    let idx = fastrand::usize(0..num_blocks);
                    let block_start = idx * log::LOG_BLOCK_SIZE;
                    let block_end = block_start + log::LOG_BLOCK_SIZE;
                    fastrand::fill(&mut buffer[block_start..block_end]);
                    corrupted.insert(idx);
                }

                corrupted.len()
            },
        }
    }
}

async fn setup_file_with_header(
    ctx: &ctx::FileContext,
    file: &file::RWFile,
    metadata_header: MetadataHeader,
) {
    let associated_data =
        file_metadata::header_associated_data(file.id(), FileGroup::Wal);

    let mut header_buffer = ctx.alloc::<{ file_metadata::HEADER_SIZE }>();
    file_metadata::encode_metadata(
        ctx.cipher(),
        &associated_data,
        &metadata_header,
        &mut header_buffer[..],
    )
    .expect("encode header");
    file.write_buffer(&mut header_buffer, 0)
        .await
        .expect("Failed to write header");
}

fn fill_buffer_with_blocks(
    ctx: &ctx::FileContext,
    file: &file::RWFile,
    buffer: &mut DmaBuffer,
    num_blocks: usize,
) {
    let mut offset = 0;
    for page_id in 0..num_blocks {
        let associated_data = op_log_associated_data(file.id(), 1, offset);

        let entry = LogEntry {
            transaction_id: 0,
            transaction_n_entries: 1,
            sequence_id: 0,
            page_file_id: PageFileId(1),
            page_id: PageId(page_id as u32),
            op: LogOp::Write,
        };
        let mut block = log::LogBlock::default();
        block.push_entry(entry, None).unwrap();

        log::encode_log_block(
            ctx.cipher(),
            &associated_data,
            &block,
            &mut buffer[offset as usize..][..log::LOG_BLOCK_SIZE],
        )
        .expect("encode header");

        offset += log::LOG_BLOCK_SIZE as u64;
    }
}
