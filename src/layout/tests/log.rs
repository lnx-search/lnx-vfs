use super::{cipher_1, cipher_2};
use crate::layout::log::{
    FreeOp,
    HEADER_SIZE,
    LogOp,
    ReassignOp,
    WriteOp,
    decode_log_block,
    decode_log_header,
    encode_log_block,
};
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageFileId, PageGroupId, PageId, encrypt};

#[rstest::rstest]
#[case::empty_ops(vec![])]
#[case::single_op_write_one_page(vec![
    LogOp::Write(WriteOp {
        page_file_id: PageFileId(0),
        page_group_id: PageGroupId(1),
        altered_pages: vec![
            PageMetadata {
                id: PageId(1),
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                data_len: 124124,
                context: [1; 40],
            },
        ]
    })
])]
#[case::single_op_write_many_page(vec![
    LogOp::Write(WriteOp {
        page_file_id: PageFileId(0),
        page_group_id: PageGroupId(1),
        altered_pages: vec![
            PageMetadata {
                id: PageId(1),
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                data_len: 124124,
                context: [1; 40],
            },
            PageMetadata {
                id: PageId(2),
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                data_len: 999,
                context: [4; 40],
            },
        ]
    })
])]
#[case::single_op_free(vec![
    LogOp::Free(FreeOp {
        page_group_id: PageGroupId(1),
    })
])]
#[case::single_op_reassign(vec![
    LogOp::Reassign(ReassignOp {
        old_page_group_id: PageGroupId(0),
        new_page_group_id: PageGroupId(1),
    })
])]
#[case::multi_op(vec![
    LogOp::Reassign(ReassignOp {
        old_page_group_id: PageGroupId(0),
        new_page_group_id: PageGroupId(1),
    }),
    LogOp::Reassign(ReassignOp {
        old_page_group_id: PageGroupId(2),
        new_page_group_id: PageGroupId(3),
    }),
    LogOp::Free(FreeOp {
        page_group_id: PageGroupId(1),
    }),
    LogOp::Write(WriteOp {
        page_file_id: PageFileId(0),
        page_group_id: PageGroupId(1),
        altered_pages: vec![
            PageMetadata {
                id: PageId(1),
                group: PageGroupId(1),
                revision: 0,
                next_page_id: PageId(1),
                data_len: 124124,
                context: [1; 40],
            },
        ]
    })
])]
fn test_log_encode_decode(
    #[values(None, Some(cipher_1()))] cipher: Option<encrypt::Cipher>,
    #[values(true, false)] empty_associated_data: bool,
    #[case] ops: Vec<LogOp>,
) {
    let associated_data = if empty_associated_data {
        b"".as_ref()
    } else {
        b"Hello world!"
    };

    let transaction_id = 1234;
    let mut buffer = Vec::new();
    encode_log_block(
        cipher.as_ref(),
        associated_data,
        transaction_id,
        &ops,
        &mut buffer,
    )
    .expect("block should be encoded");

    assert!(!buffer.is_empty());

    let (recovered_txn_id, buffer_len) =
        decode_log_header(cipher.as_ref(), associated_data, &mut buffer[..HEADER_SIZE])
            .expect("block should be decoded");
    assert_eq!(buffer.len() - HEADER_SIZE, buffer_len);

    let mut recovered_ops = Vec::new();
    decode_log_block(
        cipher.as_ref(),
        associated_data,
        &mut buffer[HEADER_SIZE..],
        &mut recovered_ops,
    )
    .expect("block should be decoded");
    assert_eq!(recovered_txn_id, transaction_id);
    assert_eq!(ops, recovered_ops);
}

#[rstest::rstest]
#[case::no_encryption_no_assoc(None, b"", None, b"")]
#[case::no_encryption_assoc_match(None, b"hello", None, b"hello")]
#[should_panic(expected = "block should be decoded: VerificationFail")]
#[case::no_encryption_assoc_match_fail(None, b"hello", None, b"world")]
#[case::encryption_no_assoc(Some(cipher_1()), b"", Some(cipher_1()), b"")]
#[case::encryption_assoc(Some(cipher_1()), b"hello", Some(cipher_1()), b"hello")]
#[should_panic(expected = "block should be decoded: DecryptionFail")]
#[case::encryption_assoc_match_fail(
    Some(cipher_1()),
    b"hello",
    Some(cipher_1()),
    b"world"
)]
#[should_panic(expected = "block should be decoded: DecryptionFail")]
#[case::encryption_cipher_match_fail(
    Some(cipher_1()),
    b"hello",
    Some(cipher_2()),
    b"hello"
)]
#[should_panic(expected = "block should be decoded: DecryptionFail")]
#[case::encryption_cipher_and_assoc_match_fail(
    Some(cipher_1()),
    b"hello",
    Some(cipher_2()),
    b"world"
)]
fn test_decode_errors(
    #[case] encode_cipher: Option<encrypt::Cipher>,
    #[case] encode_associated_data: &[u8],
    #[case] decode_cipher: Option<encrypt::Cipher>,
    #[case] decode_associated_data: &[u8],
) {
    let ops = vec![
        LogOp::Free(FreeOp {
            page_group_id: PageGroupId(0),
        }),
        LogOp::Free(FreeOp {
            page_group_id: PageGroupId(1),
        }),
        LogOp::Free(FreeOp {
            page_group_id: PageGroupId(2),
        }),
    ];

    let mut buffer = Vec::new();
    encode_log_block(
        encode_cipher.as_ref(),
        encode_associated_data,
        1234,
        &ops,
        &mut buffer,
    )
    .expect("block should be encoded");

    let mut recovered_ops = Vec::new();
    decode_log_block(
        decode_cipher.as_ref(),
        decode_associated_data,
        &mut buffer[HEADER_SIZE..],
        &mut recovered_ops,
    )
    .expect("block should be decoded");
    assert_eq!(ops, recovered_ops);
}

#[rstest::rstest]
#[case(None, "buffer verification failed")]
#[case(Some(cipher_1()), "buffer decryption failed")]
fn test_decode_err_on_corrupted_data(
    #[case] cipher: Option<encrypt::Cipher>,
    #[case] expected_err_msg: &str,
) {
    let ops = vec![
        LogOp::Free(FreeOp {
            page_group_id: PageGroupId(0),
        }),
        LogOp::Free(FreeOp {
            page_group_id: PageGroupId(1),
        }),
        LogOp::Free(FreeOp {
            page_group_id: PageGroupId(2),
        }),
    ];

    let mut buffer = Vec::new();
    encode_log_block(cipher.as_ref(), b"test", 1234, &ops, &mut buffer)
        .expect("block should be encoded");

    buffer[70..110].fill(5);

    let mut recovered_ops = Vec::new();
    let err = decode_log_block(
        cipher.as_ref(),
        b"test",
        &mut buffer[HEADER_SIZE..],
        &mut recovered_ops,
    )
    .expect_err("block should be decoded");
    assert_eq!(err.to_string(), expected_err_msg);
}
