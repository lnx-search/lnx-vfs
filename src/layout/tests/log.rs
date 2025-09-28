use crate::layout::encrypt;
use crate::layout::log::{LogOp, encode_log_block, decode_log_block, WriteOp, FreeOp, ReassignOp};
use crate::layout::page_metadata::PageMetadata;
use crate::layout::{PageId, PageFileId, PageGroupId};
use super::{cipher_1, cipher_2};


#[rstest::rstest]
#[case::empty_ops(vec![])]
#[case::single_op_write_one_page(vec![
    LogOp::Write(WriteOp {
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
    ).expect("block should be encoded");

    assert!(!buffer.is_empty());

    let mut recovered_ops = Vec::new();
    let recovered_txn_id = decode_log_block(
        cipher.as_ref(),
        associated_data,
        &mut buffer,
        &mut recovered_ops,
    ).expect("block should be decoded");
    assert_eq!(recovered_txn_id, transaction_id);
    assert_eq!(ops, recovered_ops);
}


