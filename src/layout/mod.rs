//! A page file is a collection of 8KB pages and their respective metadata.
//!
//! Each page file contains 1,000,000 pages, which brings the target page file size
//! inline with ~8GB.
//!
//! Page files are structured with the following layout:
//!
//! - Page File Metadata
//!     * Contains information like layout version.
//! - Allocation Table
//!     * A checkpoint bitset tracking what pages within the file
//!       are allocated.
//!     * This value is authenticated but not encrypted when encryption at
//!       rest is enabled.
//!     * The size of the table is aligned to 8KB, totalling 128KB.
//! - Page Operation Log
//!     * A fixed size log for completing transactional operations.
//!     * This acts similarly to a WAL.
//!     * Each entry is 64 bytes, and blocks of logs are aligned to 512B.
//!     * The log is containing up to `524,288` entries totalling 32MB of reserved
//!       space.
//! - Page Meta Table
//!     * Contains a fixed lookup table holding page metadata which are 64B in size.
//!     * Blocks are aligned on 4KB boundaries (63 x entries per block + offset)
//!     * 1 entry per page, so 1,000,000 entries totalling ~65MB.
//! - Page data
//!     * 8KB blocks of data.
//!     * Raw data, left as is.
//!
//! Overall the total overhead of the file is roughly ~98MB (128KB + 32MB + 65MB.)
//!

#[cfg(all(test, not(miri), feature = "bench-lib-unstable"))]
mod benches;
pub mod encrypt;
pub mod file_metadata;
pub mod integrity;
pub mod log;
pub mod page_metadata;
#[cfg(all(test, not(miri)))]
mod tests;

#[repr(transparent)]
#[derive(
    Debug,
    Copy,
    Clone,
    Ord,
    Hash,
    PartialOrd,
    Eq,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq))]
/// A unique identifier for a group of pages.
pub struct PageGroupId(pub(crate) u64);

impl PageGroupId {
    /// A null group ID for representing page group.
    pub const NULL: PageGroupId = PageGroupId(u64::MAX);

    /// Returns if the page group ID is `null`.
    pub const fn is_null(&self) -> bool {
        self.0 == u64::MAX
    }
}

#[derive(
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq))]
/// A unique identifier for a file of pages.
pub struct PageFileId(pub(crate) u32);

impl std::fmt::Debug for PageFileId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PageFileId({})", self.0)
    }
}

#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq))]
/// A unique ID for a page of data within a storage file.
pub struct PageId(pub(crate) u32);

impl PageId {
    /// A terminator page ID for ending a chain of pages.
    pub const TERMINATOR: PageId = PageId(u32::MAX);

    /// Returns if the page ID is a terminator value and not an actual
    /// page identifier. This is used to signal the end of a chain.
    pub const fn is_terminator(&self) -> bool {
        self.0 == u32::MAX
    }
}

impl std::fmt::Debug for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}
