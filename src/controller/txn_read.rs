use super::metadata::LookupEntry;
use super::storage::StorageController;
use crate::layout::PageGroupId;

/// A [StorageReader] allows you to read data pages for a given
/// page group.
pub struct StorageReader<'a> {
    group: PageGroupId,
    lookup: LookupEntry,
    controller: &'a StorageController,
}

impl std::fmt::Debug for StorageReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageReader(group={:?})", self.group)
    }
}

impl<'c> StorageReader<'c> {
    /// Creates a new [StorageReader].
    pub fn new(
        group: PageGroupId,
        lookup: LookupEntry,
        controller: &'c StorageController,
    ) -> Self {
        Self {
            group,
            lookup,
            controller,
        }
    }
}
