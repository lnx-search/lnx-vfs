use crate::layout::PageGroupId;

#[derive(Default)]
/// A collection of page group locks to prevent concurrent mutations of the
/// same page group.
pub(super) struct GroupLocks {
    locks: parking_lot::Mutex<foldhash::HashSet<PageGroupId>>,
}

impl GroupLocks {
    /// Attempt to get a lock guard for a given group.
    pub(super) fn try_lock(
        &self,
        group: PageGroupId,
    ) -> Result<GroupGuard<'_>, ConcurrentMutationError> {
        let did_insert = self.locks.lock().insert(group);
        if did_insert {
            Ok(GroupGuard {
                parent: self,
                group,
            })
        } else {
            Err(ConcurrentMutationError(group))
        }
    }

    fn release(&self, group: PageGroupId) {
        self.locks.lock().remove(&group);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("concurrent mutation error on {0:?}")]
/// Two or more tasks attempted to modify a page group at the same time.
pub struct ConcurrentMutationError(pub PageGroupId);

pub(super) struct GroupGuard<'l> {
    parent: &'l GroupLocks,
    group: PageGroupId,
}

impl<'l> Drop for GroupGuard<'l> {
    fn drop(&mut self) {
        self.parent.release(self.group);
    }
}
