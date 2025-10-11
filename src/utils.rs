use std::mem;
use std::sync::Arc;

use crate::buffer::ALLOC_PAGE_SIZE;
use crate::page_data::DISK_PAGE_SIZE;

#[macro_export]
/// Assert a given condition is `true` otherwise, abort.
macro_rules! assert_or_abort {
    ($condition:expr, $message:expr) => {
        if !$condition {
            $crate::utils::abort_system($message, Some(&"assertion failed"));
        }
    };
}

#[inline(never)]
#[cold]
/// Abort the process, logging as much information as possible.
pub(crate) fn abort_system(message: &str, cause: Option<&dyn std::fmt::Debug>) -> ! {
    tracing::error!(message = %message, "FATAL: system is preparing to abort");

    eprintln!("FATAL: system is preparing to abort");
    eprintln!("FATAL: system is preparing to abort");
    eprintln!("FATAL: system is preparing to abort");
    eprintln!("FATAL: {message}");
    eprintln!("FATAL: additional cause: {cause:?}");
    eprintln!(
        "FATAL: you are seeing this message as the VFS storage layer could not ensure the \
        state memory remains consistent with the persisted data. \
        This normally means a underlying device has failed."
    );

    std::thread::sleep(std::time::Duration::from_millis(200));

    #[cfg(test)]
    panic!("ABORT CALL ACTIVATED: {message}, cause: {cause:?}");
    #[cfg(not(test))]
    std::process::abort()
}

/// A helper type for having a single value on the stack of a heap allocated
/// value in an Arc.
///
/// The single value can be converted to a shared value and then cached.
pub(super) enum SingleOrShared<T> {
    #[doc(hidden)]
    None,
    Single(T),
    Shared(Arc<T>),
}

impl<T> SingleOrShared<T> {
    #[inline]
    /// Converts the `Single` variant of this type into the `Shared` variant,
    /// or if the value is already `Shared`, return a clone of the inner arc.
    pub fn share(&mut self) -> Arc<T> {
        let guard = mem::replace(self, SingleOrShared::None);
        match guard {
            SingleOrShared::None => unreachable!("variant should never be hit"),
            SingleOrShared::Single(single) => {
                let shared = Arc::new(single);
                *self = SingleOrShared::Shared(shared.clone());
                shared
            },
            SingleOrShared::Shared(shared) => {
                *self = SingleOrShared::Shared(shared.clone());
                shared
            },
        }
    }
}

/// Converts the number of disk pages to the equivalent number of memory alloc pages.
pub(crate) const fn disk_to_alloc_pages(disk_pages: usize) -> usize {
    const {
        assert!(
            DISK_PAGE_SIZE.is_multiple_of(ALLOC_PAGE_SIZE),
            "disk page size must be a multiple of the alloc page size"
        );
        assert!(
            DISK_PAGE_SIZE >= ALLOC_PAGE_SIZE,
            "disk page size must be greater than alloc page size"
        );
    };
    let total_size = disk_pages * DISK_PAGE_SIZE;
    total_size / ALLOC_PAGE_SIZE
}

pub(super) fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

pub(super) fn align_down(value: usize, align: usize) -> usize {
    (value / align) * align
}

#[cfg(test)]
pub(crate) fn parse_io_error_return<T>(
    value: Option<String>,
) -> Result<T, std::io::Error> {
    let Some(value) = value else {
        return Err(std::io::Error::other("standard fail point error"));
    };
    let error_code = value
        .parse::<i32>()
        .expect("invalid io error code provided");
    Err(std::io::Error::from_raw_os_error(-error_code))
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 4), 0);
        assert_eq!(align_up(1, 4), 4);
        assert_eq!(align_up(2, 4), 4);
        assert_eq!(align_up(3, 4), 4);
        assert_eq!(align_up(4, 4), 4);
        assert_eq!(align_up(4096, 4096), 4096);
    }

    #[test]
    fn test_align_down() {
        assert_eq!(align_down(0, 4), 0);
        assert_eq!(align_down(1, 4), 0);
        assert_eq!(align_down(4, 4), 4);
        assert_eq!(align_down(5, 4), 4);
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(20480, 4096), 20480);
    }

    #[test]
    fn test_single_or_shared() {
        let mut single = SingleOrShared::Single(123);
        let shared1 = single.share();
        let shared2 = single.share();
        assert!(matches!(single, SingleOrShared::Shared(_)));
        assert_eq!(shared1, shared2);
    }

    #[test]
    #[should_panic]
    fn test_single_or_shared_none_variant_panics() {
        let mut single: SingleOrShared<()> = SingleOrShared::None;
        single.share();
    }
}
