use std::path::Path;
use std::sync::Arc;
use std::{io, mem};

use crate::buffer::ALLOC_PAGE_SIZE;
use crate::page_data::DISK_PAGE_SIZE;

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
            DISK_PAGE_SIZE % ALLOC_PAGE_SIZE == 0,
            "disk page size must be a multiple of the alloc page size"
        );
        assert!(
            DISK_PAGE_SIZE >= ALLOC_PAGE_SIZE,
            "disk page size must be greater than alloc page size"
        );
    };
    let total_size = disk_pages * DISK_PAGE_SIZE;
    let num_alloc_pages = total_size / ALLOC_PAGE_SIZE;
    num_alloc_pages
}

pub(super) fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

pub(super) fn align_down(value: usize, align: usize) -> usize {
    (value / align) * align
}

pub(super) fn create_file(
    path: &Path,
    allow_existing: bool,
) -> io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    options.read(true);

    if allow_existing {
        options.create(true);
    } else {
        options.create_new(true);
    }

    let file = options.open(path)?;

    #[cfg(unix)]
    {
        let parent = path.parent().unwrap();
        std::fs::OpenOptions::new()
            .read(true)
            .open(&parent)?
            .sync_all()?;
    }

    Ok(file)
}

#[cfg(test)]
pub(crate) fn parse_io_error_return<T>(value: Option<String>) -> Result<T, io::Error> {
    let Some(value) = value else {
        return Err(io::Error::other("standard fail point error"));
    };
    let error_code = value
        .parse::<i32>()
        .expect("invalid io error code provided");
    Err(io::Error::from_raw_os_error(-error_code))
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use std::io::ErrorKind;

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

    #[test]
    fn test_create_file_helper() {
        let dir = tempfile::tempdir().unwrap();

        let fp = dir.path().join("test1");
        create_file(&fp, true).expect("create file that doesn't exist should work");

        let error = create_file(&fp, false)
            .expect_err("allow existing should prevent file being created");
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);

        create_file(&fp, true).expect("file should be over written");
    }
}
