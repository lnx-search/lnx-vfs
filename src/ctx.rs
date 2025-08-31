use crate::arena::ArenaAllocator;
use crate::buffer;
use crate::buffer::ALLOC_PAGE_SIZE;
use crate::directory::SystemDirectory;
use crate::layout::encrypt;
use crate::layout::file_metadata::Encryption;

/// The file context contains general settings and information
/// for the file reader and writers of any type to use.
pub struct FileContext {
    cipher: Option<encrypt::Cipher>,
    arena_allocator: ArenaAllocator,
    directory: SystemDirectory,
    #[cfg(test)]
    tmp_dir: std::sync::Arc<tempfile::TempDir>,
}

impl FileContext {
    #[cfg(test)]
    /// Create a new file context for testing.
    pub async fn for_test(encryption: bool) -> std::sync::Arc<Self> {
        let tmp_dir = tempfile::tempdir().unwrap();
        Self::for_test_in_dir(encryption, std::sync::Arc::new(tmp_dir)).await
    }

    #[cfg(test)]
    /// Create a new file context for testing.
    pub async fn for_test_in_dir(
        encryption: bool,
        tmp_dir: std::sync::Arc<tempfile::TempDir>,
    ) -> std::sync::Arc<Self> {
        use chacha20poly1305::aead::Key;
        use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

        let cipher = if encryption {
            let key = Key::<XChaCha20Poly1305>::from_slice(
                b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee",
            );
            Some(encrypt::Cipher::new(key))
        } else {
            None
        };

        let directory = SystemDirectory::open(tmp_dir.path())
            .await
            .expect("open system directory");

        std::sync::Arc::new(Self {
            cipher,
            arena_allocator: ArenaAllocator::new(100),
            directory,
            tmp_dir,
        })
    }

    #[cfg(test)]
    /// Consumes the temporary directory the context is linked to.
    pub fn tmp_dir(&self) -> std::sync::Arc<tempfile::TempDir> {
        self.tmp_dir.clone()
    }

    #[inline]
    /// Returns the encryption cipher if enabled.
    pub fn cipher(&self) -> Option<&encrypt::Cipher> {
        self.cipher.as_ref()
    }

    #[inline]
    /// Get the [Encryption] status for the given context.
    pub fn get_encryption_status(&self) -> Encryption {
        if self.cipher.is_none() {
            Encryption::Disabled
        } else {
            Encryption::Enabled
        }
    }

    #[track_caller]
    /// Allocates a new DMA buffer of a given size ([N]) in bytes.
    ///
    /// This will attempt to use an arena allocator first and then fallback
    /// to the system allocator if no space is available.
    pub fn alloc<const N: usize>(&self) -> buffer::DmaBuffer {
        const {
            assert!(
                N % ALLOC_PAGE_SIZE == 0,
                "buffer size is not aligned to a 4kb size"
            )
        };

        if N == 0 {
            return buffer::DmaBuffer::alloc_empty();
        }

        if let Some(alloc) = self.arena_allocator.alloc(N / ALLOC_PAGE_SIZE) {
            buffer::DmaBuffer::from_arena(alloc)
        } else {
            buffer::DmaBuffer::alloc_sys(N / ALLOC_PAGE_SIZE)
        }
    }

    /// Allocates a new DMA buffer of a given a fixed number of pages.
    ///
    /// This will attempt to use an arena allocator first and then fallback
    /// to the system allocator if no space is available.
    ///
    /// Your should prefer [Self::alloc] over this implementation if at all possible.
    pub fn alloc_pages(&self, num_pages: usize) -> buffer::DmaBuffer {
        if num_pages == 0 {
            return buffer::DmaBuffer::alloc_empty();
        }

        if let Some(alloc) = self.arena_allocator.alloc(num_pages) {
            buffer::DmaBuffer::from_arena(alloc)
        } else {
            buffer::DmaBuffer::alloc_sys(num_pages)
        }
    }

    #[inline]
    /// Returns a reference to the [SystemDirectory].
    pub fn directory(&self) -> &SystemDirectory {
        &self.directory
    }

    #[cfg(test)]
    pub(crate) async fn make_tmp_rw_file(
        &self,
        group: crate::directory::FileGroup,
    ) -> crate::file::RWFile {
        let file_id = self.directory.create_new_file(group).await.unwrap();
        self.directory.get_rw_file(group, file_id).await.unwrap()
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::buffer::{ALLOC_PAGE_SIZE, BufferKind};

    #[tokio::test]
    async fn test_encryption_status() {
        let ctx = FileContext::for_test(false).await;
        assert_eq!(ctx.get_encryption_status(), Encryption::Disabled);

        let ctx = FileContext::for_test(true).await;
        assert_eq!(ctx.get_encryption_status(), Encryption::Enabled);
    }

    #[rstest::rstest]
    #[case::empty(0, BufferKind::Empty)]
    #[case::arena(50, BufferKind::Arena)]
    #[case::arena(200, BufferKind::System)]
    #[tokio::test]
    async fn test_alloc_pages(
        #[case] num_pages: usize,
        #[case] backing_type: BufferKind,
    ) {
        let ctx = FileContext::for_test(false).await;

        let buffer = ctx.alloc_pages(num_pages);
        assert_eq!(buffer.len(), ALLOC_PAGE_SIZE * num_pages);
        assert_eq!(buffer.kind(), backing_type);
    }

    #[tokio::test]
    async fn test_const_alloc() {
        let ctx = FileContext::for_test(false).await;

        let buffer = ctx.alloc::<0>();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.kind(), BufferKind::Empty);

        let buffer = ctx.alloc::<{ 50 * ALLOC_PAGE_SIZE }>();
        assert_eq!(buffer.len(), 50 * ALLOC_PAGE_SIZE);
        assert_eq!(buffer.kind(), BufferKind::Arena);

        let buffer = ctx.alloc::<{ 200 * ALLOC_PAGE_SIZE }>();
        assert_eq!(buffer.len(), 200 * ALLOC_PAGE_SIZE);
        assert_eq!(buffer.kind(), BufferKind::System);
    }
}
