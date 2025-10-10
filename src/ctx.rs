use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::arena::ArenaAllocator;
use crate::buffer;
use crate::buffer::ALLOC_PAGE_SIZE;
use crate::directory::SystemDirectory;
use crate::layout::encrypt;
use crate::layout::file_metadata::Encryption;

/// Create a new [Context] instance with options.
pub struct ContextBuilder {
    base_path: PathBuf,
    encryption_key: Option<String>,
    io_memory: usize,
}

impl ContextBuilder {
    /// Creates a new [ContextBuilder] with a provided base path.
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            encryption_key: None,
            io_memory: 32 << 20,
        }
    }

    /// Set or unset the encryption key for the system.
    ///
    /// If data already exists, the system will error if the config passed in here
    /// does not match what is enabled on the existing system.
    ///
    /// For example, you cannot open an existing unencrypted system with context that
    /// has an encryption key set. And you cannot open an encrypted system with context
    /// that is missing the encryption key.
    pub fn with_encryption_key(mut self, encryption_key: Option<String>) -> Self {
        self.encryption_key = encryption_key;
        self
    }

    /// The size of the IO memory arena in bytes.
    ///
    /// System will fall back to the system allocator when ever it needs to do an allocation
    /// and the arena is full.
    ///
    /// By default, this is `32MB`.
    pub fn io_memory_arena_size(mut self, size: usize) -> Self {
        self.io_memory = size;
        self
    }

    /// Open the context.
    pub async fn open(self) -> io::Result<Context> {
        static GUARD_FILE: &str = ".lnx-vfs-guard";
        let guard_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.base_path.join(GUARD_FILE))?;

        guard_file.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::ResourceBusy,
                "VFS already open on this directory",
            )
        })?;
        tracing::info!("acquired guard file lock");

        let directory = SystemDirectory::open(&self.base_path).await?;

        let cipher = if let Some(encryption_key) = self.encryption_key {
            let keys = super::encryption_file::load_encryption_keys(
                &self.base_path,
                &encryption_key,
            )
            .await
            .map_err(io::Error::other)?;
            Some(keys.encryption_cipher)
        } else {
            None
        };

        let num_arena_pages = self.io_memory / ALLOC_PAGE_SIZE;
        let ctx = Context {
            cipher,
            arena_allocator: ArenaAllocator::new(num_arena_pages),
            directory,
            configs: parking_lot::RwLock::new(BTreeMap::new()),
            _guard_file: guard_file,
            #[cfg(test)]
            tmp_dir: std::sync::Arc::new(tempfile::tempdir()?),
        };

        Ok(ctx)
    }
}

/// The file context contains general settings and information for the system to use.
pub struct Context {
    cipher: Option<encrypt::Cipher>,
    arena_allocator: ArenaAllocator,
    directory: SystemDirectory,
    configs: parking_lot::RwLock<BTreeMap<TypeId, Box<dyn Any + Send + Sync>>>,
    _guard_file: std::fs::File,
    #[cfg(test)]
    tmp_dir: std::sync::Arc<tempfile::TempDir>,
}

impl Context {
    #[cfg(test)]
    /// Create a new file context for testing.
    pub(crate) async fn for_test(encryption: bool) -> std::sync::Arc<Self> {
        let tmp_dir = tempfile::tempdir().unwrap();
        Self::for_test_in_dir(encryption, std::sync::Arc::new(tmp_dir)).await
    }

    #[cfg(test)]
    /// Create a new file context for testing.
    pub(crate) async fn for_test_in_dir(
        encryption: bool,
        tmp_dir: std::sync::Arc<tempfile::TempDir>,
    ) -> std::sync::Arc<Self> {
        use chacha20poly1305::KeyInit;

        let cipher = if encryption {
            let key =
                encrypt::CipherKey::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
            Some(encrypt::Cipher::new(key))
        } else {
            None
        };

        let directory = SystemDirectory::open(tmp_dir.path())
            .await
            .expect("open system directory");

        let guard_file = tempfile::tempfile().unwrap();

        std::sync::Arc::new(Self {
            cipher,
            arena_allocator: ArenaAllocator::new(100),
            directory,
            configs: parking_lot::RwLock::new(BTreeMap::new()),
            _guard_file: guard_file,
            tmp_dir,
        })
    }

    #[cfg(test)]
    /// Consumes the temporary directory the context is linked to.
    pub(crate) fn tmp_dir(&self) -> std::sync::Arc<tempfile::TempDir> {
        self.tmp_dir.clone()
    }

    #[inline]
    /// Returns the encryption cipher if enabled.
    pub(crate) fn cipher(&self) -> Option<&encrypt::Cipher> {
        self.cipher.as_ref()
    }

    #[inline]
    /// Get the [Encryption] status for the given context.
    pub(crate) fn get_encryption_status(&self) -> Encryption {
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
    pub(crate) fn alloc<const N: usize>(&self) -> buffer::DmaBuffer {
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
    pub(crate) fn alloc_pages(&self, num_pages: usize) -> buffer::DmaBuffer {
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
    pub(crate) fn directory(&self) -> &SystemDirectory {
        &self.directory
    }

    /// Set a config value in the context.
    pub fn set_config<C: Any + Send + Sync + Clone>(&self, config: C) {
        let config_value = Box::new(config) as Box<dyn Any + Send + Sync>;
        self.configs.write().insert(TypeId::of::<C>(), config_value);
    }

    #[track_caller]
    /// Get a config value from the context.
    ///
    /// Panics if the config does not exist.
    pub(crate) fn config<C: Any + Send + Sync + Clone>(&self) -> C {
        self.config_opt().expect("config not initialized")
    }

    #[track_caller]
    /// Get a config value from the context.
    ///
    /// Panics if the config does not exist.
    pub(crate) fn config_opt<C: Any + Send + Sync + Clone>(&self) -> Option<C> {
        self.configs
            .read()
            .get(&TypeId::of::<C>())
            .and_then(|config| config.downcast_ref())
            .cloned()
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
        let ctx = Context::for_test(false).await;
        assert_eq!(ctx.get_encryption_status(), Encryption::Disabled);

        let ctx = Context::for_test(true).await;
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
        let ctx = Context::for_test(false).await;

        let buffer = ctx.alloc_pages(num_pages);
        assert_eq!(buffer.len(), ALLOC_PAGE_SIZE * num_pages);
        assert_eq!(buffer.kind(), backing_type);
    }

    #[tokio::test]
    async fn test_const_alloc() {
        let ctx = Context::for_test(false).await;

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

    #[tokio::test]
    async fn test_config_system_opt() {
        let ctx = Context::for_test(false).await;
        assert!(ctx.config_opt::<()>().is_none());
        ctx.set_config(());
        assert!(ctx.config_opt::<()>().is_some());
    }

    #[should_panic(expected = "config not initialized")]
    #[tokio::test]
    async fn test_config_system_config_panics() {
        let ctx = Context::for_test(false).await;
        ctx.config::<()>();
    }
}
