use std::collections::BTreeMap;
use std::io;
use std::io::ErrorKind;
use std::ops::Deref;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::file;
use crate::file::DynamicGuard;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
/// A unique identifier for a file.
pub struct FileId(u32);

/// The default expected ref count if there are no other
/// access to the file other than:
///
/// - The directory
/// - The i2o2 scheduler
const DEFAULT_FILE_REF_COUNT: usize = 2;
const MAX_REGISTERED_FILES: u32 = 32_000;

#[derive(Debug, Copy, Clone)]
/// The [FileGroup] determines where a file is stored
/// and what validations are applied.
pub enum FileGroup {
    /// File contains page data.
    PageData,
    /// File contains a set of page metadata updates in the form of a checkpoint.
    PageTableCheckpoint,
    /// File contains a WAL of page metadata operations being applied.
    Wal,
}

impl FileGroup {
    fn extension(&self) -> &'static str {
        match self {
            FileGroup::PageData => "dat.lnx",
            FileGroup::PageTableCheckpoint => "pts.lnx",
            FileGroup::Wal => "wal.lnx",
        }
    }

    fn idx(&self) -> usize {
        match self {
            FileGroup::PageData => 0,
            FileGroup::PageTableCheckpoint => 1,
            FileGroup::Wal => 2,
        }
    }
}

#[derive(Clone)]
/// The [SystemDirectory] tracks all files within the VFS storage system and
/// managed creation, deletion and cleanup of files.
///
///
pub struct SystemDirectory(Arc<SystemDirectoryInner>);

impl Deref for SystemDirectory {
    type Target = SystemDirectoryInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SystemDirectory {
    /// Open the [SystemDirectory] at a given file path.
    ///
    /// If the folder does not exist, it will be created.
    pub async fn open(base_path: &Path) -> io::Result<Self> {
        create_base_dir(base_path).await?;

        let groups = [
            FileGroupDirectory::open(FileGroup::PageData, base_path)
                .await
                .map(tokio::sync::RwLock::new)?,
            FileGroupDirectory::open(FileGroup::PageTableCheckpoint, base_path)
                .await
                .map(tokio::sync::RwLock::new)?,
            FileGroupDirectory::open(FileGroup::Wal, base_path)
                .await
                .map(tokio::sync::RwLock::new)?,
        ];

        let inner = SystemDirectoryInner { groups };

        Ok(Self(Arc::new(inner)))
    }
}

pub struct SystemDirectoryInner {
    groups: [tokio::sync::RwLock<FileGroupDirectory>; 3],
}

impl SystemDirectoryInner {
    /// Create a new file in the target group.
    pub async fn create_new_file(&self, group: FileGroup) -> io::Result<FileId> {
        let mut directory = self.groups[group.idx()].write().await;
        let file_id = directory.create_new_file().await?;
        Ok(FileId(file_id))
    }

    /// Remove an existing file in the target group.
    ///
    /// The file must not currently be in use by any other systems otherwise this call
    /// will error.
    pub async fn remove_file(
        &self,
        group: FileGroup,
        file_id: FileId,
    ) -> io::Result<()> {
        let mut directory = self.groups[group.idx()].write().await;
        directory.remove_file(file_id.0).await
    }

    /// Get a read only reference to the target file in a given group.
    pub async fn get_ro_file(
        &self,
        group: FileGroup,
        file_id: FileId,
    ) -> io::Result<file::ROFile> {
        let directory = self.groups[group.idx()].read().await;
        directory.get_ro_file(file_id.0)
    }

    /// Get a read/write reference to the target file in a group.
    pub async fn get_rw_file(
        &self,
        group: FileGroup,
        file_id: FileId,
    ) -> io::Result<file::RWFile> {
        let directory = self.groups[group.idx()].read().await;
        directory.get_rw_file(file_id.0)
    }

    /// List all valid files in the directory.
    ///
    /// A list of the file IDs in the group is returned which are already open.
    pub async fn list_dir(&self, group: FileGroup) -> Vec<FileId> {
        let directory = self.groups[group.idx()].read().await;
        directory.list_dir().map(FileId).collect()
    }

    /// Counts the number of currently open files in the directory.
    pub async fn num_open_files(&self, group: FileGroup) -> usize {
        let directory = self.groups[group.idx()].read().await;
        directory.num_open_files()
    }
}

/// An inner directory within the [SystemDirectory] that stores
/// file specifically for a given [FileGroup].
///
/// Each directory is backed by a separate [i2o2::I2o2Scheduler] to
/// separate IO priorities.
struct FileGroupDirectory {
    file_group: FileGroup,
    handle: i2o2::I2o2Handle<DynamicGuard>,
    runtime_handle: std::thread::JoinHandle<io::Result<()>>,
    directory_file: file::DirFile,
    base_path: PathBuf,
    files: BTreeMap<u32, RingFile>,
}

impl FileGroupDirectory {
    /// Open a [FileGroupDirectory] targeting a specific file group located within
    /// the parent base path.
    async fn open(file_group: FileGroup, parent_path: &Path) -> io::Result<Self> {
        let base_path = match file_group {
            FileGroup::PageData => parent_path.join("data"),
            FileGroup::PageTableCheckpoint => parent_path.join("metadata"),
            FileGroup::Wal => parent_path.join("wal"),
        };

        tokio::task::spawn_blocking(move || Self::open_inner(file_group, base_path))
            .await
            .expect("spawn worker thread")
    }

    #[tracing::instrument("open", skip(base_path))]
    fn open_inner(file_group: FileGroup, base_path: PathBuf) -> io::Result<Self> {
        tracing::info!(path = %base_path.display(), "opening directory");

        match std::fs::create_dir(&base_path) {
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {},
            other => other?,
        }

        tracing::info!("creating i2o2 runtime");
        let (runtime_handle, handle) = i2o2::builder()
            .skip_unsupported_features(true)
            .with_coop_task_run(true)
            .with_num_registered_files(MAX_REGISTERED_FILES)
            .try_spawn::<DynamicGuard>()?;

        tracing::info!("opening ring directory");
        let directory_file = open_ring_directory(&base_path, handle.clone())?;

        let mut this = Self {
            file_group,
            handle,
            runtime_handle,
            directory_file,
            base_path,
            files: BTreeMap::new(),
        };

        tracing::info!("opening existing files");
        for (id, path) in list_files(file_group, &this.base_path)? {
            let file = open_file(&path).map(Arc::new)?;

            let fd = file.as_raw_fd();
            let ring_id = this
                .handle
                .register_file(fd, Some(file.clone() as DynamicGuard))
                .map_err(io::Error::other)?;

            let ring_file = RingFile {
                id,
                ring_id,
                inner: file,
            };

            this.files.insert(ring_id, ring_file);
        }

        Ok(this)
    }

    fn list_dir(&self) -> impl Iterator<Item = u32> + '_ {
        self.files.keys().copied()
    }

    /// Returns the number of open files within the directory.
    fn num_open_files(&self) -> usize {
        self.files.len()
    }

    /// Get the file path of a file with a given ID.
    fn file_path(&self, file_id: u32) -> PathBuf {
        let file_name =
            format!("{file_id:010}-{file_id}.{}", self.file_group.extension());
        self.base_path.join(file_name)
    }

    /// Create a new file in the directory.
    ///
    /// A unique ID will be assigned to the file.
    async fn create_new_file(&mut self) -> io::Result<u32> {
        let assigned_id = self.get_next_file_id();

        let file_path = self.file_path(assigned_id);
        let file = create_file(&file_path).await.map(Arc::new)?;

        let fut = async {
            self.directory_file.sync().await?;
            self.register_file_with_ring(file.clone()).await
        };

        let result = fut.await;
        let ring_id = match result {
            Ok(ring_id) => ring_id,
            Err(err) => {
                drop(file);
                let _ = remove_file(&file_path).await;
                return Err(err);
            },
        };

        let ring_file = RingFile {
            id: assigned_id,
            ring_id,
            inner: file,
        };

        let old_value = self.files.insert(assigned_id, ring_file.clone());
        assert!(
            old_value.is_some(),
            "BUG! Inserted file that already existed"
        );

        Ok(assigned_id)
    }

    /// Remove an existing file.
    ///
    /// Does nothing if the file does not exist.
    async fn remove_file(&mut self, file_id: u32) -> io::Result<()> {
        let Some(file_ref) = self.files.remove(&file_id) else {
            return Ok(());
        };

        let ref_count = file_ref.ref_count();
        if ref_count > DEFAULT_FILE_REF_COUNT {
            return Err(io::Error::new(
                ErrorKind::ResourceBusy,
                "file is still in use",
            ));
        }

        self.unregister_file_with_ring(&file_ref).await?;

        let file_path = self.file_path(file_id);
        remove_file(&file_path).await?;
        self.directory_file.sync().await
    }

    /// Get a read-only file reference with the given ID.
    fn get_ro_file(&self, file_id: u32) -> io::Result<file::ROFile> {
        let file_ref = self
            .files
            .get(&file_id)
            .cloned()
            .ok_or_else(|| io::Error::from(ErrorKind::NotFound))?;
        Ok(file::ROFile::new(file_ref, self.handle.clone()))
    }

    /// Get a read/write file reference with the given ID.
    ///
    /// WARNING: This does not prevent you shooting your own feet off, you can
    /// create multiple mutable writers to the same file unless you are careful.
    fn get_rw_file(&self, file_id: u32) -> io::Result<file::RWFile> {
        let file_ref = self
            .files
            .get(&file_id)
            .cloned()
            .ok_or_else(|| io::Error::from(ErrorKind::NotFound))?;
        Ok(file::RWFile::new(file_ref, self.handle.clone()))
    }

    /// Get the next sequential ID to assign to a new file.
    fn get_next_file_id(&mut self) -> u32 {
        let last_file_id = self.files.keys().last().copied().unwrap_or(1000);
        last_file_id + 1
    }

    async fn register_file_with_ring(
        &mut self,
        file: Arc<std::fs::File>,
    ) -> io::Result<u32> {
        let fd = file.as_raw_fd();
        self.handle
            .register_file_async(fd, Some(file as DynamicGuard))
            .await
            .map_err(io::Error::other)
    }

    async fn unregister_file_with_ring(&mut self, file: &RingFile) -> io::Result<()> {
        self.handle
            .unregister_file_async(file.ring_id())
            .await
            .map_err(io::Error::other)
    }
}

#[derive(Clone)]
/// An open file that is registered with the io_uring ring.
pub struct RingFile {
    id: u32,
    ring_id: u32,
    inner: Arc<std::fs::File>,
}

impl RingFile {
    #[inline]
    /// The unique ID assigned to the file.
    pub fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    /// Returns the ID assigned to the file by the ring.
    pub fn ring_id(&self) -> u32 {
        self.ring_id
    }

    #[inline]
    /// Returns the file descriptor of the file.
    pub fn fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }

    #[inline]
    /// Returns the inner file reference.
    pub fn as_std_file(&self) -> &std::fs::File {
        &self.inner
    }

    #[inline]
    /// Returns the current strong count of the ring file.
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }
}

const FILE_FLAGS: libc::c_int = libc::O_DIRECT | libc::O_CLOEXEC;

async fn create_file(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .custom_flags(FILE_FLAGS)
            .open(&path)
    })
    .await
    .expect("spawn worker thread")
}

async fn remove_file(path: &Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::remove_file(&path))
        .await
        .expect("spawn worker thread")
}

async fn create_base_dir(path: &Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::create_dir_all(&path))
        .await
        .expect("spawn worker thread")
}

fn open_file(path: &Path) -> io::Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .custom_flags(FILE_FLAGS)
        .open(&path)?;

    // On open, we always issue a full fsync to ensure we don't have random
    // fragments sitting around from a process crash.
    file.sync_all()?;

    Ok(file)
}

fn open_ring_directory(
    path: &Path,
    handle: i2o2::I2o2Handle<DynamicGuard>,
) -> io::Result<file::DirFile> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .map(Arc::new)?;

    let fd = file.as_raw_fd();
    let ring_id = handle
        .register_file(fd, Some(file.clone() as DynamicGuard))
        .map_err(io::Error::other)?;

    let ring_file = RingFile {
        id: 0,
        ring_id,
        inner: file,
    };

    Ok(file::DirFile::new(ring_file, handle))
}

#[tracing::instrument(skip(path))]
fn list_files(file_group: FileGroup, path: &Path) -> io::Result<Vec<(u32, PathBuf)>> {
    let mut file_ids = Vec::new();

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let path = entry.path();

        if metadata.is_dir() {
            tracing::warn!(
                path = %path.display(),
                "unexpected directory present in system files",
            );
            continue;
        }

        let raw_file_name = entry.file_name();
        let file_name = raw_file_name.to_string_lossy();
        if !file_name.ends_with(file_group.extension()) {
            tracing::warn!(
                path = %path.display(),
                "unexpected file present in system files",
            );
            continue;
        }

        let (_sort_id, remaining) = file_name.split_once('-').unwrap();
        let (file_id, _remaining) = remaining.split_once('.').unwrap();

        let file_id = file_id.parse::<u32>().map_err(|e| {
            io::Error::new(ErrorKind::Other, format!("invalid file id present: {e}"))
        })?;

        // if the file is empty, try remove it
        if metadata.len() == 0 {
            if let Err(err) = std::fs::remove_file(&path) {
                tracing::warn!(error = %err, "cannot remove empty file");
            }
            continue;
        }

        file_ids.push((file_id, path));
    }

    Ok(file_ids)
}
