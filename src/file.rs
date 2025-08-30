use std::any::{Any, type_name};
use std::fmt::Formatter;
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::directory::{FileId, RingFile};
use crate::buffer::DmaBuffer;

/// The alignment disk read and write lengths must be.
pub const DISK_ALIGN: usize = 4096;

pub type DynamicGuard = Arc<dyn Any + Send + Sync>;
/// A read only file.
pub type ROFile = File<RO>;
/// A write only file.
pub type RWFile = File<RW>;
/// A directory file.
pub type DirFile = File<Dir>;

/// Read mode.
pub struct RO;
impl FileMode for RO {}
impl sealed::Sealed for RO {}

/// Read/write mode.
pub struct RW;
impl FileMode for RW {}
impl sealed::Sealed for RW {}

/// Directory mode.
pub struct Dir;
impl DirMode for Dir {}
impl sealed::Sealed for Dir {}

/// The file mode.
pub trait FileMode: sealed::Sealed {}

/// The directory mode.
pub trait DirMode: sealed::Sealed {}

/// An open file backed by the i2o2 scheduler.
pub struct File<M> {
    file_ref: RingFile,
    handle: i2o2::I2o2Handle<DynamicGuard>,
    write_lockout: Arc<AtomicBool>,
    _mode: PhantomData<M>,
}

impl<M> Clone for File<M> {
    fn clone(&self) -> Self {
        Self {
            file_ref: self.file_ref.clone(),
            handle: self.handle.clone(),
            write_lockout: self.write_lockout.clone(),
            _mode: PhantomData,
        }
    }
}

impl<M> std::fmt::Debug for File<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "File(mode={}, id={:?})", type_name::<M>(), self.id())
    }
}

impl<M> File<M> {
    /// Creates a new file with a set mode.
    pub(super) fn new(
        file_ref: RingFile,
        handle: i2o2::I2o2Handle<DynamicGuard>,
    ) -> Self {
        Self {
            file_ref,
            handle,
            write_lockout: Arc::new(AtomicBool::new(false)),
            _mode: PhantomData,
        }
    }

    /// Returns the unique ID of the file.
    pub fn id(&self) -> FileId {
        self.file_ref.id()
    }
}

impl<M: FileMode> File<M> {
    /// Submit an IOP to read a buffer of a target length.
    ///
    /// # Safety
    ///
    /// You must ensure the pointers remain valid for as long as the scheduler
    /// requires, this means the pointer must be valid until the `guard` (if passed)
    pub async fn submit_read(
        &self,
        buffer: &mut DmaBuffer,
        len: usize,
        offset: u64,
    ) -> io::Result<i2o2::ReplyReceiver> {
        assert!(len < buffer.len());

        #[cfg(test)]
        fail::fail_point!("file::ro::submit_read", crate::utils::parse_io_error_return);

        let guard = buffer.share_guard();
        let op = i2o2::opcode::Read::new(
            i2o2::types::Fixed(self.file_ref.ring_id()),
            buffer.as_mut_ptr(),
            len,
            offset,
        );

        unsafe {
            self.handle
                .submit_async(op, Some(guard))
                .await
                .map_err(io::Error::other)
        }
    }

    /// Attempt to `len` bytes and write the result to [DmaBuffer].
    pub async fn read_buffer(
        &self,
        buffer: &mut DmaBuffer,
        len: usize,
        offset: u64,
    ) -> io::Result<usize> {
        assert!(len < buffer.len());

        #[cfg(test)]
        fail::fail_point!("file::ro::read_buffer", crate::utils::parse_io_error_return);

        let reply = self.submit_read(buffer, len, offset).await?;
        wait_for_reply(reply).await
    }

    /// Returns the true length of the file.
    pub async fn get_len(&self) -> io::Result<u64> {
        #[cfg(test)]
        fail::fail_point!("file::ro::get_len", crate::utils::parse_io_error_return);

        let file = self.file_ref.clone();
        let metadata =
            tokio::task::spawn_blocking(move || file.as_std_file().metadata())
                .await
                .expect("spawn worker thread")?;
        Ok(metadata.len())
    }
}

impl File<RW> {
    /// Ensure data is flushed to the underlying device.
    pub async fn fdatasync(&self) -> io::Result<()> {
        #[cfg(test)]
        fail::fail_point!("file::rw::fdatasync", crate::utils::parse_io_error_return);

        self.ensure_writeable()?;

        let op = i2o2::opcode::Fsync::new(
            i2o2::types::Fixed(self.file_ref.ring_id()),
            i2o2::opcode::FSyncMode::Data,
        );

        let reply = unsafe {
            self.handle
                .submit_async(op, None)
                .await
                .map_err(io::Error::other)?
        };

        let result = wait_for_reply(reply).await;
        if let Err(err) = result {
            self.lockout_writes();
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Submit a write IOP to be processed by the scheduler.
    ///
    /// # Safety
    ///
    /// You must ensure the pointers remain valid for as long as the scheduler
    /// requires, this means the pointer must be valid until the `guard` (if passed)
    /// is dropped by the scheduler.
    pub async fn submit_write(
        &self,
        buffer: &mut DmaBuffer,
        len: usize,
        offset: u64,
    ) -> io::Result<i2o2::ReplyReceiver> {
        assert!(len < buffer.len());

        #[cfg(test)]
        fail::fail_point!(
            "file::rw::submit_write",
            crate::utils::parse_io_error_return
        );

        self.ensure_writeable()?;

        let guard = buffer.share_guard();
        let op = i2o2::opcode::Read::new(
            i2o2::types::Fixed(self.file_ref.ring_id()),
            buffer.as_mut_ptr(),
            len,
            offset,
        );

        unsafe {
            self.handle
                .submit_async(op, Some(guard))
                .await
                .map_err(io::Error::other)
        }
    }

    /// Write the given buffer to the file at the target offset.
    pub async fn write_buffer(
        &self,
        buffer: &mut DmaBuffer,
        offset: u64,
    ) -> io::Result<usize> {
        #[cfg(test)]
        fail::fail_point!(
            "file::rw::write_buffer",
            crate::utils::parse_io_error_return
        );

        let reply = self.submit_write(buffer, buffer.len(), offset).await?;
        wait_for_reply(reply).await
    }

    /// Lockout any future mutations to the file, this is normally
    /// because a fsync error is not safe to retry, and we might lose data
    /// even if we technically _do_ use `O_DIRECT`.
    pub(self) fn lockout_writes(&self) {
        self.write_lockout.store(true, Ordering::Release);
    }

    pub(self) fn ensure_writeable(&self) -> io::Result<()> {
        if self.write_lockout.load(Ordering::Acquire) {
            Err(io::Error::new(
                ErrorKind::ReadOnlyFilesystem,
                "file is read-only due to a prior fsync failure",
            ))
        } else {
            Ok(())
        }
    }
}

impl File<Dir> {
    /// Sync the file directory.
    pub async fn sync(&self) -> io::Result<()> {
        #[cfg(test)]
        fail::fail_point!("file::dir::sync", crate::utils::parse_io_error_return);

        let op = i2o2::opcode::Fsync::new(
            i2o2::types::Fixed(self.file_ref.ring_id()),
            i2o2::opcode::FSyncMode::Data,
        );

        let reply = unsafe {
            self.handle
                .submit_async(op, None)
                .await
                .map_err(io::Error::other)?
        };

        wait_for_reply(reply).await?;
        Ok(())
    }
}

impl From<RWFile> for ROFile {
    fn from(file: RWFile) -> Self {
        Self {
            file_ref: file.file_ref,
            handle: file.handle,
            write_lockout: file.write_lockout,
            _mode: PhantomData,
        }
    }
}

/// Wait for the IOP to complete and convert the result into a [io::Result].
pub async fn wait_for_reply(reply: i2o2::ReplyReceiver) -> io::Result<usize> {
    let result = reply
        .await
        .map_err(|e| io::Error::new(ErrorKind::Interrupted, e))?;
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result as usize)
    }
}

mod sealed {
    pub trait Sealed {}
}
