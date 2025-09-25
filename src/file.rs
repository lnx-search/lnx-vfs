use std::any::{Any, type_name};
use std::fmt::Formatter;
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::sync::Arc;

use i2o2::TryGetResultError;

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
    _mode: PhantomData<M>,
}

impl<M> Clone for File<M> {
    fn clone(&self) -> Self {
        Self {
            file_ref: self.file_ref.clone(),
            handle: self.handle.clone(),
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
    pub unsafe fn submit_read(
        &self,
        buffer_ptr: *mut u8,
        len: usize,
        offset: u64,
        guard: Option<DynamicGuard>,
    ) -> impl Future<Output = io::Result<i2o2::ReplyReceiver>> + Send + '_ {
        let op = i2o2::opcode::Read::new(
            i2o2::types::Fixed(self.file_ref.ring_id()),
            buffer_ptr,
            len,
            offset,
        );

        async {
            #[cfg(test)]
            fail::fail_point!(
                "file::ro::submit_read",
                crate::utils::parse_io_error_return
            );

            unsafe {
                self.handle
                    .submit_async(op, guard)
                    .await
                    .map_err(io::Error::other)
            }
        }
    }

    /// Attempt to `len` bytes and write the result to [DmaBuffer].
    pub async fn read_buffer(
        &self,
        buffer: &mut DmaBuffer,
        offset: u64,
    ) -> io::Result<usize> {
        debug_assert_eq!(buffer.len() % DISK_ALIGN, 0);

        #[cfg(test)]
        fail::fail_point!("file::ro::read_buffer", crate::utils::parse_io_error_return);

        let buffer_guard = buffer.share_guard();
        let buffer_ptr = buffer.as_mut_ptr();
        let buffer_len = buffer.len();

        let reply = unsafe {
            self.submit_read(buffer_ptr, buffer_len, offset, Some(buffer_guard))
                .await?
        };
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
    /// Submit a write IOP to be processed by the scheduler.
    ///
    /// Writes will be durable once the reply is returned.
    ///
    /// # Safety
    ///
    /// You must ensure the pointers remain valid for as long as the scheduler
    /// requires, this means the pointer must be valid until the `guard` (if passed)
    /// is dropped by the scheduler.
    pub unsafe fn submit_write(
        &self,
        buffer_ptr: *const u8,
        len: usize,
        offset: u64,
        guard: Option<DynamicGuard>,
    ) -> impl Future<Output = io::Result<i2o2::ReplyReceiver>> + Send + '_ {
        let op = i2o2::opcode::Write::new(
            i2o2::types::Fixed(self.file_ref.ring_id()),
            buffer_ptr,
            len,
            offset,
        );

        async move {
            #[cfg(test)]
            fail::fail_point!(
                "file::rw::submit_write",
                crate::utils::parse_io_error_return
            );

            unsafe {
                self.handle
                    .submit_async(op, guard)
                    .await
                    .map_err(io::Error::other)
            }
        }
    }

    /// Write N bytes of a given buffer to the file at the target offset.
    ///
    /// Writes will be durable once this function returns.
    pub async fn write_buffer(
        &self,
        buffer: &mut DmaBuffer,
        offset: u64,
    ) -> io::Result<usize> {
        debug_assert_eq!(buffer.len() % DISK_ALIGN, 0);

        #[cfg(test)]
        fail::fail_point!(
            "file::rw::write_buffer",
            crate::utils::parse_io_error_return
        );

        let buffer_guard = buffer.share_guard();
        let buffer_ptr = buffer.as_ptr();
        let buffer_len = buffer.len();

        let reply = unsafe {
            self.submit_write(buffer_ptr, buffer_len, offset, Some(buffer_guard))
                .await?
        };
        wait_for_reply(reply).await
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
            _mode: PhantomData,
        }
    }
}

/// Wait for the IOP to complete and convert the result into a [io::Result].
pub async fn wait_for_reply(reply: i2o2::ReplyReceiver) -> io::Result<usize> {
    let result = if let Ok(result) = reply.try_get_result() {
        result
    } else {
        reply
            .await
            .map_err(|e| io::Error::new(ErrorKind::BrokenPipe, e))?
    };

    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result as usize)
    }
}

/// Attempt to get the result from the reply without waiting.
pub fn try_get_reply(reply: &i2o2::ReplyReceiver) -> io::Result<usize> {
    match reply.try_get_result() {
        Ok(result) if result < 0 => Err(io::Error::from_raw_os_error(-result)),
        Ok(result) => Ok(result as usize),
        Err(TryGetResultError::Cancelled) => Err(io::Error::new(
            ErrorKind::BrokenPipe,
            TryGetResultError::Cancelled,
        )),
        Err(TryGetResultError::Pending) => Err(io::Error::from(ErrorKind::WouldBlock)),
    }
}

mod sealed {
    pub trait Sealed: Send + Sync + 'static {}
}
