use std::io::ErrorKind;
use std::sync::Arc;
use std::{cmp, io};

use crate::buffer::DmaBuffer;
use crate::{ctx, directory, file};

pub const READ_BUFFER_SIZE: usize = 32 << 10;
const MAX_READ_RETRY: usize = 2;

/// A builder for creating new [StreamReader]s.
pub struct StreamReaderBuilder {
    ctx: Arc<ctx::FileContext>,
    file: file::ROFile,
    offset: u64,
    read_buffer_size: usize,
}

impl StreamReaderBuilder {
    /// Create a new [StreamReaderBuilder] using the target file.
    pub fn new(ctx: Arc<ctx::FileContext>, file: impl Into<file::ROFile>) -> Self {
        Self {
            ctx,
            file: file.into(),
            offset: 0,
            read_buffer_size: READ_BUFFER_SIZE,
        }
    }

    /// Start the reader from a target offset.
    ///
    /// This must be a multiple of [DISK_ALIGN].
    pub fn with_offset(mut self, offset: u64) -> Self {
        assert_eq!(
            offset % file::DISK_ALIGN as u64,
            0,
            "offset must be a multiple of DISK_ALIGN"
        );
        self.offset = offset;
        self
    }

    /// Set the buffer size used for each read IOP.
    ///
    /// This must be a multiple of [DISK_ALIGN].
    ///
    /// Defaults to 32KB.
    pub fn with_buffer_size(mut self, read_buffer_size: usize) -> Self {
        assert_ne!(read_buffer_size, 0, "buffer size must not be zero");
        assert_eq!(
            read_buffer_size % file::DISK_ALIGN,
            0,
            "buffer size must a multiple of DISK_ALIGN"
        );
        self.read_buffer_size = read_buffer_size;
        self
    }

    /// Build a new reader using the configured options.
    pub fn build(self) -> StreamReader {
        let read_buffer = self
            .ctx
            .alloc_pages(self.read_buffer_size / file::DISK_ALIGN);

        StreamReader {
            file: self.file,

            file_len: 0,
            file_len_init: false,
            file_cursor: self.offset,

            position: self.offset,

            read_buffer,
            read_buffer_cursor: 0,
            read_buffer_init_end: 0,
        }
    }
}

/// A reader for scanning through a [file::ROFile].
pub struct StreamReader {
    file: file::ROFile,

    file_len: u64,
    file_len_init: bool,
    file_cursor: u64,

    position: u64,

    read_buffer: DmaBuffer,
    read_buffer_cursor: usize,
    read_buffer_init_end: usize,
}

impl StreamReader {
    #[inline]
    /// Returns the unique ID assigned to the file.
    pub fn file_id(&self) -> directory::FileId {
        self.file.id()
    }

    #[inline]
    /// Returns the position the reader is at in the file.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Read N bytes from the reader and copy the data into the buffer.
    ///
    /// This returns the number of bytes read.
    pub async fn read_n(&mut self, output: &mut Vec<u8>, n: usize) -> io::Result<usize> {
        if !self.file_len_init {
            self.file_len = self.file.get_len().await?;
            self.file_len_init = true;
        }

        let mut read_n = n;
        loop {
            let n = self.fill_using_last_read(output, read_n);
            read_n -= n;

            if read_n == 0 {
                self.position += output.len() as u64;
                return Ok(output.len());
            }

            // EOF
            if self.file_cursor >= self.file_len {
                let n = output.len() - read_n;
                self.position += n as u64;
                return Ok(n);
            }

            self.fill_buffer().await?;
        }
    }

    async fn fill_buffer(&mut self) -> io::Result<()> {
        let expected_len = cmp::min(
            (self.file_len - self.file_cursor) as usize,
            self.read_buffer.len(),
        );
        let mut result = self.try_fill_read_buffer().await?;

        // Retry the rare but possible short reads.
        let mut attempt = 0;
        while result != expected_len {
            attempt += 1;

            // It is incredibly rare for a read to return a size smaller
            // than request outside EOF, but we should retry if we get
            // one at least once, and then error if it happens again.
            if attempt > MAX_READ_RETRY {
                return Err(io::Error::new(
                    ErrorKind::BrokenPipe,
                    "kernel read returned small buffer after retries",
                ));
            }

            result = self.try_fill_read_buffer().await?;
        }

        self.file_cursor += expected_len as u64;
        self.read_buffer_cursor = 0;
        self.read_buffer_init_end = expected_len;

        Ok(())
    }

    async fn try_fill_read_buffer(&mut self) -> io::Result<usize> {
        self.file
            .read_buffer(&mut self.read_buffer, self.file_cursor)
            .await
    }

    fn remaining_buffer(&self) -> &[u8] {
        &self.read_buffer[self.read_buffer_cursor..self.read_buffer_init_end]
    }

    fn fill_using_last_read(&mut self, output: &mut Vec<u8>, limit: usize) -> usize {
        let buffered_data = self.remaining_buffer();
        let take_n = cmp::min(limit, buffered_data.len());
        output.extend_from_slice(&buffered_data[..take_n]);
        self.read_buffer_cursor += take_n;
        take_n
    }
}
