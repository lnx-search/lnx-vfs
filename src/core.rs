use std::ops::Range;

#[allow(async_fn_in_trait)]
/// The core file system operations that can be performed on the VFS.
pub trait FileSystemCore {
    /// Create a new file writer.
    ///
    /// This allows for streaming of the data rather than a full in-memory copy.
    async fn create_writer(&self, file_id: u64);

    /// Create a new file reader.
    ///
    /// This allows for streaming of the data rather than a full in-memory copy.
    ///
    /// NOTE: This will still go via the cache.
    async fn create_reader(&self, file_id: u64);

    /// Read a new from disk into memory.
    async fn read(&self, file_id: u64, range: Range<u64>);

    /// Write a file at the given file path with the target data.
    async fn write(&self, file_id: u64, data: &[u8]);

    /// Remove an existing file at the given path.
    async fn remove(&self, file_id: u64);

    /// Rename a file at a given path and move it to a new path.
    async fn rename(&self, new_file_id: u64, old_file_id: u64);
}
