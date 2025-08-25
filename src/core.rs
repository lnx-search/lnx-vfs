use std::ops::Range;

#[allow(async_fn_in_trait)]
/// The core file system operations that can be performed on the VFS.
pub trait FileSystemCore {
    /// Create a new file writer.
    ///
    /// This allows for streaming of the data rather than a full in-memory copy.
    async fn create_writer(&self, fp: &str);

    /// Create a new file reader.
    ///
    /// This allows for streaming of the data rather than a full in-memory copy.
    ///
    /// NOTE: This will still go via the cache.
    async fn create_reader(&self, fp: &str);

    /// Read a new from disk into memory.
    async fn read(&self, fp: &str, range: Range<u64>);

    /// Write a file at the given file path with the target data.
    async fn write(&self, fp: &str, data: &[u8]);

    /// Remove an existing file at the given path.
    async fn remove(&self, fp: &str);

    /// Rename a file at a given path and move it to a new path.
    async fn rename(&self, old_fp: &str, new_fp: &str);
}