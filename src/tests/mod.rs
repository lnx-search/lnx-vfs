use std::io;
use std::path::Path;

mod cache_layer;
mod dir_basic;
mod dir_fault_injection;
mod stream_reader;

/// A utility for getting the files in a directory and string paths.
fn list_files(base_path: &Path) -> io::Result<Vec<String>> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(base_path)? {
        let entry = entry?;
        let path = entry.path();

        let cleaned = path.strip_prefix(base_path).unwrap();
        files.push(cleaned.to_str().unwrap().to_string());
    }

    Ok(files)
}

#[rstest::fixture]
pub fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create temp dir")
}
