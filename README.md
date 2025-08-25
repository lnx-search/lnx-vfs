# lnx VFS

The lnx Virtual File System.

This system performs smarter caching for read heavy IO workloads, supports encryption at rest
and makes heavy use of io_uring for more efficient IOPS.

This system is still fairly greenfield, hence why some optimisations for small files are currently missing.

## Features

- 100% Durable file operations.
    * Every operation ensures updates are durable before returning.
- LFU based page cache.
- Reads produced single contiguous slices without additional clones.
- Encryption at rest

## Design

The system is more or less only a light abstraction around a WAL for metadata operations
and the file system with calls being issues via io_uring.

All write and read operations are performed using `O_DIRECT`, so it is _not_ recommended 
to read files managed by this file system using external tools at your page cache entries may
not be updated correctly.
