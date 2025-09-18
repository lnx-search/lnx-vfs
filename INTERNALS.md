

## System Assumptions

- The system is running on Linux of a kernel version 5.15+.
- `io_uring` is enabled.
- `PageId`s are always assigned mapped to data from smallest to largest, meaning the smallest allocated `PageId`
  will always hold the start of the data in a buffer.
- Writes a COW, meaning the page table can never (outside of testing) modify a page in-place, any page writes
  to the page tables under normal operations should be writing to an unassigned page.
- `transaction_id`s are never `u64::MAX` in value.
- A single page group/file will never be modified/updated more than `u32::MAX` times.
- The maximum required alignment for `O_DIRECT` is `4KB`.
- The file system is not insane.

## Durability 

### Fsync & Syscall usage 

You might notice that internally during file writes, we do not call `fdatasync`/`fsync` to flush buffers,
_we do_ however, do this for synchronizing directories when we create/remove files or modify their names
(i.e. for "atomic" files). The only exception is during startup when every file is opened and issued
a `fdatasync` just to be sure.

The reason why we don't do this under normal write conditions is because we open files with 
the `O_DSYNC` & `O_DIRECT` flag (See: [open(2)](https://www.man7.org/linux/man-pages/man2/open.2.html))
which combined give us the same durability guarantees we need with a few notable and desirable behaviours:

- Using `O_DIRECT` hopefully* bypasses the OS page cache which minimises the overhead of flushing the pages
  immediately to the underlying storage. And prevents issues around `fsync` errors where the file systems have
  differing behaviours around what they do with dirty pages in the page cache.
  * (*) NOTE: `O_DIRECT` is not guaranteed to bypass the cache, it is technically possible if the call does not meet
        certain requirements for this to result in writes to the page cache. This does not strictly cause us issues
        for reasons I mention a bit later on.
- Using `O_DSYNC` effectively means every write we do will immediately follow by a flush ensuring the same integrity 
  as an `fdatasync` call would have. BUT, these methods of synchronizing to disk do very different things, in particular
  `O_DSYNC` issues the equivalent of a FUA (Forced Unit Access) request going to the backing storage which differs
  to `fdatasync` which will request a flush of the device's entire write cache if it is not non-volatile/durable.
  * The kernel does _not_ check if the device has a non-volatile cache or not in order to decide if a FUA request is
    issued, instead it is always issued and the device may simply choose to ignore it if it knows better.
  * `O_DSYNC` writes end up being a lot slower latency wise, in particular even on modern NVMEs (with a volatile write cache)
    results in around `1ms` latency, however, `write + fdatasync` is not strictly lower latency, you are simply deferring
    the wait. In the VFS' case, we have built the IO layer with this higher latency in mind and made all operations
    asynchronous and highly concurrent, so even though our single write IOP might take `1ms`, we can still issue 
    thousands of IOPS and maximise device write throughput.
  * As mentioned in the point above, `fdatasync` still ends up having to wait, and during this time it normally issues
    a full flush of the device cache causing a stall in other operations in flight.
    When testing with `i2o2` we saw that `O_DSYNC` can hit the maximum device throughput easily with concurrent IOPS
    while still ensuring durability, while issuing an `fsync` even once every 10 IOPS causes slow-downs and limits
    us to `MB/s`.

### Disabling durability guarantees

You _CANNOT_ disable any sort of durability in the VFS, it is forced on always. If a device has non-volatile write caches
we have confirmed on multiple enterprise drives that they will silently ignore the FUA request.
- I am also not convinced that having a non-volatile write cache means you can simply not issue these sync 
  operations (either `fsync`/`fdatasync` or `O_SYNC`/`O_DSYNC`) as there is nothing (as far as I am aware) 
  preventing file systems from deferring metadata updates of files (be it length, name, etc...) until the 
  sync call is issued. Meaning I still believe data loss is possible when not issuing sync calls.

### Data layout

The system internally must align all writes to the logical block size of the device, which is normally either `512`
or `4096`bytes (4KB), in the VFS' case, we always issue writes aligned to 4KB both in terms of memory alignment 
and position and length alignment.

We _also_ assume that we only have torn-write protection on `512` byte writes, anything less than, or above
this size we assume can have a torn write take place. 

This combination means a lot of things are serialized and fix within a `512` byte buffer, which we maintain
as we pack multiples of those buffers into the buffer we're about to write to disk.
Meaning if we have a struct that is serialized to `450` bytes, we will pad it to `512` to ensure we keep alignment 
internally and do not run into corruption in the event that a torn-write takes place.

The only place where torn-write awareness truly matters is in the WAL / Page Operation Log as that is
our log of metadata operations that need to be recovered and re-applied on restart or recovery after a crash.

### References Used

- [Linux man pages for `open(2)` & `O_DSYNC`](https://www.man7.org/linux/man-pages/man2/open.2.html)
- [PostgreSQL's Fsync Errors page](https://wiki.postgresql.org/wiki/Fsync_Errors)
- ["Can Applications Recover from fsync Failures?" - CuttleFS Paper](https://dl.acm.org/doi/fullHtml/10.1145/3450338)
- [The XFS source code](https://elixir.bootlin.com/linux/v6.14.11/source/fs/xfs)
- [The EXT4 source code](https://elixir.bootlin.com/linux/v6.14.11/source/fs/ext4)
