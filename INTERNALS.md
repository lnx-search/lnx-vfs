

## System Assumptions

- The system is running on Linux of a kernel version 5.15+.
- `io_uring` is enabled.
- The maximum required alignment for `O_DIRECT` is `4KB`.
- The file system is not insane.


## Implementation Assumptions

These are assumptions other components of the system might make, and the system itself is responsible to 
ensure is correct.

- Writes a COW, meaning the page table can never (outside of testing) modify a page in-place, any page writes
  to the page tables under normal operations should be writing to an unassigned page.
- `transaction_id`s are never `u64::MAX` in value.
- `PageId`s are always assigned mapped to data from smallest to largest, meaning the smallest allocated `PageId`
  will always hold the start of the data in a buffer.


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

### IO error playbook

File system error handling is an incredibly dangerous game, there are many ways you can trip up and accidentally
shoot yourself in the foot.

We define the error handling rules for each component here:

#### Page file IO

Page file IO may be retried for both reads and writes without issue providing write errors are treated  
as a total failure and must be retried in their entirety.

Page files should be created via the [atomic API in the directory system](#atomic-file-creation).

#### Checkpoint file IO

Checkpoints must be created via the [atomic API in the directory system](#atomic-file-creation), once
all data is written and persisted they can be made durable.

An error on the checkpoint file write can be handled gracefully, however, old checkpoint files must not
be removed until the new checkpoint file is confirmed to be durable and persisted.

#### WAL file IO

Checkpoints must be created via the [atomic API in the directory system](#atomic-file-creation), once
all data is written and persisted they can be made durable.

Particular care must be taken here as this is the file IO stage before an operation is either
confirmed as durable or rejected.

If an IO error occurs on the WAL:
1. The WAL file must be locked out preventing any subsequent writes
from being completed.
2. Then the WAL file must attempt to be truncated back to the original size and a fsync issued.
   - If this operation is successful, the WAL file can be rotated and writes may continue.
   - If the operation fails, it may be retried twice more with exponential backoff. 
     - Note: The truncate and fsync must _both_ be tried.
   - If the operation continues to fail, the process must abort. 
     - This is because we cannot ensure the memory state remains consistent with the disk state.


### Atomic file creation

We create "atomic" files by following the given set of steps:

- Create new file with `.atomic` suffix.
- `fsync` file
- `fsync` parent directory
- *Allow downstream system to initialise the file with content*.
- `fdatasync` file
- `rename` file, removing the `.atomic` suffix.
- `fsync` file
- `fsync` parent directory.

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

Even though we assume torn write protection of `512` bytes, we don't actively depend on this behaviour currently.

### References Used

- [Linux man pages for `open(2)` & `O_DSYNC`](https://www.man7.org/linux/man-pages/man2/open.2.html)
- [PostgreSQL's Fsync Errors page](https://wiki.postgresql.org/wiki/Fsync_Errors)
- ["Can Applications Recover from fsync Failures?" - CuttleFS Paper](https://dl.acm.org/doi/fullHtml/10.1145/3450338)
- [The XFS source code](https://elixir.bootlin.com/linux/v6.14.11/source/fs/xfs)
- [The EXT4 source code](https://elixir.bootlin.com/linux/v6.14.11/source/fs/ext4)

## Controllers

The system was built effectively from the bottom up, however, there is a bit of a messy connection point for
all the various individual components as they begin to be connected (particularly around metadata handling.)

The implementation is not terribly pretty, but in short:

- The WAL controller manages new WAL files and rotations
- The metadata controller manages recovery from checkpoints & WAL files _and_ lookup table handling and page
  metadata information. 
  * It is used to create the initial disk allocators, but not to update them.
  * The updating of group ID lookups and page metadata information, although they're under the same controller
    are not performed as part of the same operation.
- The storage controller wraps all the others and adds the "transactional" part of the storage system ensuring
  that the in-memory state is correct when aborting etc...