# Benchmark Notes

Some observations as benchmarks have been ran.

## Read benches - 2025-10-12

When we have an internal limit of 128 IOPS per read task and a coalesce factor of `1.3`:
```shell
starting benchmark
Running bench: 5.37 GB
    Running concurrency=1
       cache size: 2.15 GB, result: 2.323384337s per reader 4.56 GB/s
       cache size: 6.44 GB, result: 1.448096655s per reader 7.46 GB/s
    Running concurrency=10
       cache size: 2.15 GB, result: 8.735082529s per reader 12.06 GB/s
       cache size: 6.44 GB, result: 1.054118535s per reader 98.91 GB/s
    Running concurrency=25
       cache size: 2.15 GB, result: 21.518630545s per reader 12.09 GB/s
       cache size: 6.44 GB, result: 1.292721539s per reader 203.04 GB/s
    Running concurrency=50
       cache size: 2.15 GB, result: 79.00651933s per reader 6.61 GB/s
       cache size: 6.44 GB, result: 1.630225084s per reader 321.81 GB/s
Running bench: 10.74 GB
    Running concurrency=1
       cache size: 2.15 GB, result: 2.914480993s per reader 3.50 GB/s
       cache size: 6.44 GB, result: 2.006276729s per reader 5.25 GB/s
    Running concurrency=10
       cache size: 2.15 GB, result: 8.535761585s per reader 12.33 GB/s
       cache size: 6.44 GB, result: 6.342934089s per reader 16.42 GB/s
    Running concurrency=25
       cache size: 2.15 GB, result: 25.124409778s per reader 10.47 GB/s
       cache size: 6.44 GB, result: 15.99470065s per reader 16.22 GB/s
    Running concurrency=50
       cache size: 2.15 GB, result: 86.393704379s per reader 6.09 GB/s
       cache size: 6.44 GB, result: 54.866632872s per reader 9.59 GB/s
complete
```

When we have an internal limit of 128 IOPS per read task and a coalesce factor of `1.0`:
```shell
starting benchmark
Running bench: 5.37 GB
    Running concurrency=1
       cache size: 2.15 GB, result: 2.557129265s per reader 4.12 GB/s
       cache size: 6.44 GB, result: 1.535927654s per reader 6.85 GB/s
    Running concurrency=10
       cache size: 2.15 GB, result: 7.88268713s per reader 13.28 GB/s
       cache size: 6.44 GB, result: 1.037590317s per reader 101.40 GB/s
    Running concurrency=25
       cache size: 2.15 GB, result: 21.13207944s per reader 12.31 GB/s
       cache size: 6.44 GB, result: 1.236560954s per reader 212.44 GB/s
    Running concurrency=50
       cache size: 2.15 GB, result: 77.804059944s per reader 6.75 GB/s
       cache size: 6.44 GB, result: 1.471764871s per reader 355.76 GB/s
Running bench: 10.74 GB
    Running concurrency=1
       cache size: 2.15 GB, result: 3.108333074s per reader 3.40 GB/s
       cache size: 6.44 GB, result: 2.136246195s per reader 4.92 GB/s
    Running concurrency=10
       cache size: 2.15 GB, result: 8.843499094s per reader 11.85 GB/s
       cache size: 6.44 GB, result: 6.12635121s per reader 16.98 GB/s
    Running concurrency=25
       cache size: 2.15 GB, result: 23.577872034s per reader 11.10 GB/s
       cache size: 6.44 GB, result: 14.185582357s per reader 18.47 GB/s
    Running concurrency=50
       cache size: 2.15 GB, result: 81.395231209s per reader 6.43 GB/s
       cache size: 6.44 GB, result: 54.204326231s per reader 9.70 GB/s
complete
```

> [!NOTE]  
> Overall it seems like the coalesce factor does not have a huge impact both in a positive or negative way,
> to test we probably need a more detailed benchmark for sparse reads in particular.
> 
> One very interesting thing however is the concurrency having a net-negative performance impact, unfortunately
> profiling this is tricky.


## Post moka-removal benches - 2025-10-14

The above benchmarks are before we removed moka, once we implemented a custom cache and then wrapped it in a mutex,
we saw some very big gains to the read performance.

> [!NOTE]  
> One thing to node is the rate of the requests is so high now the memory usage goes above the cache size,
> since the system cannot free the pages before new reads hit it and expire the eviction.

```shell
starting benchmark
Running bench: 5.37 GB
    Running concurrency=1
       cache size: 2.15 GB, result: 9.787717053s per reader 10.62 GB/s
       cache size: 6.44 GB, result: 2.549728861s per reader 40.85 GB/s
    Running concurrency=25
       cache size: 2.15 GB, result: 24.020964274s per reader 109.09 GB/s
       cache size: 6.44 GB, result: 10.034491125s per reader 260.58 GB/s
    Running concurrency=50
       cache size: 2.15 GB, result: 38.489019408s per reader 136.30 GB/s
       cache size: 6.44 GB, result: 12.819887969s per reader 408.29 GB/s
Running bench: 10.74 GB
    Running concurrency=1
       cache size: 2.15 GB, result: 19.24550986s per reader 5.47 GB/s
       cache size: 6.44 GB, result: 12.964350779s per reader 8.04 GB/s
    Running concurrency=25
       cache size: 2.15 GB, result: 36.400605358s per reader 71.93 GB/s
       cache size: 6.44 GB, result: 35.127687673s per reader 74.51 GB/s
    Running concurrency=50
       cache size: 2.15 GB, result: 69.883044855s per reader 74.94 GB/s
       cache size: 6.44 GB, result: 60.184298085s per reader 87.08 GB/s
complete
```