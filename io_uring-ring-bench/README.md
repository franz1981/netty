# io_uring buffer ring allocator harness

Two harnesses used to size `IoUringRecyclingBufferRingAllocator` (netty/netty#17635) against
`IoUringFixedBufferRingAllocator`. Not part of the PR; kept on this branch so the numbers can be reproduced.

## Why a receive sink and not the echo server

The allocator runs once per buffer the kernel consumed. An echo server spends a few microseconds of CPU per
message (parse, write, zero-copy or copy, flush), so 15-30 ns of allocator work is 0.1-0.6% of it and no e2e
number can resolve it. `RingSinkServer` removes everything but receive and release, so the buffer turnover per
unit of other work is maximal - the same shape liburing's `examples/proxy.c` uses when it defaults to 32 byte
buffers. One event loop, pinned to one core, is the unit under test: what is being measured is CPU per received
buffer on a saturated loop, not aggregate throughput.

* `RingSinkServer <fixed|recycling> <slotSize> 1 <port> <bufferRingSize>` - io_uring server, provided buffer ring,
  multishot recv, no bundles, no incremental consumption. Counts reads and bytes, releases every buffer, prints
  `STAT <nanos> <reads> <bytes>` every 200 ms. Reports `FALLBACKS` (recycling allocator leaving its region) and
  `EXHAUSTED` (ring ran dry) on shutdown - both must be 0 for a cell to count.
* `SinkClient <host> <port> <connections> <warmup> <measure> <threads>` - NIO, keeps 16 KiB writes in flight while
  writable, so the server is never starved.
* `RingEchoServer` / `RingEchoClient` - the echo cell, for the "does it matter in a real server" direction.

## Running

```
W=/path/to/netty ./run-sink.sh recycling 64 19001 tag 5 20   # one cell
W=/path/to/netty ./sink-ab.sh                                # fixed vs recycling, 64/256/4096 B, 3 reps
W=/path/to/netty ./sink-thp-ab.sh                            # THP=madvise vs always, 4096 B slots
```

`W` points at a netty checkout with the PR built (`mvn install -pl transport-native-io_uring -am` plus
`microbench`); `io_uring-ring-bench/classes` holds the compiled harness. `CORE`, `CONNS`, `RING`, `JAVA`,
`SERVER_NUMA`, `CLIENT_NUMA`, `OUTDIR` are env overrides. The A/B scripts pin every CPU to a fixed frequency with
`sudo` and verify the readback, and interleave the variants so drift hits both.

`run-sink.sh` prints CPU per read from `perf stat` on the server pid over the measured window, plus the
`STAT` delta for reads/bytes; the `-thp` variants add `ls_l1_d_tlb_miss.all_l2_miss` (page walks) and an
`AnonHugePages` snapshot of the server.

Sanity fields to check in every cell before reading a number: server core at ~99% share, `FALLBACKS 0`,
`EXHAUSTED 0`, bytes received == bytes sent, low runnable count.
