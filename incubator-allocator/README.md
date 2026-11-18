# netty-incubator-allocator

An allocator for Netty `ByteBuf`s that takes all its memory from one shared page store. It is **incubating and
opt-in**: nothing selects it unless you ask, and its API, class names, package, property names and JFR event names
may change in any release, including a bugfix one.

## What it is

`io.netty.incubator.allocator.AdaptiveByteBufAllocator` is a pooling `ByteBufAllocator`. It has the same simple name
as `io.netty.buffer.AdaptiveByteBufAllocator`, which is why the package is the only part of an import that tells them
apart. The Maven artifact is `io.netty:netty-incubator-allocator`, the Java module is `io.netty.incubator.allocator`.

## What it does differently from the adaptive allocator (high level)

- All memory comes from a `PageStore`, shared by every thread of the allocator: 64 KiB slices, cut from blocks (2 or
  4 MiB), which sit in regions. The direct and the heap side of the allocator each have a store of their own.
- Up to the largest size class a buffer is a slot of a chunk that is a span of slices of one size class. Above it, up
  to a block, a span holds one buffer alone; larger buffers, or buffers when no span can be had, get a one-shot chunk
  that is freed with them.
- Magazines are grouped in stripe heaps, each guarded by one lock. A `FastThreadLocalThread` gets a heap of its own,
  which needs no lock.
- Idle memory is given back to the OS, after a delay (see below). With `mmap` regions, free slices are purged in
  place with `madvise(MADV_DONTNEED)`. Regions of one `malloc`'d block are freed whole once all their slices are free.

## How to enable it

1. Add the dependency, next to `netty-buffer`:

   ```xml
   <dependency>
     <groupId>io.netty</groupId>
     <artifactId>netty-incubator-allocator</artifactId>
     <version>${netty.version}</version>
   </dependency>
   ```

2. Select it as Netty's default allocator, `ByteBufAllocator.DEFAULT` and `ByteBufUtil.DEFAULT_ALLOCATOR`:
   `-Dio.netty.allocator.type=pagestore`. If the class cannot be loaded (the dependency is missing), `ByteBufUtil`
   logs a warning and uses the adaptive allocator. On the module path nothing requires this module: resolve it with
   `--add-modules io.netty.incubator.allocator`.

   Or build it yourself: `new io.netty.incubator.allocator.AdaptiveByteBufAllocator()`.

3. Quarkus: `quarkus.netty.allocator-type=pagestore`, with the dependency above on the class path. This key is read
   from a Quarkus change that is not in this repository; check that your Quarkus version has it.

On the module path, `netty-buffer` exports `io.netty.buffer.internal` to this module only, for the access the
allocator needs.

## Features and tunables

All are system properties, read once when the class that owns them loads. The `segment*` and `heapSegmentSize`
values are clamped as the table says, logged at DEBUG, and logged at WARN when the clamp changed what was set.

| Property | Default | What it does |
|---|---|---|
| `io.netty.allocator.pagestore.mmap` | `false` | Direct regions are `mmap(2)` regions (see below). Off: `malloc`'d blocks. |
| `io.netty.allocator.pagestore.segmentSize` | 4 MiB (2 MiB in low-memory mode) | The block size; rounded to a multiple of 2 MiB, 2 to 4 MiB. |
| `io.netty.allocator.pagestore.segmentRegionSize` | 64 blocks | Size of an `mmap` region: 2 to 64 blocks, a multiple of the block size. `0`: regions of one `malloc`'d block. |
| `io.netty.allocator.pagestore.heapSegmentSize` | block size minus 64 KiB | Block size of the heap allocator: 3 to 64 slices, rounded down to whole slices. |
| `io.netty.allocator.pagestore.segmentPurgeDelay` | 4000 (ms) | How long a free slice stays free before a purge. Clamped to 10 to 600000. |
| `io.netty.allocator.pagestore.lowMemory` | `true` if the max heap is at most 512 MiB | Low-memory mode, see below. |
| `io.netty.allocator.pagestore.disableThreadLocalMagazinesOnLowMemory` | `true` | `true`: in low-memory mode no thread has a heap of its own. `false` keeps them. |
| `io.netty.allocator.pagestore.useCachedMagazinesForNonEventLoopThreads` | `false` | Threads that are not event loop threads also get a heap of their own. |
| `io.netty.allocator.pagestore.chunkPurgeInterval` | `4` (minimum 1) | How often, in chunks' worth of allocations, a size-class magazine applies the notes left by other threads. |
| `io.netty.allocator.pagestore.magazineBufferQueueCapacity` | `1024` (minimum 2) | How many `AdaptiveByteBuf` objects a stripe keeps for reuse. Pools the objects only, not their memory. |

**mmap (opt-in) and native access.** With `-Dio.netty.allocator.pagestore.mmap=true`, direct regions are `mmap`'d and
idle slices are purged with `madvise`. This needs Java 23 or later, Linux on x86_64 or aarch64, native access for the
module (`--enable-native-access=ALL-UNNAMED` on the class path, `--enable-native-access=io.netty.incubator.allocator`
on the module path), a non-zero `segmentRegionSize`, and Netty counting direct memory (not
`-Dio.netty.maxDirectMemory=0`). When any of these is missing the allocator uses `malloc`'d one-block regions and logs
the reason at DEBUG. It is never linked inside a GraalVM native image. At startup it logs a WARN on a Linux kernel
older than 6.17, where `madvise(MADV_DONTNEED)` holds the process-wide `mmap_lock` (it names Linux commit
a6fde7add78d; a kernel with a backport is not affected).

**Purge.** A freed span arms the purge. Once `segmentPurgeDelay` has passed, the next allocation that looks at the
clock runs one pass, as the only purger, which gives back at most 64 MiB and resumes where it stopped.
A released run of 32 MiB or more is the exception: its release purges it at once.

**Low memory.** With `lowMemory` on: one stripe heap; only the size classes up to 16896 bytes are pooled, and every
larger buffer is an allocation of its own, freed with it (no spans), as in Netty's adaptive allocator in the same
mode; the default block size is 2 MiB; and no thread has a heap of its own unless
`disableThreadLocalMagazinesOnLowMemory=false`, which gives threads their heaps as outside low-memory mode, for those
size classes.

**Contention.** A thread without a heap of its own tries each stripe's lock twice, and stripes are added as
contention asks, up to twice the cores rounded up to a power of two. When every lock stays taken the buffer is
allocated on its own, unpooled, and freed with it. In low-memory mode there is one stripe: any contention on its
lock does this, for every thread unless `disableThreadLocalMagazinesOnLowMemory=false`.

**JFR events.** Registered only when a Flight Recorder exists (`-Dio.netty.jfr.enabled=false` keeps Netty away from
`jdk.jfr`), and all disabled until a recording setting enables them. Category `Netty` / `Page Store`:
`io.netty.pagestore.AllocateBuffer`, `ReallocateBuffer`, `FreeBuffer`, `AllocateChunk`, `FreeChunk`, `PageStoreMap`,
`PageStoreUnmap`, `PageStorePurge`, and `PageStoreState` (a periodic event, 1 s, with the address ranges of every
region's slices by state: claimed, free with memory, purged, never used).

**Other.** BlockHound integration (a service file in the jar) and GraalVM native-image configuration
(`META-INF/native-image/io.netty/netty-incubator-allocator/`).

## Known limitation

There is no idle timer and no thread of its own. Idle memory goes back only from allocation paths: a purge pass runs
when some thread allocates after the delay. An allocator that nothing allocates from keeps its idle memory until
something does.

A buffer that outlives the heap of the thread that allocated it and is never released keeps the purge running: one
pass over every block per `segmentPurgeDelay`, while something allocates.

## References

Related designs, for comparison. Not a claim that this module copies them.

- Leijen, Zorn, de Moura, *Mimalloc: Free List Sharding in Action*, MSR-TR-2019-18, June 2019.
  https://www.microsoft.com/en-us/research/publication/mimalloc-free-list-sharding-in-action/ (v1-era; it does not
  describe purge delays).
- mimalloc docs, "Environment Options" (page header "mi-malloc 3.6/2.5/1.15", undated):
  `MIMALLOC_PURGE_DELAY`, 1000 ms by default in v3. https://microsoft.github.io/mimalloc/environment.html
- mimalloc v3.7.0 (1ed67b5156a7), `src/arena/arena-alloc.c:397-416`: a freed arena range is purged in free when the
  delay is 0 or the range is at least `arena_purge_immediate_size`, otherwise its purge is delayed.
  https://github.com/microsoft/mimalloc/blob/1ed67b5156a763441d2ec3c20221a7b3cd866129/src/arena/arena-alloc.c#L397-L416
- jemalloc 5.3.0 (54eaed1d8b56), `TUNING.md`: decay time, and background threads for purging, opt-in.
  https://github.com/jemalloc/jemalloc/blob/5.3.0/TUNING.md
- jemalloc 5.3.0, `src/extent.c:940-946`: a freed extent at or above `oversize_threshold` is purged eagerly.
  https://github.com/jemalloc/jemalloc/blob/54eaed1d8b56b1aa528be3bdd1877e59c56fa90c/src/extent.c#L940-L946
- Hunter et al., *Beyond malloc efficiency to fleet efficiency: a hugepage-aware memory allocator*, OSDI '21, July 2021.
  https://www.usenix.org/conference/osdi21/presentation/hunter
- tcmalloc, `docs/tuning.md:114-132` at 6cb4939e97a0 (no tags; HEAD on 2026-11-18): memory is released from the page
  heap by a background thread at a set rate.
  https://github.com/google/tcmalloc/blob/6cb4939e97a057a7ecea2d277e0d77706c2fb820/docs/tuning.md#L114-L132

Fetched and checked 2026-11-18; the docs pages carry no date, so check them against the version you run.
