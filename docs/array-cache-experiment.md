# Experiment: unbounded size-class chunk cache on a partitioned array

Branch `4.2_eager_on_striped`, based on `5f7d50ded4` (the commit that lifts the stripe lock:
ONE `StampedLock` per stripe covering all size classes — the enabling change, without which the
release path has no correctly-scoped lock to take).

## What this branch does

Removes upstream 4.2's cache ceiling and replaces scan-based capacity discovery with
release-triggered transitions, **keeping the array**:

- `offerChunk` never refuses. `maxCachedChunks` is gone. A retention floor is the only bound.
- Per-chunk notification claim (`pendingNext`) + Treiber stack on the cache; drain re-arms before
  processing. Release path is three-way: owner thread / stripe lock won -> inline transition;
  lock lost -> offer segment, then leave a note.
- One flat array split at `reusableCount`; each chunk stores its own slot in `cacheIndex`, so
  removal from the middle is O(1) by swapping with the boundary or the end.
- Deleted as newly dead: the ring (`head`/`tail`), `notEmptyCount`, `scanForCapacity{,Fallback}`,
  `runPurgeScan`, the Dutch-flag `partition`, `purgeEpoch`, `CHUNK_PURGE_THRESHOLD`.
- `cacheListState` is not needed: region is derived from `cacheIndex < reusableCount`, and
  `cacheIndex == NOT_CACHED` means not cached.

Tests: full buffer suite 13,922 pass, 0 failures, assertions on.

## Measurements (32t, MLB=65536, SOCKET_PROXY, event-loop, f=10, clean builds)

| variant | ns/op | RSS |
|---|---|---|
| reference `4.2_chunk_recycling` (intrusive lists) | 429.7 +/- 24.1 | 6,965 MB |
| **this branch** | **470.0 +/- 34.3** | 7,180 MB |
| upstream 4.2 pristine (capped cache) | ~8,046 | ~26,214 MB |

So it solves the problem it set out to solve — ~17x faster and ~3.6x leaner than upstream — but is
~9% slower than the linked-list reference.

## The LIFO/FIFO finding (root cause of ALL cache-behaviour differences)

The reference polls `reusableHead` and inserts at the head: **LIFO**. This branch polls `chunks[0]`
while `addToReusable`/`moveToReusable` place at `reusableCount`: **FIFO**.

Changing this branch to LIFO (`chunks[reusableCount - 1]`) converged every counter onto the
reference:

| counter | reference | this branch (FIFO) | this branch + LIFO |
|---|---|---|---|
| free segments at poll | 17.38 | 68.05 | 17.82 |
| cache size (avg at poll) | 191.72 | 160.03 | 191.69 |
| cache polls | 24.9M | 7.9M | 20.4M |
| cache misses | 98,757 | 490,601 | 82,332 |
| recycler polls | 80,645 | 472,489 | 64,220 |
| evictions | 86,539 | 483,985 | 75,956 |

FIFO makes a chunk wait a full cache cycle before reuse, so it is polled nearly full (68 vs 17 free
segments, hence 3x fewer polls) but also reaches *fully*-free while cached far more often, which is
what drives eviction churn.

**But LIFO made latency WORSE: 470 -> 522 ns/op.** FIFO's 3x fewer polls more than pay for its
churn. The churn is real and fully explained, and it is *not* what costs this branch its time.
Removing the release-path `drainPending()` also did not help. Neither change is in this branch.

## The unresolved question

Matched clean-build `perfnorm`:

| metric | reference | this branch |
|---|---|---|
| instructions/op | 852 | 1,665 (+95%) |
| CPI | 2.215 | 2.186 (-1.3%) |
| branches | 178 | 316 |
| L1-dcache-loads | 359 | 743 |

Identical CPI means this is pure extra work, not stalls or cache behaviour. It surfaces as only ~9%
of wall time because the allocator is a minority of this benchmark (the harness's own
`readAndRelease` is 55-62% of profiler samples).

**Where those ~813 instructions/op live is NOT established.** Sampling shows no allocator hotspot,
and the array bookkeeping that can be counted (a few writes per transition, at 0.55-0.69 transitions
per op) is nowhere near enough. `perfasm` on the two clean builds is the next step.

Note: this branch's tick is *cheaper* than the reference's — it sweeps only its own cache, whereas
the reference also calls `purgeHeapSiblings()` across all 16 size classes. So the tick cannot
explain a gap in this direction, and the two tick policies are worth deciding on their own
footprint merits separately.

## Honest caveats

- The array never shrinks; it retains peak length. Judged noise against 128 KiB+ chunks.
- Quiescence is not guaranteed if the very last cross-thread release loses the stripe lock — its
  note waits for the next allocation on that stripe. The reference has the same hole.
- No measured advantage for the array has been found. The memory saving (one `int` vs two refs plus
  a state field) is ~12-16 bytes per chunk against chunks of 128 KiB-528 KiB.
