/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Where an allocator's heaps take their segments from and give them back to: free slots of its {@link Region}s, or,
 * without a {@link RegionSource}, one allocation per segment. Slow paths only: once per segment taken or given back,
 * and the heaps' decays.
 * <p>
 * Regions: a slot is taken and given back by a CAS on its region's bitmap, without a lock; a new region is mapped
 * under this store's monitor, the only lock, when no region has a free slot. Regions are only added, never removed,
 * until {@link #close}. Instead, the memory of free slots is purged: see {@link #purgeIfDue}; and so is the memory of
 * the idle free slices of the segments heaps hold, by their own decays: see {@link #purgeSlices}. Once a region cannot
 * be mapped, none is mapped again, and a take that finds no free slot allocates its segment on its own.
 * <p>
 * Used memory, reported to {@link AdaptivePoolingAllocator#chunkBufferAllocated} and
 * {@link AdaptivePoolingAllocator#chunkBufferFreed} per segment: without regions, a segment from its allocation to
 * its free. With regions, the committed segments: a slot counts from the time it is taken with no memory behind it to
 * the time it is purged (or the close), whether a heap holds it or it is free meanwhile. A slot never taken, or
 * purged and not taken since, does not count: this follows what the process has resident, except for the pages of a
 * committed segment nobody touched yet, or whose idle slices its heap purged. The same slots are charged to
 * {@link PlatformDependent}'s direct memory limit, never whole regions; a segment allocated on its own is charged by
 * its allocation.
 */
final class PageStore {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStore.class);
    private static final AtomicLongFieldUpdater<PageStore> SEGMENTS_COMMITTED =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "segmentsCommitted");
    private static final AtomicIntegerFieldUpdater<PageStore> PURGING =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "purging");
    private static final AtomicLongFieldUpdater<PageStore> SLICE_PURGE_CALLS =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "slicePurgeCalls");
    private static final AtomicLongFieldUpdater<PageStore> SLICE_PURGE_BYTES =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "slicePurgeBytes");
    private static final AtomicLongFieldUpdater<PageStore> SLICE_PURGE_HUGE_BLOCKS =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "slicePurgeHugeBlocks");
    private static final AtomicLongFieldUpdater<PageStore> SLICE_PURGE_FAILURES =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "slicePurgeFailures");
    private static final Region[] NO_REGIONS = new Region[0];
    /** mimalloc's per-heap segment reserve: 32 MiB, 1 to 8 segments. */
    private static final int RESERVE_BYTES = 32 * 1024 * 1024;
    private static final int MAX_RESERVED_SEGMENTS = 8;

    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    final SegmentSource segmentSource;
    /** {@code null} when every segment is an allocation of its own. */
    final RegionSource regionSource;
    /**
     * Cleared for good when a region cannot be mapped: from then on a take that finds no free slot in the regions
     * mapped so far allocates its segment on its own.
     */
    volatile boolean mapsRegions;
    /** Replaced, one longer, under this store's monitor; read without it. */
    volatile Region[] regions = NO_REGIONS;
    private boolean closed;
    /** 1 while a thread purges: one purger at a time. */
    private volatile int purging;
    private volatile long lastPurgeNanos = System.nanoTime();
    /** Counts the purges; a slot given back stamps itself with it. Written by the purger. */
    volatile int purgeEpoch;
    // Read by tests and dumps.
    /** Slots taken while no memory backed them: each such take costs page faults as the segment is touched. */
    volatile long segmentsCommitted;
    // Written by the purger only.
    long purges;
    long purgeCalls;
    long segmentsPurged;
    long bytesPurged;
    long purgeFailures;
    // Written by any heap's decay: the purges of idle slices of the segments heaps hold (see purgeSlices).
    volatile long slicePurgeCalls;
    volatile long slicePurgeBytes;
    /** Whole {@link PageStoreConfig#regionAlignment} blocks inside the purged runs: huge pages a THP kernel keeps. */
    volatile long slicePurgeHugeBlocks;
    volatile long slicePurgeFailures;

    /** Without {@code regionSource}, or without regions in {@code config}, every segment is allocated on its own. */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, SegmentSource segmentSource,
              RegionSource regionSource) {
        this.allocator = allocator;
        this.config = config;
        this.segmentSource = segmentSource;
        this.regionSource = config.regionSize > 0 ? regionSource : null;
        mapsRegions = this.regionSource != null;
    }

    /**
     * A segment for {@code heap}, which owns it from now on: a free slot of the fullest region (the first from the
     * heap's offset on a tie), else of a new region; without regions, or once one could not be mapped and no slot is
     * free, a new allocation.
     */
    Segment take(HeapSegments heap) {
        Segment segment = regionSource != null ? takeFromRegions(heap) : null;
        if (segment == null) {
            segment = new Segment(segmentSource.allocateSegment(config.segmentSize), config.sliceSize);
            allocator.chunkBufferAllocated(segment, true, heap.isThreadLocal());
        }
        segment.owner = heap;
        return segment;
    }

    private Segment takeFromRegions(HeapSegments heap) {
        for (;;) {
            Region[] regions = this.regions;
            int n = regions.length;
            Region fullest = null;
            int fullestFree = Integer.MAX_VALUE;
            int i = n == 0 ? 0 : heap.regionOffset % n;
            for (int k = 0; k < n; k++, i++) {
                if (i == n) {
                    i = 0;
                }
                Region region = regions[i];
                int free = Long.bitCount(region.free);
                if (free != 0 && free < fullestFree) {
                    fullest = region;
                    fullestFree = free;
                    if (free == 1) {
                        break;
                    }
                }
            }
            if (fullest == null) {
                if (!mapsRegions || !addRegion(regions)) {
                    return null;
                }
                continue;
            }
            int slot = fullest.takeSlot();
            if (slot < 0) {
                continue; // taken meanwhile: look again
            }
            boolean taken = false;
            try {
                Segment segment = fullest.segment(slot, segmentSource, config);
                if (fullest.freedEpoch[slot] == Region.UNCOMMITTED) {
                    PlatformDependent.incrementMemoryCounter(config.segmentSize);
                    segment.resident = 0;
                    fullest.freedEpoch[slot] = 0;
                    SEGMENTS_COMMITTED.incrementAndGet(this);
                    allocator.chunkBufferAllocated(segment, true, heap.isThreadLocal());
                }
                taken = true;
                return segment;
            } finally {
                if (!taken) {
                    fullest.giveBack(slot, purgeEpoch);
                }
            }
        }
    }

    /**
     * Maps a new region, unless one was added since {@code seen} was read. Three system calls. Returns false, and
     * maps no region ever again, if it cannot.
     */
    private synchronized boolean addRegion(Region[] seen) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        if (regions != seen) {
            return true;
        }
        if (!mapsRegions) {
            return false;
        }
        AbstractByteBuf buffer;
        try {
            buffer = regionSource.allocateRegion(config.regionSize, config.regionAlignment);
        } catch (OutOfMemoryError | RuntimeException e) {
            mapsRegions = false;
            logger.warn("Cannot map a region of {} bytes: segments are allocated one by one from now on.",
                    config.regionSize, e);
            return false;
        }
        assert buffer.capacity() == config.regionSize;
        Region[] grown = Arrays.copyOf(seen, seen.length + 1);
        grown[seen.length] = new Region(buffer, config.segmentsPerRegion());
        regions = grown;
        return true;
    }

    /**
     * {@code segment}, wholly free and owned by no heap, goes back to its region's free slots, or to its source,
     * which may return memory to the OS.
     */
    void free(Segment segment) {
        assert segment.isWhollyFree() && segment.owner == null;
        if (segment.region != null) {
            segment.region.giveBack(segment.slot, purgeEpoch);
            return;
        }
        allocator.chunkBufferFreed(segment, true);
        segment.buffer.release();
    }

    /**
     * Unmaps every region, with whatever segments are still in heaps: only when nothing can touch them any more
     * (the allocator is unreachable). Takes nothing from then on.
     */
    synchronized void close() {
        closed = true;
        Region[] regions = this.regions;
        this.regions = NO_REGIONS;
        for (Region region : regions) {
            for (int slot = 0; slot < region.slots; slot++) {
                if (region.freedEpoch[slot] != Region.UNCOMMITTED) {
                    region.freedEpoch[slot] = Region.UNCOMMITTED;
                    PlatformDependent.decrementMemoryCounter(config.segmentSize);
                    allocator.chunkBufferFreed(region.segmentOrNull(slot), true);
                }
            }
            region.buffer.release();
        }
    }

    /**
     * Any thread, from a heap's decay. At most once per {@link PageStoreConfig#decayIntervalNanos}, and by one thread
     * at a time (a try-guard: a caller that finds a purge running returns at once), gives back to the OS the memory
     * of the free slots that stayed free through a whole interval: those given back before the previous purge.
     */
    void purgeIfDue(long now) {
        if (regionSource == null || now - lastPurgeNanos < config.decayIntervalNanos
                || !PURGING.compareAndSet(this, 0, 1)) {
            return;
        }
        try {
            if (now - lastPurgeNanos >= config.decayIntervalNanos) {
                lastPurgeNanos = now;
                purge();
            }
        } finally {
            purging = 0;
        }
    }

    /**
     * Purges each run of contiguous purgeable free slots with one {@link RegionSource#purge} call, holding only that
     * run's slots, claimed by CAS, for the call: a take meanwhile still finds every other free slot. A slot purged
     * keeps its free bit's place in the order of takes: taking it again only costs the page faults of touching it.
     */
    private void purge() {
        int epoch = purgeEpoch + 1;
        purgeEpoch = epoch;
        purges++;
        for (Region region : regions) {
            long candidates = purgeable(region, region.free, epoch);
            while (candidates != 0) {
                long run = lowestRun(candidates);
                candidates &= ~run;
                long claimed = region.claim(run);
                if (claimed == 0) {
                    continue;
                }
                try {
                    purgeRuns(region, purgeable(region, claimed, epoch));
                } finally {
                    region.giveBackAll(claimed);
                }
            }
        }
    }

    /** The lowest run of contiguous set bits of {@code bits}, which is not 0. */
    private static long lowestRun(long bits) {
        int start = Long.numberOfTrailingZeros(bits);
        int length = Long.numberOfTrailingZeros(~(bits >>> start));
        return length == Long.SIZE ? -1L : (1L << length) - 1 << start;
    }

    /**
     * The slots of {@code slots} with memory behind them, given back before the previous purge: free through a whole
     * interval. Racy for slots the caller does not own, exact for those it does.
     */
    private static long purgeable(Region region, long slots, int epoch) {
        long purgeable = 0;
        for (long bits = slots; bits != 0; bits &= bits - 1) {
            int slot = Long.numberOfTrailingZeros(bits);
            int freed = region.freedEpoch[slot];
            if (freed != Region.UNCOMMITTED && epoch - freed >= 2) {
                purgeable |= 1L << slot;
            }
        }
        return purgeable;
    }

    /**
     * Purges each run of contiguous slots of {@code slots}, which the caller claimed, with one call. A run whose call
     * fails keeps its memory, and stays purgeable: the failure never reaches the allocation that drove the decay.
     */
    private void purgeRuns(Region region, long slots) {
        int segmentSize = config.segmentSize;
        while (slots != 0) {
            long bits = lowestRun(slots);
            slots &= ~bits;
            int start = Long.numberOfTrailingZeros(bits);
            int run = Long.bitCount(bits);
            try {
                regionSource.purge(region.buffer, start * segmentSize, run * segmentSize);
            } catch (Throwable t) {
                purgeFailed(t);
                continue;
            }
            purgeCalls++;
            bytesPurged += (long) run * segmentSize;
            PlatformDependent.decrementMemoryCounter(run * segmentSize);
            for (int slot = start; slot < start + run; slot++) {
                region.freedEpoch[slot] = Region.UNCOMMITTED;
                segmentsPurged++;
                allocator.chunkBufferFreed(region.segmentOrNull(slot), true);
            }
        }
    }

    /**
     * Any heap, from its decay, for a region segment it holds: gives the memory of each run of contiguous slices of
     * {@code slices}, all free in the segment and owned by the heap, back to the OS with one call. Returns the slices
     * purged: a run whose call fails keeps its memory, and the failure never reaches the heap. The segment stays in
     * the used memory, whole, as long as a heap holds it.
     */
    long purgeSlices(Segment segment, long slices) {
        int sliceSize = segment.sliceSize;
        int base = segment.slot * config.segmentSize;
        int hugeSlices = config.regionAlignment / sliceSize;
        long purged = 0;
        while (slices != 0) {
            long run = lowestRun(slices);
            slices &= ~run;
            int start = Long.numberOfTrailingZeros(run);
            int n = Long.bitCount(run);
            try {
                regionSource.purge(segment.region.buffer, base + start * sliceSize, n * sliceSize);
            } catch (Throwable t) {
                slicePurgeFailed(t);
                continue;
            }
            purged |= run;
            SLICE_PURGE_CALLS.incrementAndGet(this);
            SLICE_PURGE_BYTES.addAndGet(this, (long) n * sliceSize);
            if (hugeSlices > 1) {
                int blocks = (start + n) / hugeSlices - (start + hugeSlices - 1) / hugeSlices;
                if (blocks > 0) {
                    SLICE_PURGE_HUGE_BLOCKS.addAndGet(this, blocks);
                }
            }
        }
        return purged;
    }

    private void slicePurgeFailed(Throwable cause) {
        if (SLICE_PURGE_FAILURES.getAndIncrement(this) == 0) {
            logger.warn("Cannot purge idle slices of a segment: their memory stays committed. Further failures are "
                    + "logged at debug level.", cause);
        } else {
            logger.debug("Cannot purge idle slices of a segment ({} failures).", slicePurgeFailures, cause);
        }
    }

    private void purgeFailed(Throwable cause) {
        if (purgeFailures++ == 0) {
            logger.warn("Cannot purge free region slots: their memory stays committed. Further failures are logged "
                    + "at debug level.", cause);
        } else {
            logger.debug("Cannot purge free region slots ({} failures).", purgeFailures, cause);
        }
    }

    /**
     * The most wholly free segments a heap keeps (see {@link HeapSegments}). Without regions, a segment given back is
     * freed, and taking one allocates a buffer, a {@link Segment} and later its span views: mimalloc's reserve of
     * 32 MiB, from 1 to 8 segments, avoids that. While regions are mapped, one: a free slot is taken again by a CAS,
     * its {@link Segment} reused, while a reserved slot is not free to the other heaps nor to the purge.
     */
    int reserveLimit() {
        return mapsRegions ? 1 : maxReserveLimit();
    }

    int maxReserveLimit() {
        return Math.min(Math.max(1, RESERVE_BYTES / config.segmentSize), MAX_RESERVED_SEGMENTS);
    }

    int regionCount() {
        return regions.length;
    }

    /**
     * Racy, for tests and dumps: the slots of all regions {@code {in heaps, free with memory behind, free without}}.
     * A slot the purger claimed counts as in a heap.
     */
    int[] slotCounts() {
        int[] counts = new int[3];
        for (Region region : regions) {
            long free = region.free;
            for (int slot = 0; slot < region.slots; slot++) {
                if ((free & 1L << slot) == 0) {
                    counts[0]++;
                } else if (region.freedEpoch[slot] != Region.UNCOMMITTED) {
                    counts[1]++;
                } else {
                    counts[2]++;
                }
            }
        }
        return counts;
    }
}
