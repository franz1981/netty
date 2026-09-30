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

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    // The heap a segment is taken or given back for, in the JFR events.
    static final String STRIPE = "stripe";
    static final String THREAD_LOCAL = "thread-local";
    static final String NO_HEAP = "none";
    // Where a segment comes from, or goes to, in the JFR events.
    static final String FRESH_SLOT = "fresh-slot";
    static final String COMMITTED_SLOT = "committed-slot";
    static final String PURGED_SLOT = "purged-slot";
    static final String HEAP_RESERVE = "heap-reserve";
    static final String OWN_ALLOCATION = "own-allocation";
    static final String SLOT_RUN = "slot-run";
    static final String FREE_SLOT = "free-slot";
    static final String OWN_FREED = "own-freed";

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
    /**
     * With JFR available, for {@link PageStoreStateEvent} only, else {@code null}: the segments allocated on their own
     * that are out, and the heaps, weakly.
     */
    final Set<Segment> ownSegments;
    final Queue<WeakReference<HeapSegments>> heaps;

    /** Without {@code regionSource}, or without regions in {@code config}, every segment is allocated on its own. */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, SegmentSource segmentSource,
              RegionSource regionSource) {
        this.allocator = allocator;
        this.config = config;
        this.segmentSource = segmentSource;
        this.regionSource = config.regionSize > 0 ? regionSource : null;
        mapsRegions = this.regionSource != null;
        if (PlatformDependent.isJfrEnabled()) {
            ownSegments = ConcurrentHashMap.newKeySet();
            heaps = new ConcurrentLinkedQueue<WeakReference<HeapSegments>>();
            PageStoreStateEvent.register(this);
        } else {
            ownSegments = null;
            heaps = null;
        }
    }

    /** For {@link PageStoreStateEvent}: once per heap. */
    void registerHeap(HeapSegments heap) {
        Queue<WeakReference<HeapSegments>> heaps = this.heaps;
        if (heaps != null) {
            heaps.add(new WeakReference<HeapSegments>(heap));
        }
    }

    /** When {@code heap} is freed: it and the heaps collected meanwhile leave {@link #heaps}. */
    void unregisterHeap(HeapSegments heap) {
        Queue<WeakReference<HeapSegments>> heaps = this.heaps;
        if (heaps != null) {
            for (Iterator<WeakReference<HeapSegments>> it = heaps.iterator(); it.hasNext();) {
                HeapSegments registered = it.next().get();
                if (registered == null || registered == heap) {
                    it.remove();
                }
            }
        }
    }

    static void taken(long address, long length, int segments, int region, String source, String heap) {
        if (PlatformDependent.isJfrEnabled() && SegmentTakeEvent.isEventEnabled()) {
            SegmentTakeEvent.commit(address, length, segments, region, source, heap);
        }
    }

    static void givenBack(long address, long length, int segments, int region, String destination, String heap) {
        if (PlatformDependent.isJfrEnabled() && SegmentGiveBackEvent.isEventEnabled()) {
            SegmentGiveBackEvent.commit(address, length, segments, region, destination, heap);
        }
    }

    /**
     * A segment for {@code heap}, which owns it from now on: a free slot of the fullest region (the first from the
     * heap's offset on a tie), else of a new region; without regions, or once one could not be mapped and no slot is
     * free, a new allocation.
     */
    Segment take(HeapSegments heap) {
        Segment segment = takeSegment(heap.regionOffset, heap.kind());
        segment.owner = heap;
        return segment;
    }

    /**
     * Any thread. A segment for a chunk that uses it whole, outside any heap's segments: owned by no heap, and given
     * back with {@link #free} from any thread once it is wholly free, which it stays: see
     * {@link Segment#releasedWhole}.
     *
     * @param regionOffset where to start looking among the regions, as {@link HeapSegments#regionOffset}
     */
    Segment takeWhole(int regionOffset) {
        return takeSegment(regionOffset, NO_HEAP);
    }

    private Segment takeSegment(int regionOffset, String heap) {
        Segment segment = regionSource != null ? takeFromRegions(regionOffset, heap) : null;
        if (segment == null) {
            segment = new Segment(segmentSource.allocateSegment(config.segmentSize), config.sliceSize);
            allocator.chunkBufferAllocated(segment, true, heap == THREAD_LOCAL);
            if (ownSegments != null) {
                ownSegments.add(segment);
            }
            taken(segment.memoryAddress(), config.segmentSize, 1, -1, OWN_ALLOCATION, heap);
        }
        return segment;
    }

    private Segment takeFromRegions(int regionOffset, String heap) {
        for (;;) {
            Region[] regions = this.regions;
            int n = regions.length;
            Region fullest = null;
            int fullestFree = Integer.MAX_VALUE;
            int i = n == 0 ? 0 : regionOffset % n;
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
                String source = COMMITTED_SLOT;
                if (fullest.freedAt[slot] == Region.UNCOMMITTED) {
                    PlatformDependent.incrementMemoryCounter(config.segmentSize);
                    segment.resident = 0;
                    fullest.freedAt[slot] = 0;
                    source = fullest.everCommitted[slot] ? PURGED_SLOT : FRESH_SLOT;
                    fullest.everCommitted[slot] = true;
                    SEGMENTS_COMMITTED.incrementAndGet(this);
                    allocator.chunkBufferAllocated(segment, true, heap == THREAD_LOCAL);
                }
                taken = true;
                taken(segment.memoryAddress(), config.segmentSize, 1, fullest.index, source, heap);
                return segment;
            } finally {
                if (!taken) {
                    fullest.giveBack(slot, System.nanoTime());
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
        Object event = PlatformDependent.isJfrEnabled() && PageStoreMapEvent.isEventEnabled() ?
                PageStoreMapEvent.start() : null;
        try {
            buffer = regionSource.allocateRegion(config.regionSize, config.regionAlignment);
        } catch (OutOfMemoryError | RuntimeException e) {
            if (event != null) {
                AbstractPageStoreCallEvent.end(event, 0, config.regionSize, seen.length, e);
            }
            mapsRegions = false;
            logger.warn("Cannot map a region of {} bytes: segments are allocated one by one from now on.",
                    config.regionSize, e);
            return false;
        }
        assert buffer.capacity() == config.regionSize;
        if (event != null) {
            AbstractPageStoreCallEvent.end(event, buffer._memoryAddress(), config.regionSize, seen.length, null);
        }
        Region[] grown = Arrays.copyOf(seen, seen.length + 1);
        Region region = new Region(buffer, config.segmentsPerRegion());
        region.index = seen.length;
        grown[seen.length] = region;
        regions = grown;
        return true;
    }

    /**
     * Any thread. {@code slots} contiguous free slots of one region, for a buffer larger than a segment: returns the
     * region's index in {@link #regions} in the high half and the first slot in the low half, or -1 without regions,
     * or when {@code slots} is more than a region holds. Maps a new region when none has such a run. Each slot is
     * committed and counted as {@link #take} does; give the run back with {@link #freeRun}, from any thread.
     */
    long takeRun(int slots, int regionOffset) {
        if (regionSource == null || slots > config.segmentsPerRegion()) {
            return -1;
        }
        for (;;) {
            Region[] regions = this.regions;
            int n = regions.length;
            for (int k = 0, i = n == 0 ? 0 : regionOffset % n; k < n; k++, i = i + 1 == n ? 0 : i + 1) {
                Region region = regions[i];
                int start = region.takeRun(slots);
                if (start >= 0) {
                    commitRun(region, start, slots);
                    taken(region.buffer._memoryAddress() + (long) start * config.segmentSize,
                            (long) slots * config.segmentSize, slots, i, SLOT_RUN, NO_HEAP);
                    return (long) i << 32 | start;
                }
            }
            if (!mapsRegions || !addRegion(regions)) {
                return -1;
            }
        }
    }

    /** The run's slots with no memory behind them are charged and counted; on failure the whole run goes back. */
    private void commitRun(Region region, int start, int slots) {
        boolean committed = false;
        try {
            for (int slot = start; slot < start + slots; slot++) {
                Segment segment = region.segment(slot, segmentSource, config);
                if (region.freedAt[slot] == Region.UNCOMMITTED) {
                    PlatformDependent.incrementMemoryCounter(config.segmentSize);
                    region.freedAt[slot] = 0;
                    region.everCommitted[slot] = true;
                    SEGMENTS_COMMITTED.incrementAndGet(this);
                    allocator.chunkBufferAllocated(segment, true, false);
                }
            }
            committed = true;
        } finally {
            if (!committed) {
                region.giveBackRun(start, slots, System.nanoTime());
            }
        }
    }

    /** Any thread: the run {@link #takeRun} returned goes back to its region's free slots, purged as any free slot. */
    void freeRun(Region region, int start, int slots) {
        long now = System.nanoTime();
        for (int slot = start; slot < start + slots; slot++) {
            region.segmentOrNull(slot).releasedWhole(now);
        }
        region.giveBackRun(start, slots, now);
        givenBack(region.buffer._memoryAddress() + (long) start * config.segmentSize,
                (long) slots * config.segmentSize, slots, region.index, SLOT_RUN, NO_HEAP);
    }

    Region region(int index) {
        return regions[index];
    }

    /**
     * {@code segment}, wholly free and owned by no heap, goes back to its region's free slots, or to its source,
     * which may return memory to the OS.
     */
    void free(Segment segment) {
        free(segment, NO_HEAP);
    }

    /** As {@link #free(Segment)}, on behalf of {@code heap}: {@link #STRIPE}, {@link #THREAD_LOCAL} or none. */
    void free(Segment segment, String heap) {
        assert segment.isWhollyFree() && segment.owner == null;
        long address = segment.memoryAddress();
        Region region = segment.region;
        if (region != null) {
            region.giveBack(segment.slot, System.nanoTime());
            givenBack(address, config.segmentSize, 1, region.index, FREE_SLOT, heap);
            return;
        }
        if (ownSegments != null) {
            ownSegments.remove(segment);
        }
        allocator.chunkBufferFreed(segment, true);
        segment.buffer.release();
        givenBack(address, config.segmentSize, 1, -1, OWN_FREED, heap);
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
                if (region.freedAt[slot] != Region.UNCOMMITTED) {
                    region.freedAt[slot] = Region.UNCOMMITTED;
                    PlatformDependent.decrementMemoryCounter(config.segmentSize);
                    allocator.chunkBufferFreed(region.segmentOrNull(slot), true);
                }
            }
            long address = region.buffer._memoryAddress();
            Object event = PlatformDependent.isJfrEnabled() && PageStoreUnmapEvent.isEventEnabled() ?
                    PageStoreUnmapEvent.start() : null;
            Throwable failure = null;
            try {
                region.buffer.release();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                if (event != null) {
                    AbstractPageStoreCallEvent.end(event, address, config.regionSize, region.index, failure);
                }
            }
        }
    }

    /**
     * Any thread, from a heap's purge tick (see {@link HeapSegments#purgeTick}). At most once per
     * {@link PageStoreConfig#purgeCheckNanos}, and by one thread at a time (a try-guard: a caller that finds a purge
     * running returns at once), gives back to the OS the memory of the free slots that stayed free for
     * {@link PageStoreConfig#purgeDelayNanos} at least.
     */
    void purgeIfDue(long now) {
        if (regionSource == null || now - lastPurgeNanos < config.purgeCheckNanos
                || !PURGING.compareAndSet(this, 0, 1)) {
            return;
        }
        try {
            if (now - lastPurgeNanos >= config.purgeCheckNanos) {
                lastPurgeNanos = now;
                purge(now);
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
    private void purge(long now) {
        purges++;
        long delay = config.purgeDelayNanos;
        for (Region region : regions) {
            long candidates = purgeable(region, region.free, now, delay);
            while (candidates != 0) {
                long run = lowestRun(candidates);
                candidates &= ~run;
                long claimed = region.claim(run);
                if (claimed == 0) {
                    continue;
                }
                try {
                    purgeRuns(region, purgeable(region, claimed, now, delay));
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
     * The slots of {@code slots} with memory behind them, given back {@code delay} before {@code now} or earlier.
     * Racy for slots the caller does not own, exact for those it does.
     */
    private static long purgeable(Region region, long slots, long now, long delay) {
        long purgeable = 0;
        for (long bits = slots; bits != 0; bits &= bits - 1) {
            int slot = Long.numberOfTrailingZeros(bits);
            long freed = region.freedAt[slot];
            if (freed != Region.UNCOMMITTED && now - freed >= delay) {
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
            Object event = PlatformDependent.isJfrEnabled() && PageStorePurgeEvent.isEventEnabled() ?
                    PageStorePurgeEvent.start() : null;
            try {
                regionSource.purge(region.buffer, start * segmentSize, run * segmentSize);
            } catch (Throwable t) {
                purged(event, SLOTS, region, start * segmentSize, run * segmentSize, t);
                purgeFailed(t);
                continue;
            }
            purged(event, SLOTS, region, start * segmentSize, run * segmentSize, null);
            purgeCalls++;
            bytesPurged += (long) run * segmentSize;
            PlatformDependent.decrementMemoryCounter(run * segmentSize);
            for (int slot = start; slot < start + run; slot++) {
                region.freedAt[slot] = Region.UNCOMMITTED;
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
            Object event = PlatformDependent.isJfrEnabled() && PageStorePurgeEvent.isEventEnabled() ?
                    PageStorePurgeEvent.start() : null;
            try {
                regionSource.purge(segment.region.buffer, base + start * sliceSize, n * sliceSize);
            } catch (Throwable t) {
                purged(event, SLICES, segment.region, base + start * sliceSize, n * sliceSize, t);
                slicePurgeFailed(t);
                continue;
            }
            purged(event, SLICES, segment.region, base + start * sliceSize, n * sliceSize, null);
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

    private static final String SLOTS = "slots";
    private static final String SLICES = "slices";

    private static void purged(Object event, String unit, Region region, int offset, int length, Throwable failure) {
        if (event != null) {
            PageStorePurgeEvent.end(event, unit, region.buffer._memoryAddress() + offset, length, region.index,
                    failure);
        }
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
                } else if (region.freedAt[slot] != Region.UNCOMMITTED) {
                    counts[1]++;
                } else {
                    counts[2]++;
                }
            }
        }
        return counts;
    }
}
