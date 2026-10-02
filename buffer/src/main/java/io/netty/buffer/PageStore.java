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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * The units, largest first, and their names in the code:
 * <pre>
 * region      one piece of memory from a {@link RegionSource}       {@link Region}
 *  block      4 MiB, one 64-bit free bitmap                       {@link Segment}
 *   slice     64 KiB, one bit of its block's bitmap
 *    chunk    a run of slices serving one size class              SizeClassedChunk
 *     slot    one buffer's place in a chunk                       SizeClassedChunk's "segment", segmentSize
 *    span     a run of slices holding one buffer above the sizes  SpanMagazine, SharedSpanChunk
 * heap        a thread-local heap or a stripe                     StripedHeap, ThreadLocalSizeClassHeap
 * </pre>
 * Heaps own chunks, never blocks: any heap claims a run of slices from the regions' shared bitmaps by CAS, and
 * whichever thread frees the run gives it back by CAS ({@link #claimSlices}, {@link #releaseSlices}), as mimalloc v3
 * claims a page's slices straight from its arena's bitmap
 * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L240-L246). A hole one heap leaves is reused by any.
 * A buffer above a block takes a run of whole blocks ({@link #takeRun}). A new region is added under
 * this store's monitor, the only lock, when no region has a fit.
 * <p>
 * The source is a detail of the memory, not of the ownership: {@code mmap} regions (256 MiB, {@link MmapRegionSource})
 * purge the free slices idle for the purge delay in place, and charge and count a slice from the claim that finds no
 * memory behind it to its purge; regions of one block ({@link MallocRegionSource}: a {@code malloc}'d 4 MiB, or a
 * {@code byte[]} of 4032 KiB for heap memory) are charged and counted whole by their allocation, and go back whole once
 * all of them stayed free for the delay. Once an {@code mmap} region cannot be mapped, new regions are one
 * {@code malloc}'d block each, next to the regions mapped so far. One purger at a time, driven by the heaps' ticks: see
 * {@link #purgeIfDue}.
 */
final class PageStore {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStore.class);
    private static final AtomicIntegerFieldUpdater<PageStore> PURGING =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "purging");
    private static final AtomicLongFieldUpdater<PageStore> SLICES_COMMITTED =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "slicesCommitted");
    private static final AtomicLongFieldUpdater<PageStore> PURGE_WAITS =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "purgeWaits");
    /** The longest a claim waits for the purger to give back the run it holds: see {@link #awaitPurgedRun}. */
    private static final long PURGE_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final AtomicIntegerFieldUpdater<PageStore> HEAP_SEQUENCE =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "heapSequence");
    private static final Region[] NO_REGIONS = new Region[0];
    // The heap a segment is taken or given back for, in the JFR events.
    static final String STRIPE = "stripe";
    static final String THREAD_LOCAL = "thread-local";
    static final String NO_HEAP = "none";
    // Where memory comes from, or goes to, in the JFR events.
    static final String SHARED_SLICES = "shared-slices";
    static final String PURGED_SLICES = "purged-slices";

    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    final SegmentSource segmentSource;
    /** Where new regions come from. Replaced once, under this store's monitor, by {@link #fallbackSource}. */
    volatile RegionSource regionSource;
    /** The blocks of a new region, and where it starts: replaced with {@link #regionSource}. */
    private volatile int regionBlocks;
    private int regionAlignment;
    /**
     * Regions of one block to make from when a region of {@link #regionSource} cannot be had, or {@code null}. Under
     * this store's monitor.
     */
    private RegionSource fallbackSource;
    /** Replaced under this store's monitor, one longer or with a released region's place taken; read without it. */
    volatile Region[] regions = NO_REGIONS;
    private boolean closed;
    /** 1 while a thread purges: one purger at a time. */
    private volatile int purging;
    private volatile long lastPurgeNanos = System.nanoTime();
    // Read by tests and dumps.
    /** Shared slices claimed while no memory backed them. */
    volatile long slicesCommitted;
    /** Shared slices: claims that found no fit while the purger held slices, and waited for it. */
    volatile long purgeWaits;
    /**
     * Shared slices, written by the purger only: odd while it holds slices it claimed for their purge, incremented
     * again when it gave them back.
     */
    private volatile long purgeSequence;
    /** The next heap's sequence: see {@link #nextHeapSequence}. */
    private volatile int heapSequence;
    // Written by the purger only.
    long purges;
    long purgeCalls;
    long bytesPurged;
    long purgeFailures;
    /** Shared slices purged. */
    long slicesPurged;
    /** Regions given back whole: see {@link #releaseIdleRegions}. */
    long regionsReleased;

    /** {@code config} must have regions, and {@code regionSource} make them. */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, SegmentSource segmentSource,
              RegionSource regionSource) {
        if (config.regionSize == 0 || regionSource == null) {
            throw new IllegalArgumentException("a page store needs regions: " + config.regionSize + ", "
                    + regionSource);
        }
        this.allocator = allocator;
        this.config = config;
        this.segmentSource = segmentSource;
        this.regionSource = regionSource;
        regionBlocks = config.segmentsPerRegion();
        regionAlignment = config.regionAlignment;
        fallbackSource = config.mallocRegionSize > 0 && config.regionSize != config.mallocRegionSize ?
                segmentSource.mallocRegionSource() : null;
        if (PlatformDependent.isJfrEnabled()) {
            PageStoreStateEvent.register(this);
        }
    }

    /**
     * As mimalloc's thread sequence: 0, 1, 2... per heap made, never negative. A heap's claims start in the block of
     * its sequence (see {@link Region#claimSlices}).
     */
    int nextHeapSequence() {
        return HEAP_SEQUENCE.getAndIncrement(this) & Integer.MAX_VALUE;
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
     * Maps a new region, unless one was added since {@code seen} was read: three system calls for {@code mmap}, one
     * allocation for a region of one block. When a region cannot be had, switches to {@link #fallbackSource} if there
     * is one, and makes no region: the caller scans again.
     */
    private synchronized void addRegion(Region[] seen, String heap) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        if (regions != seen) {
            return;
        }
        RegionSource source = regionSource;
        int blocks = regionBlocks;
        int size = blocks * config.segmentSize;
        AbstractByteBuf buffer;
        Object event = PlatformDependent.isJfrEnabled() && PageStoreMapEvent.isEventEnabled() ?
                PageStoreMapEvent.start() : null;
        try {
            buffer = source.allocateRegion(size, regionAlignment);
        } catch (OutOfMemoryError | RuntimeException e) {
            if (event != null) {
                AbstractPageStoreCallEvent.end(event, 0, size, seen.length, e);
            }
            RegionSource fallback = fallbackSource;
            if (fallback == null) {
                // A block (malloc, byte[]) that cannot be had, or the direct memory limit it is charged to: this
                // allocation fails, the next region may well fit.
                throw e;
            }
            fallbackSource = null;
            regionBlocks = 1;
            regionAlignment = 0;
            regionSource = fallback;
            logger.warn("Cannot map a region of {} bytes: regions are one block each from now on.", size, e);
            return;
        }
        assert buffer.capacity() == size;
        if (event != null) {
            AbstractPageStoreCallEvent.end(event, buffer._memoryAddress(), size, seen.length, null);
        }
        Region region = sharedRegion(buffer, source, blocks);
        if (!region.purgesSlices) {
            // Charged by its allocation, counted whole from now on.
            allocator.storeBytesCommitted(buffer._memoryAddress(), size, buffer.isDirect(), heap == THREAD_LOCAL);
        }
        int index = 0;
        while (index < seen.length && !seen[index].released) {
            index++;
        }
        // The place of a region given back is taken again: nothing claims in a released region.
        Region[] grown = index < seen.length ? seen.clone() : Arrays.copyOf(seen, seen.length + 1);
        region.index = index;
        grown[index] = region;
        regions = grown;
    }

    /** A region charged whole has memory behind all of it, free since now. */
    private Region sharedRegion(AbstractByteBuf buffer, RegionSource source, int blocks) {
        Region region = new Region(buffer, source, blocks, segmentSource, config, !source.canPurgeSlices(),
                System.nanoTime());
        for (int slot = 0; slot < region.slots; slot++) {
            Segment block = region.block(slot);
            block.sharedSpans = new AdaptivePoolingAllocator.SharedSpanChunk(block, this, false);
            block.threadLocalSpans = new AdaptivePoolingAllocator.SharedSpanChunk(block, this, true);
        }
        return region;
    }

    /**
     * Shared slices, any thread: claims a run of {@code slices} free slices of one block, at most a block, the first
     * fit from block {@code seq} on (see {@link Region#claimSlices}) in the first region that has one, mapping a new
     * region when none has. Returns the region's index in the high half and the run's first slice in the region in
     * the low half. The run's slices are committed (see {@link #commitSlices}); give it back with
     * {@link #releaseSlices}, from any thread.
     */
    long claimSlices(int slices, int seq, String heap) {
        boolean rescanned = false;
        for (;;) {
            long purgeSeen = purgeSequence;
            Region[] regions = this.regions;
            for (int i = 0; i < regions.length; i++) {
                Region region = regions[i];
                int slice = region.claimSlices(slices, seq);
                if (slice >= 0) {
                    int perBlock = config.slicesPerSegment();
                    commitSlices(region.block(slice / perBlock), slice % perBlock, slices, heap);
                    return (long) i << 32 | slice;
                }
            }
            if (!rescanned && awaitPurgedRun(purgeSeen)) {
                rescanned = true;
                continue;
            }
            addRegion(regions, heap);
        }
    }

    /**
     * Any thread: claims {@code blocks} contiguous wholly free blocks of one region, for a buffer larger than a block
     * (see {@link Region#claimBlocks}), mapping a new region when none has them. Returns the region's index in
     * {@link #regions} in the high half and the first block in the low half, or -1 when new regions hold fewer blocks.
     * Every slice is committed; give them back with {@link #freeRun}.
     */
    long takeRun(int blocks) {
        boolean rescanned = false;
        for (;;) {
            if (blocks > regionBlocks) {
                // Since regions are one block each.
                return -1;
            }
            long purgeSeen = purgeSequence;
            Region[] regions = this.regions;
            for (int i = 0; i < regions.length; i++) {
                Region region = regions[i];
                int first = region.claimBlocks(blocks);
                if (first >= 0) {
                    commitBlocks(region, first, blocks);
                    return (long) i << 32 | first;
                }
            }
            if (!rescanned && awaitPurgedRun(purgeSeen)) {
                rescanned = true;
                continue;
            }
            addRegion(regions, NO_HEAP);
        }
    }

    /**
     * Before a claim that found no fit maps a region: whether the purger held slices while the claim scanned (its
     * sequence moved since {@code seen}, or is odd), in which case this waits, by yields and at most
     * {@link #PURGE_WAIT_NANOS}, until it holds none, so that the caller scans once more instead of mapping a region
     * for slices that were only out for their purge.
     */
    private boolean awaitPurgedRun(long seen) {
        long current = purgeSequence;
        if (current == seen && (current & 1) == 0) {
            return false;
        }
        PURGE_WAITS.incrementAndGet(this);
        long start = System.nanoTime();
        while ((purgeSequence & 1) != 0 && System.nanoTime() - start < PURGE_WAIT_NANOS) {
            Thread.yield();
        }
        return true;
    }

    private void commitBlocks(Region region, int first, int blocks) {
        int committed = 0;
        try {
            for (; committed < blocks; committed++) {
                Segment block = region.block(first + committed);
                commitSlices(block, 0, block.slices, NO_HEAP);
            }
        } finally {
            if (committed < blocks) {
                // commitSlices gave back the block it failed on: the ones after it go back as they are.
                long now = System.nanoTime();
                for (int slot = 0; slot < committed; slot++) {
                    Segment block = region.block(first + slot);
                    block.releaseRun(0, block.slices, now);
                }
                for (int slot = committed + 1; slot < blocks; slot++) {
                    Segment block = region.block(first + slot);
                    block.giveBack(block.allFree);
                }
            }
        }
    }

    /**
     * The claimer's run of {@code n} slices of {@code block} from {@code start}: its slices with no memory behind them
     * are charged to the direct memory limit and counted in the used memory, all at once. On failure the run goes back,
     * any slice charged meanwhile staying charged and free, until it is purged.
     */
    private void commitSlices(Segment block, int start, int n, String heap) {
        Region region = block.region;
        int sliceSize = block.sliceSize;
        long[] freedAt = block.freedAt;
        int fresh = 0;
        for (int i = start; i < start + n; i++) {
            if (freedAt[i] == Region.UNCOMMITTED) {
                fresh++;
            }
        }
        long address = block.memoryAddress() + (long) start * sliceSize;
        boolean committed = false;
        try {
            if (fresh != 0) {
                PlatformDependent.incrementMemoryCounter(fresh * sliceSize);
                int base = block.slot * block.slices;
                for (int i = start; i < start + n; i++) {
                    if (freedAt[i] == Region.UNCOMMITTED) {
                        freedAt[i] = 0;
                        region.sliceEverCommitted[base + i] = true;
                    }
                }
                SLICES_COMMITTED.addAndGet(this, fresh);
                allocator.storeBytesCommitted(address, fresh * sliceSize, block.isDirect(), heap == THREAD_LOCAL);
                // A memory event, as the purge's give-back: from the run's start, for the slices committed now.
                taken(address, (long) fresh * sliceSize, 0, region.index, SHARED_SLICES, heap);
            }
            committed = true;
        } finally {
            if (!committed) {
                block.releaseRun(start, n, System.nanoTime());
            }
        }
    }

    /** The block of a run {@link #claimSlices} returned. */
    Segment block(long run) {
        return regions[(int) (run >>> 32)].block((int) run / config.slicesPerSegment());
    }

    /** The first slice in its block of a run {@link #claimSlices} returned. */
    int start(long run) {
        return (int) run % config.slicesPerSegment();
    }

    /**
     * Shared slices, any thread: the run of {@code n} slices of {@code block} from {@code start}, which the caller
     * claimed, goes back at once, free for any heap, and purged once idle.
     */
    void releaseSlices(Segment block, int start, int n) {
        block.releaseRun(start, n, System.nanoTime());
    }

    /** Any thread: the blocks {@link #takeRun} returned go back to the shared slices. */
    void freeRun(Region region, int start, int slots) {
        long now = System.nanoTime();
        for (int slot = start; slot < start + slots; slot++) {
            Segment block = region.block(slot);
            block.releaseRun(0, block.slices, now);
        }
    }

    Region region(int index) {
        return regions[index];
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
            if (region.released) {
                continue;
            }
            if (region.purgesSlices) {
                closeSlices(region);
            } else {
                allocator.storeBytesReleased(region.buffer._memoryAddress(), region.length, region.buffer.isDirect());
            }
            long address = region.buffer._memoryAddress();
            Object event = PlatformDependent.isJfrEnabled() && PageStoreUnmapEvent.isEventEnabled() ?
                    PageStoreUnmapEvent.start() : null;
            Throwable failure = null;
            try {
                region.source.releaseRegion(region.buffer);
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                if (event != null) {
                    AbstractPageStoreCallEvent.end(event, address, region.length, region.index, failure);
                }
            }
        }
    }

    /** Credits every committed slice of {@code region}: the close only. */
    private void closeSlices(Region region) {
        int sliceSize = config.sliceSize;
        for (int slot = 0; slot < region.slots; slot++) {
            Segment block = region.block(slot);
            int committed = 0;
            for (int i = 0; i < block.slices; i++) {
                if (block.freedAt[i] != Region.UNCOMMITTED) {
                    block.freedAt[i] = Region.UNCOMMITTED;
                    committed++;
                }
            }
            if (committed != 0) {
                PlatformDependent.decrementMemoryCounter(committed * sliceSize);
                allocator.storeBytesReleased(block.memoryAddress(), committed * sliceSize, block.isDirect());
            }
        }
    }

    /**
     * Any thread, from a heap's purge tick (see {@code IdleDecay#count}) or a one-shot buffer. At most once per
     * {@link PageStoreConfig#purgeCheckNanos}, and by one thread at a time (a try-guard: a caller that finds a purge
     * running returns at once), gives back to the OS the memory of the free slots that stayed free for
     * {@link PageStoreConfig#purgeDelayNanos} at least.
     */
    void purgeIfDue(long now) {
        if (now - lastPurgeNanos < config.purgeCheckNanos
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
        purgeSharedSlices(now);
        releaseIdleRegions(now);
    }

    /**
     * Shared slices: purges the free slices with memory behind them freed {@link PageStoreConfig#purgeDelayNanos} ago
     * or earlier, one call per run of contiguous ones within a block. As mimalloc v3's {@code mi_arena_try_purge_range}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L2345-L2359): a run's
     * slices are claimed by CAS first, so that no claim can take them while their memory goes, and given back after;
     * and as its {@code _mi_bitmap_forall_setc_ranges}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L1466-L1509), a run never
     * spans more than one bitmap word (a block): the purger holds at most one block's run at a time, and a claim
     * meanwhile finds every other free slice (see {@link #awaitPurgedRun}).
     */
    private void purgeSharedSlices(long now) {
        long delay = config.purgeDelayNanos;
        for (Region region : regions) {
            if (!region.purgesSlices) {
                continue;
            }
            for (int slot = 0; slot < region.slots; slot++) {
                Segment block = region.block(slot);
                long candidates = purgeable(block, block.free, now, delay);
                while (candidates != 0) {
                    long run = lowestRun(candidates);
                    candidates &= ~run;
                    purgeSequence++; // odd: the purger holds slices
                    try {
                        long claimed = block.claimFree(run);
                        long exact = purgeable(block, claimed, now, delay);
                        if (exact != claimed) {
                            block.giveBack(claimed & ~exact);
                        }
                        while (exact != 0) {
                            long bits = lowestRun(exact);
                            exact &= ~bits;
                            purgeSliceRun(block, bits);
                        }
                    } finally {
                        purgeSequence++;
                    }
                }
            }
        }
    }

    /**
     * Shared slices of a source that cannot purge part of a region: gives back each region all of whose slices stayed
     * free for the purge delay. Its blocks are claimed whole by CAS first, as a purge claims its run, so that no claim
     * can take a slice of it meanwhile; a region that is no longer wholly free and idle once claimed goes back to use.
     * A released region keeps its blocks claimed, so that a claim that still sees it finds nothing there, and its
     * place in {@link #regions} is taken by the next region mapped.
     */
    private void releaseIdleRegions(long now) {
        long delay = config.purgeDelayNanos;
        for (Region region : regions) {
            if (region.purgesSlices || region.released || !idle(region, now, delay, false)) {
                continue;
            }
            purgeSequence++; // odd: the purger holds slices
            try {
                int claimed = 0;
                while (claimed < region.slots && region.block(claimed).claimWhole()) {
                    claimed++;
                }
                if (claimed == region.slots && idle(region, now, delay, true) && release(region)) {
                    continue;
                }
                for (int slot = 0; slot < claimed; slot++) {
                    Segment block = region.block(slot);
                    block.giveBack(block.allFree);
                }
            } finally {
                purgeSequence++;
            }
        }
    }

    /**
     * Whether every slice of {@code region} is free, or claimed by the caller ({@code claimed}), and was freed
     * {@code delay} before {@code now} or earlier. Racy unless the caller claimed every block.
     */
    private static boolean idle(Region region, long now, long delay, boolean claimed) {
        for (int slot = 0; slot < region.slots; slot++) {
            Segment block = region.block(slot);
            if (!claimed && !block.isWhollyFree()) {
                return false;
            }
            for (long freed : block.freedAt) {
                if (now - freed < delay) {
                    return false;
                }
            }
        }
        return true;
    }

    /** {@code region}, which the purger holds whole, goes back to its source, unless the store was closed. */
    private synchronized boolean release(Region region) {
        if (closed) {
            return false;
        }
        long address = region.buffer._memoryAddress();
        region.released = true;
        allocator.storeBytesReleased(address, region.length, region.buffer.isDirect());
        Object event = PlatformDependent.isJfrEnabled() && PageStoreUnmapEvent.isEventEnabled() ?
                PageStoreUnmapEvent.start() : null;
        Throwable failure = null;
        try {
            region.source.releaseRegion(region.buffer);
            regionsReleased++;
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            if (event != null) {
                AbstractPageStoreCallEvent.end(event, address, region.length, region.index, failure);
            }
        }
        return true;
    }

    /**
     * Free slices with memory behind them of {@code slices}, freed {@code delay} before {@code now} or earlier. Racy
     * for slices the caller does not own, exact for those it does.
     */
    private static long purgeable(Segment block, long slices, long now, long delay) {
        long purgeable = 0;
        long[] freedAt = block.freedAt;
        for (long bits = slices; bits != 0; bits &= bits - 1) {
            int slice = Long.numberOfTrailingZeros(bits);
            long freed = freedAt[slice];
            if (freed != Region.UNCOMMITTED && now - freed >= delay) {
                purgeable |= 1L << slice;
            }
        }
        return purgeable;
    }

    /**
     * Purges the contiguous slices {@code bits} of {@code block}, which the purger claimed, with one call, then gives
     * them back. A run whose call fails keeps its memory, and stays purgeable.
     */
    private void purgeSliceRun(Segment block, long bits) {
        Region region = block.region;
        int sliceSize = block.sliceSize;
        int start = Long.numberOfTrailingZeros(bits);
        int n = Long.bitCount(bits);
        int offset = block.slot * config.segmentSize + start * sliceSize;
        int length = n * sliceSize;
        boolean purged = false;
        try {
            Object event = PlatformDependent.isJfrEnabled() && PageStorePurgeEvent.isEventEnabled() ?
                    PageStorePurgeEvent.start() : null;
            Throwable failure = null;
            try {
                region.source.purge(region.buffer, offset, length);
                purged = true;
            } catch (Throwable t) {
                failure = t;
            }
            purged(event, SLICES, region, offset, length, failure);
            if (failure != null) {
                purgeFailed(failure);
            }
        } finally {
            try {
                if (purged) {
                    for (int i = start; i < start + n; i++) {
                        block.freedAt[i] = Region.UNCOMMITTED;
                    }
                    // Credited before a claim can find the slices uncommitted and charge them again.
                    purgeCalls++;
                    bytesPurged += length;
                    slicesPurged += n;
                    PlatformDependent.decrementMemoryCounter(length);
                    long address = region.buffer._memoryAddress() + offset;
                    allocator.storeBytesReleased(address, length, block.isDirect());
                    givenBack(address, length, 0, region.index, PURGED_SLICES, NO_HEAP);
                }
            } finally {
                block.giveBack(bits);
            }
        }
    }

    /** The lowest run of contiguous set bits of {@code bits}, which is not 0. */
    private static long lowestRun(long bits) {
        int start = Long.numberOfTrailingZeros(bits);
        int length = Long.numberOfTrailingZeros(~(bits >>> start));
        return length == Long.SIZE ? -1L : (1L << length) - 1 << start;
    }

    private static final String SLICES = "slices";

    private static void purged(Object event, String unit, Region region, int offset, int length, Throwable failure) {
        if (event != null) {
            PageStorePurgeEvent.end(event, unit, region.buffer._memoryAddress() + offset, length, region.index,
                    failure);
        }
    }

    private void purgeFailed(Throwable cause) {
        if (purgeFailures++ == 0) {
            logger.warn("Cannot purge free slices: their memory stays committed. Further failures are logged at "
                    + "debug level.", cause);
        } else {
            logger.debug("Cannot purge free slices ({} failures).", purgeFailures, cause);
        }
    }

    /**
     * Racy, for tests and dumps, shared slices only: the slices of all regions {@code {claimed, free with memory
     * behind, free without}}. A slice the purger claimed counts as claimed.
     */
    int[] sliceCounts() {
        int[] counts = new int[3];
        for (Region region : regions) {
            if (region.released) {
                continue;
            }
            for (int slot = 0; slot < region.slots; slot++) {
                Segment block = region.block(slot);
                long free = block.free;
                for (int i = 0; i < block.slices; i++) {
                    if ((free & 1L << i) == 0) {
                        counts[0]++;
                    } else if (block.freedAt[i] != Region.UNCOMMITTED) {
                        counts[1]++;
                    } else {
                        counts[2]++;
                    }
                }
            }
        }
        return counts;
    }

    int regionCount() {
        return regions.length;
    }

}
