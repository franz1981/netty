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
 * whichever thread frees the run gives it back by CAS ({@link #claimSlices}, {@link Segment#releaseRun}), as
 * mimalloc v3 claims a page's slices straight from its arena's bitmap
 * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L240-L246). A hole one heap leaves is reused by any.
 * A buffer above a block takes a run of whole blocks ({@link #takeRun}). A new region is added under
 * this store's monitor, the only lock, when no region has a fit, and the claim that added it takes its run there
 * before any other thread sees the region.
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
    private static final AtomicIntegerFieldUpdater<PageStore> HEAP_SEQUENCE =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "heapSequence");
    private static final AtomicIntegerFieldUpdater<PageStore> ARMED =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "armed");
    private static final Region[] NO_REGIONS = new Region[0];
    /**
     * The calls a purge pass makes at most (see {@link #purge}). Measured with {@code mmap}, 1 GiB freed at once: 8 make
     * a pass of about 3 ms and give the GiB back in about a minute; one pass of all of it took 130 to 145 ms.
     */
    static final int PURGE_CALLS = 8;

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
    /** Set by tests. */
    volatile long lastPurgeNanos = System.nanoTime();
    // Read by every run release, written by the release that arms and by the purger: on this object's lines with
    // regions and slicesCommitted, which every claim reads.
    /** 1 once a run was released since the purger last disarmed: see {@link #armPurge}. */
    private volatile int armed;
    /** When {@link #armed} was set, as {@link System#nanoTime()}: a pass waits the purge delay from then. */
    private volatile long armedAt;
    // Read by tests and dumps.
    /** Shared slices claimed while no memory backed them. */
    volatile long slicesCommitted;
    /** The next heap's sequence: see {@link #nextHeapSequence}. */
    private volatile int heapSequence;
    // Written by the purger only.
    long purges;
    long purgeCalls;
    long bytesPurged;
    long purgeFailures;
    /** Shared slices purged. */
    long slicesPurged;
    /** Regions given back whole: see {@link #releaseIfIdle}. */
    long regionsReleased;
    /** Whether the pass left free slices that were not idle for the delay yet, and the longest any of them waited. */
    private boolean skipped;
    private long longestWait;
    /** Where the next pass starts: a region's index in {@link #regions}, and a block in it. */
    private int nextRegion;
    private int nextBlock;

    /**
     * @param regionSource where the regions come from, or {@code null} for {@code segmentSource}'s: {@code mmap} where
     *                     the config has regions and the source can map them, else {@code malloc}'d regions of one
     *                     block where the config has them
     */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, SegmentSource segmentSource,
              RegionSource regionSource) {
        if (regionSource == null) {
            regionSource = config.regionSize > 0 ? segmentSource.regionSource() : null;
            if (regionSource == null && config.mallocRegionSize > 0) {
                regionSource = segmentSource.mallocRegionSource();
                config = config.withMallocRegions();
            }
        }
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

    /**
     * Maps a new region, unless one was added since {@code seen} was read: three system calls for {@code mmap}, one
     * allocation for a region of one block. Claims the caller's run in it before publishing it: {@code blocks} whole
     * blocks, or if 0, {@code slices} slices of its first block. Published wholly free, a region could be given back
     * by the purger, idle, before the claim that mapped it scanned it, and the claim map another: with a short purge
     * delay, without end. Returns the run as {@link #claimSlices} and {@link #takeRun} encode theirs, not committed,
     * or -1 for the caller to scan again: a region was added meanwhile, the run does not fit a new region, or a region
     * cannot be had and this switched to {@link #fallbackSource}.
     */
    private synchronized long addRegion(Region[] seen, boolean threadLocal, int slices, int blocks) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        if (regions != seen) {
            return -1;
        }
        RegionSource source = regionSource;
        int slots = regionBlocks;
        int size = slots * config.segmentSize;
        AbstractByteBuf buffer;
        Object event = PlatformDependent.isJfrEnabled() && PageStoreMapEvent.isEventEnabled() ?
                PageStoreMapEvent.start() : null;
        try {
            buffer = source.allocateRegion(size, regionAlignment);
        } catch (OutOfMemoryError | RuntimeException e) {
            if (event != null) {
                AbstractPageStoreEvent.end(event, 0, size, seen.length, e);
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
            return -1;
        }
        assert buffer.capacity() == size;
        if (event != null) {
            AbstractPageStoreEvent.end(event, buffer._memoryAddress(), size, seen.length, null);
        }
        Region region = sharedRegion(buffer, source, slots);
        if (!region.purgesSlices) {
            // Charged by its allocation, counted whole from now on.
            allocator.storeBytesCommitted(buffer._memoryAddress(), size, buffer.isDirect(), threadLocal);
        }
        int index = 0;
        while (index < seen.length && !seen[index].released) {
            index++;
        }
        // The place of a region given back is taken again: nothing claims in a released region.
        Region[] grown = index < seen.length ? seen.clone() : Arrays.copyOf(seen, seen.length + 1);
        region.index = index;
        int first = blocks != 0 ? region.claimBlocks(blocks) : region.claimSlices(slices, 0);
        grown[index] = region;
        regions = grown;
        if (!region.purgesSlices) {
            // Its free slices have memory behind them, all of them if the claim failed.
            armPurge(System.nanoTime());
        }
        return first < 0 ? -1 : (long) index << 32 | first;
    }

    /** A region charged whole has memory behind all of it, free since now. */
    private Region sharedRegion(AbstractByteBuf buffer, RegionSource source, int blocks) {
        Region region = new Region(this, buffer, source, blocks, segmentSource, config, !source.canPurgeSlices(),
                System.nanoTime());
        for (int slot = 0; slot < region.slots; slot++) {
            Segment block = region.blocks[slot];
            block.sharedSpans = new AdaptivePoolingAllocator.SharedSpanChunk(block, this, false);
            block.threadLocalSpans = new AdaptivePoolingAllocator.SharedSpanChunk(block, this, true);
        }
        return region;
    }

    /**
     * Shared slices, any thread: claims a run of {@code slices} free slices of one block, at most a block, the first
     * fit from block {@code seq} on (see {@link Region#claimSlices}) in the first region that has one, else in a new
     * region (see {@link #addRegion}). Returns the region's index in the high half and the run's first slice in the
     * region in the low half. The run's slices are committed (see {@link #commitSlices}); give it back with
     * {@link Segment#releaseRun}, from any thread.
     */
    long claimSlices(int slices, int seq, boolean threadLocal) {
        for (;;) {
            Region[] regions = this.regions;
            for (int i = 0; i < regions.length; i++) {
                Region region = regions[i];
                int slice = region.claimSlices(slices, seq);
                if (slice >= 0) {
                    int perBlock = config.slicesPerSegment();
                    commitSlices(region.blocks[slice / perBlock], slice % perBlock, slices, threadLocal);
                    return (long) i << 32 | slice;
                }
            }
            long run = addRegion(regions, threadLocal, slices, 0);
            if (run >= 0) {
                commitSlices(block(run), start(run), slices, threadLocal);
                return run;
            }
        }
    }

    /**
     * Any thread: claims {@code blocks} contiguous wholly free blocks of one region, for a buffer larger than a block
     * (see {@link Region#claimBlocks}), else in a new region (see {@link #addRegion}). Returns the region's index in
     * {@link #regions} in the high half and the first block in the low half, or -1 when new regions hold fewer blocks.
     * Every slice is committed; give them back with {@link #freeRun}.
     */
    long takeRun(int blocks) {
        for (;;) {
            if (blocks > regionBlocks) {
                // Since regions are one block each.
                return -1;
            }
            Region[] regions = this.regions;
            for (int i = 0; i < regions.length; i++) {
                Region region = regions[i];
                int first = region.claimBlocks(blocks);
                if (first >= 0) {
                    commitBlocks(region, first, blocks);
                    return (long) i << 32 | first;
                }
            }
            long run = addRegion(regions, false, 0, blocks);
            if (run >= 0) {
                commitBlocks(this.regions[(int) (run >>> 32)], (int) run, blocks);
                return run;
            }
        }
    }

    private void commitBlocks(Region region, int first, int blocks) {
        int committed = 0;
        try {
            for (; committed < blocks; committed++) {
                Segment block = region.blocks[first + committed];
                commitSlices(block, 0, block.slices, false);
            }
        } finally {
            if (committed < blocks) {
                // commitSlices gave back the block it failed on: the ones after it go back as they are.
                long now = System.nanoTime();
                for (int slot = 0; slot < committed; slot++) {
                    Segment block = region.blocks[first + slot];
                    block.releaseRun(0, block.slices, now);
                }
                for (int slot = committed + 1; slot < blocks; slot++) {
                    Segment block = region.blocks[first + slot];
                    block.giveBack(block.allFree);
                }
                armPurge(now);
            }
        }
    }

    /**
     * The claimer's run of {@code n} slices of {@code block} from {@code start}: its slices with no memory behind them
     * are charged to the direct memory limit and counted in the used memory, all at once. On failure the run goes back,
     * any slice charged meanwhile staying charged and free, until it is purged.
     */
    private void commitSlices(Segment block, int start, int n, boolean threadLocal) {
        Region region = block.region;
        int sliceSize = block.sliceSize;
        long[] freedAt = block.freedAt;
        int fresh = 0;
        for (int i = start; i < start + n; i++) {
            if (freedAt[i] == Region.UNCOMMITTED) {
                fresh++;
            }
        }
        long address = block.buffer._memoryAddress() + (long) start * sliceSize;
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
                allocator.storeBytesCommitted(address, fresh * sliceSize, block.buffer.isDirect(), threadLocal);
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
        return regions[(int) (run >>> 32)].blocks[(int) run / config.slicesPerSegment()];
    }

    /** The first slice in its block of a run {@link #claimSlices} returned. */
    int start(long run) {
        return (int) run % config.slicesPerSegment();
    }

    /** Any thread: the blocks {@link #takeRun} returned go back to the shared slices. */
    void freeRun(Region region, int start, int slots) {
        long now = System.nanoTime();
        for (int slot = start; slot < start + slots; slot++) {
            Segment block = region.blocks[slot];
            block.releaseRun(0, block.slices, now);
        }
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
                    AbstractPageStoreEvent.end(event, address, region.length, region.index, failure);
                }
            }
        }
    }

    /** Credits every committed slice of {@code region}: the close only. */
    private void closeSlices(Region region) {
        int sliceSize = config.sliceSize;
        for (int slot = 0; slot < region.slots; slot++) {
            Segment block = region.blocks[slot];
            int committed = 0;
            for (int i = 0; i < block.slices; i++) {
                if (block.freedAt[i] != Region.UNCOMMITTED) {
                    block.freedAt[i] = Region.UNCOMMITTED;
                    committed++;
                }
            }
            if (committed != 0) {
                PlatformDependent.decrementMemoryCounter(committed * sliceSize);
                allocator.storeBytesReleased(block.buffer._memoryAddress(), committed * sliceSize,
                        block.buffer.isDirect());
            }
        }
    }

    /**
     * Any thread, after it released a run at {@code now}: arms the purge, unless armed. The common case reads one
     * shared field; only the release that finds it disarmed writes, unlike mimalloc v3, which sets its arena's purge
     * expiry by CAS on every free (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L2325). A purger
     * that sees {@link #armed} before {@link #armedAt} is written may run a pass early: it finds the run not idle yet
     * and arms again for it.
     */
    void armPurge(long now) {
        if (armed == 0 && ARMED.compareAndSet(this, 0, 1)) {
            armedAt = now;
        }
    }

    /**
     * Any thread, from a heap's purge tick (see {@code IdleDecay#count}) or a one-shot buffer. At most once per
     * {@link PageStoreConfig#purgeCheckNanos}, only once the purge was armed {@link PageStoreConfig#purgeDelayNanos}
     * ago (see {@link #armPurge}), and by one thread at a time (a try-guard: a caller that finds a purge running
     * returns at once), gives back to the OS the memory of the free slices, or of the regions of one block, that
     * stayed free for the delay at least. The pass disarms the purge before it scans, so that a release during the
     * pass arms it again, and arms it again itself for the free slices it found not idle long enough yet.
     */
    void purgeIfDue(long now) {
        if (!isDue(now) || !PURGING.compareAndSet(this, 0, 1)) {
            return;
        }
        try {
            if (isDue(now)) {
                lastPurgeNanos = now;
                armed = 0;
                purge(now);
            }
        } finally {
            purging = 0;
        }
    }

    private boolean isDue(long now) {
        return now - lastPurgeNanos >= config.purgeCheckNanos && armed != 0
                && now - armedAt >= config.purgeDelayNanos;
    }

    /**
     * Purger: arms the purge as if armed {@code waited} before {@code now}, unless armed to be due sooner. Racy against
     * a release that arms meanwhile: either arming stands, and a pass that finds slices not idle yet arms again.
     */
    private void rearm(long now, long waited) {
        if (armed == 0 || now - armedAt < waited) {
            armedAt = now - waited;
            armed = 1;
        }
    }

    /** Purger: free slices, or a region, free for {@code waited}, short of the delay, are left for a later pass. */
    private void skip(long waited) {
        if (!skipped || waited > longestWait) {
            skipped = true;
            longestWait = waited;
        }
    }

    /**
     * One pass: the blocks of every region in turn, from where the last pass stopped and around to it, at most
     * {@link #PURGE_CALLS} calls ({@code madvise} or region releases); one that stops short arms the purge again, due at
     * once, and the next pass, after the cadence floor, goes on from where this one stopped. mimalloc v3 bounds a pass
     * by arenas purged instead, a quarter of them plus one, from an arena chosen by the thread's sequence
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L2433-L2450); one region of ours can need
     * thousands of calls.
     */
    private void purge(long now) {
        purges++;
        skipped = false;
        Region[] regions = this.regions;
        int count = regions.length;
        int first = nextRegion < count ? nextRegion : 0;
        int from = first == nextRegion ? nextBlock : 0;
        int budget = PURGE_CALLS;
        // Region first from block from on, every other region, then region first up to block from.
        for (int k = 0; k <= count && count != 0; k++) {
            int index = first + k < count ? first + k : first + k - count;
            Region region = regions[index];
            int start = k == 0 ? from : 0;
            // A region given back whole is visited once, as its first block.
            int end = Math.min(k < count ? region.slots : from, region.purgesSlices ? region.slots : 1);
            for (int slot = start; slot < end && !region.released; slot++) {
                budget -= region.purgesSlices ? purgeBlock(region.blocks[slot], now, budget) :
                        releaseIfIdle(region, now);
                if (budget == 0) {
                    // This block may have more.
                    nextRegion = index;
                    nextBlock = slot;
                    rearm(now, config.purgeDelayNanos);
                    return;
                }
            }
        }
        if (skipped) {
            rearm(now, longestWait);
        }
    }

    /**
     * Shared slices: purges the free slices of {@code block} with memory behind them freed
     * {@link PageStoreConfig#purgeDelayNanos} ago or earlier, one call per run of contiguous ones, at most
     * {@code budget} calls, and returns the calls made. As mimalloc v3's {@code mi_arena_try_purge_range}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L2345-L2359): a run's
     * slices are claimed by CAS first, so that no claim can take them while their memory goes, and given back after;
     * and as its {@code _mi_bitmap_forall_setc_ranges}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L1466-L1509), a run never
     * spans more than one bitmap word (a block): the purger holds at most one block's run at a time, and a claim
     * meanwhile finds every other free slice. A claim never waits for the purger: one that finds no fit maps a region,
     * as mimalloc v3's claim fails on slices its purge holds
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L246) and reserves a new arena
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L548-L564).
     */
    private int purgeBlock(Segment block, long now, int budget) {
        long delay = config.purgeDelayNanos;
        int calls = 0;
        long candidates = purgeable(block, block.free, now, delay);
        while (candidates != 0 && calls < budget) {
            long run = lowestRun(candidates);
            candidates &= ~run;
            long claimed = block.claimFree(run);
            long exact = purgeable(block, claimed, now, delay);
            if (exact != claimed) {
                block.giveBack(claimed & ~exact);
            }
            while (exact != 0) {
                if (calls == budget) {
                    block.giveBack(exact);
                    break;
                }
                long bits = lowestRun(exact);
                exact &= ~bits;
                purgeSliceRun(block, bits);
                calls++;
            }
        }
        return calls;
    }

    /**
     * Gives back {@code region}, of a source that cannot purge part of a region, if all its slices stayed free for the
     * purge delay, and returns the calls made: 1 if it did. Its blocks are claimed whole by CAS first, as a purge
     * claims its run, so that no claim can take a slice of it meanwhile; a region that is no longer wholly free and
     * idle once claimed goes back to use. A released region keeps its blocks claimed, so that a claim that still sees
     * it finds nothing there, and its place in {@link #regions} is taken by the next region mapped.
     */
    private int releaseIfIdle(Region region, long now) {
        long delay = config.purgeDelayNanos;
        if (!whollyFree(region)) {
            return 0;
        }
        long waited = shortestWait(region, now);
        if (waited < delay) {
            skip(waited);
            return 0;
        }
        int claimed = 0;
        while (claimed < region.slots && region.blocks[claimed].claimWhole()) {
            claimed++;
        }
        if (claimed == region.slots && shortestWait(region, now) >= delay && release(region)) {
            return 1;
        }
        for (int slot = 0; slot < claimed; slot++) {
            Segment block = region.blocks[slot];
            block.giveBack(block.allFree);
        }
        return 0;
    }

    /** Racy: whether every slice of {@code region} is free. */
    private static boolean whollyFree(Region region) {
        for (Segment block : region.blocks) {
            if (!block.isWhollyFree()) {
                return false;
            }
        }
        return true;
    }

    /**
     * How long, at {@code now}, the slice of {@code region} freed last has been free: the region is idle once this
     * reaches the delay. Racy unless the caller claimed every block.
     */
    private static long shortestWait(Region region, long now) {
        long shortest = Long.MAX_VALUE;
        for (Segment block : region.blocks) {
            for (long freed : block.freedAt) {
                shortest = Math.min(shortest, now - freed);
            }
        }
        return shortest;
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
                AbstractPageStoreEvent.end(event, address, region.length, region.index, failure);
            }
        }
        return true;
    }

    /**
     * Free slices with memory behind them of {@code slices}, freed {@code delay} before {@code now} or earlier; the
     * others with memory behind them are skipped (see {@link #skip}). Racy for slices the caller does not own, exact
     * for those it does.
     */
    private long purgeable(Segment block, long slices, long now, long delay) {
        long purgeable = 0;
        long[] freedAt = block.freedAt;
        for (long bits = slices; bits != 0; bits &= bits - 1) {
            int slice = Long.numberOfTrailingZeros(bits);
            long freed = freedAt[slice];
            if (freed == Region.UNCOMMITTED) {
                continue;
            }
            long waited = now - freed;
            if (waited >= delay) {
                purgeable |= 1L << slice;
            } else {
                skip(waited);
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
            if (event != null) {
                AbstractPageStoreEvent.end(event, region.buffer._memoryAddress() + offset, length, region.index,
                        failure);
            }
            if (failure != null) {
                purgeFailed(failure);
                // Still purgeable: the next pass tries again.
                skip(config.purgeDelayNanos);
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
                    allocator.storeBytesReleased(address, length, block.buffer.isDirect());
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
                Segment block = region.blocks[slot];
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
