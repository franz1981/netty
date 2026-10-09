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

import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The units, largest first, and their names in the code:
 * <pre>
 * region      one piece of memory, mapped or allocated whole        {@link Region}
 *  block      4 MiB, one 64-bit free bitmap                       {@link Segment}
 *   slice     64 KiB, one bit of its block's bitmap
 *    chunk    a run of slices serving one size class              SizeClassedChunk
 *     slot    one buffer's place in a chunk                       SizeClassedChunk's slotSize
 *    span     a run of slices holding one buffer above the sizes  Heap#allocateLarge, SharedSpanChunk
 * heap        a thread-local heap or a stripe                     AdaptivePoolingAllocator.Heap
 * </pre>
 * Heaps own chunks, never blocks: any heap claims and releases a run of slices from the regions' shared bitmaps by
 * CAS ({@link #claimSlices}, {@link Segment#releaseRun}), found through bins and chunkmaps ({@link #binOf},
 * {@link #maps}), as mimalloc v3's bitmap.h. {@code mmap} regions ({@link #mmap}) purge idle free slices in place;
 * one-block regions ({@link #memory}) are purged whole. One purger at a time: see {@link #purgeIfDue}.
 */
final class PageStore {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStore.class);
    private static final AtomicIntegerFieldUpdater<PageStore> PURGING =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "purging");
    private static final AtomicIntegerFieldUpdater<PageStore> ARMED =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "armed");
    private static final AtomicReferenceFieldUpdater<PageStore, SizeClassedChunk> ABANDONED =
            AtomicReferenceFieldUpdater.newUpdater(PageStore.class, SizeClassedChunk.class, "abandoned");
    private static final Region[] NO_REGIONS = new Region[0];
    /** The bytes a purge pass gives back before it stops: bytes, not calls, as a call costs about what it gives. */
    static final long PURGE_BYTES = 64L << 20;

    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    /** Per run length in slices, its bin, so short runs do not cut the holes long ones leave. */
    final byte[] binOf;
    final int otherBin;
    /** Per bin, the run length whose fit its bit in {@link #maps} stands for: 1 for {@link #otherBin}. */
    final int[] binSlices;
    /** The map of the empty blocks in {@link #maps}, after one per bin. */
    final int emptyMap;
    /** A block's id is its region's index shifted by this, or its slot: a region's blocks have consecutive ids. */
    private final int idShift;
    /**
     * Bitmaps over the block ids: per bin, a bit set when a block of that bin has a free run of the bin's length;
     * and the empty blocks, for any bin. Hints only: the truth is each block's {@link Segment#free}, so a stale bit
     * is corrected on the claim that finds it wrong ({@link #unmark}).
     */
    private volatile AtomicLongArray maps;
    final MemorySource memory;
    /** Where new regions are mapped, or {@code null}: one block from {@link #memory} instead. */
    volatile MmapRegionSource mmap;
    /** The blocks of a new region, and where it starts: 1 and 0 once {@link #mmap} fails and is given up. */
    private volatile int regionBlocks;
    private int regionAlignment;
    /** Replaced under {@link #lock}, one longer or with a released region's place taken; read without it. */
    volatile Region[] regions = NO_REGIONS;
    /** Taken to add a region, to give one back whole, and to close: never on a claim or a release. */
    private final ReentrantLock lock = new ReentrantLock();
    /** Under {@link #lock}. */
    private boolean closed;
    private volatile int purging;
    volatile long lastPurgeNanos = System.nanoTime();
    /** 1 once a run was released since the purger last disarmed: read by every release, written by one at a time. */
    private volatile int armed;
    private volatile long armedAt;
    /** Chunks abandoned since the last pass, a stack linked through {@code nextInQueue}: see {@link #abandon}. */
    private volatile SizeClassedChunk abandoned;
    /** Purger only: abandoned chunks with buffers still out, linked through {@code nextInQueue}. */
    private SizeClassedChunk waiting;
    long purgeFailures;
    private boolean skipped;
    private long longestWait;
    /** Where the next pass starts: a region's index in {@link #regions}, and a block in it. */
    private int nextRegion;
    private int nextBlock;

    /**
     * @param mmap where {@code mmap} regions come from, or {@code null}: {@code memory}'s regions of one block
     *             instead, or where the config has no regions at all
     */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, MemorySource memory,
              MmapRegionSource mmap) {
        if (mmap != null && config.regionSize == 0) {
            throw new IllegalArgumentException("a page store needs regions: " + config.regionSize + ", " + mmap);
        }
        if (mmap == null) {
            if (config.mallocRegionSize <= 0) {
                throw new IllegalArgumentException("a page store needs regions: " + config.regionSize + ", " + mmap);
            }
            config = config.withMallocRegions();
        }
        this.allocator = allocator;
        this.config = config;
        this.memory = memory;
        int perBlock = config.slicesPerSegment();
        int[] kinds = allocator.table.pageKinds;
        otherBin = kinds.length;
        binOf = new byte[perBlock + 1];
        Arrays.fill(binOf, (byte) otherBin);
        binSlices = new int[otherBin + 1];
        binSlices[otherBin] = 1;
        for (int bin = 0; bin < kinds.length; bin++) {
            binOf[kinds[bin]] = (byte) bin;
            binSlices[bin] = kinds[bin];
        }
        emptyMap = otherBin + 1;
        idShift = Integer.SIZE - Integer.numberOfLeadingZeros(config.segmentsPerRegion() - 1);
        maps = new AtomicLongArray(emptyMap + 1);
        this.mmap = mmap;
        regionBlocks = config.segmentsPerRegion();
        regionAlignment = config.regionAlignment;
        if (PlatformDependent.isJfrEnabled()) {
            PageStoreStateEvent.register(this);
        }
    }

    /** Maps a new region, unless one was added since {@code seen} was read, claiming the caller's run in it before
     *  publishing it: published empty, the purger could give it back idle before this claim scanned it. One adder
     *  at a time: the ones that wait find the region added and claim in it. */
    private long addRegion(Region[] seen, boolean threadLocal, int slices, int blocks) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("closed");
            }
            if (regions != seen) {
                return -1;
            }
            MmapRegionSource mmap = this.mmap;
            int slots = regionBlocks;
            int size = slots * config.segmentSize;
            AbstractByteBuf buffer;
            AbstractPageStoreEvent event = PageStoreEvents.beginMap();
            try {
                buffer = mmap != null ? mmap.allocateRegion(size, regionAlignment) : memory.allocate(size, size);
            } catch (OutOfMemoryError | RuntimeException e) {
                PageStoreEvents.end(event, 0, size, seen.length, e);
                if (mmap == null || config.mallocRegionSize <= 0 || config.regionSize == config.mallocRegionSize) {
                    // A block (malloc, byte[]) that cannot be had, or the direct memory limit it is charged to: this
                    // allocation fails, the next region may well fit.
                    throw e;
                }
                this.mmap = null;
                regionBlocks = 1;
                regionAlignment = 0;
                logger.warn("Cannot map a region of {} bytes: regions are one block each from now on.", size, e);
                return -1;
            }
            assert buffer.capacity() == size;
            PageStoreEvents.end(event, buffer._memoryAddress(), size, seen.length, null);
            return publish(seen, threadLocal, slices, blocks, buffer, mmap, slots, size);
        } finally {
            lock.unlock();
        }
    }

    /** Under {@link #lock}: publishes the region of {@code buffer}, or gives {@code buffer} back if that fails. */
    private long publish(Region[] seen, boolean threadLocal, int slices, int blocks, AbstractByteBuf buffer,
                         MmapRegionSource mmap, int slots, int size) {
        int index = 0;
        while (index < seen.length && !seen[index].released) {
            index++;
        }
        Region region = null;
        boolean published = false;
        try {
            // A region charged whole has memory behind all of it, free since now.
            region = new Region(this, buffer, mmap, slots, config, mmap == null, System.nanoTime(), index);
            if (region.source == null) {
                // Charged by its allocation, counted whole from now on.
                allocator.memoryCommitted(buffer._memoryAddress(), size, buffer.isDirect(), true, threadLocal);
            }
            // The place of a region given back is taken again: nothing claims in a released region.
            Region[] grown = index < seen.length ? seen.clone() : Arrays.copyOf(seen, seen.length + 1);
            growMaps(index);
            int first;
            if (blocks != 0) {
                first = region.claimBlocks(blocks);
                if (first >= 0) {
                    first *= config.slicesPerSegment();
                }
            } else {
                Segment block = region.blocks[0];
                first = block.claimFirst(slices, binOf[slices]) ? 0 : -1;
            }
            grown[index] = region;
            regions = grown;
            published = true;
            for (Segment block : region.blocks) {
                slicesReleased(block, block.free);
            }
            if (region.source == null) {
                // Its free slices have memory behind them, all of them if the claim failed.
                armPurge(System.nanoTime());
            }
            return first < 0 ? -1 : run(index, first);
        } finally {
            if (!published) {
                // A throw (out of heap) before the region was reachable: nothing else would ever give it back.
                if (region != null) {
                    releaseRegion(region);
                } else {
                    buffer.release();
                }
            }
        }
    }

    /** Shared slices, any thread: claims a run of {@code slices} free slices of one block, lowest fit first, so
     *  the highest blocks drain and go back. Give it back with {@link Segment#releaseRun}, from any thread. */
    long claimSlices(int slices, boolean threadLocal) {
        int bin = binOf[slices];
        for (;;) {
            Region[] regions = this.regions;
            long run = claimFit(regions, bin, slices);
            if (run < 0) {
                run = claimEmpty(regions, bin, slices);
            }
            if (run < 0) {
                run = addRegion(regions, threadLocal, slices, 0);
            }
            if (run >= 0) {
                commitSlices(block(run), start(run), slices, threadLocal);
                return run;
            }
        }
    }

    /** The lowest fit of {@code n} slices in a block of {@code bin} that {@link #maps} shows, or -1. */
    private long claimFit(Region[] regions, int bin, int n) {
        AtomicLongArray maps = this.maps;
        int words = wordsPerMap(maps);
        int base = bin * words;
        int length = binSlices[bin];
        for (int w = 0; w < words; w++) {
            for (long bits = maps.get(base + w); bits != 0; bits &= bits - 1) {
                int id = w << 6 | Long.numberOfTrailingZeros(bits);
                Segment block = blockOf(regions, id);
                if (block == null) {
                    // A region not seen yet.
                    continue;
                }
                if (block.bin == bin) {
                    int claimed = block.claimRun(n, bin);
                    if (claimed >= 0) {
                        return claimed(block, id, bin, claimed);
                    }
                    if (length != n && block.hasFit(length)) {
                        // A run of another length of the other bin.
                        continue;
                    }
                }
                unmark(bin, id);
            }
        }
        return -1;
    }

    /** The first {@code n} slices of the lowest empty block {@link #maps} shows, for {@code bin}, or -1. */
    private long claimEmpty(Region[] regions, int bin, int n) {
        AtomicLongArray maps = this.maps;
        int words = wordsPerMap(maps);
        int base = emptyMap * words;
        for (int w = 0; w < words; w++) {
            for (long bits = maps.get(base + w); bits != 0; bits &= bits - 1) {
                int id = w << 6 | Long.numberOfTrailingZeros(bits);
                Segment block = blockOf(regions, id);
                if (block == null) {
                    continue;
                }
                if (block.claimFirst(n, bin)) {
                    return claimed(block, id, bin, Segment.FIRST);
                }
                unmark(emptyMap, id);
            }
        }
        return -1;
    }

    /** After a claim of {@code bin} in {@code block}: the maps show it as it now is, and as not empty if it was. */
    private long claimed(Segment block, int id, int bin, int claimed) {
        if ((claimed & Segment.FIRST) != 0) {
            // This claim holds its run: the block cannot be empty again before the bit is clear.
            clear(emptyMap, id);
        }
        if (block.hasFit(binSlices[bin])) {
            mark(bin, id);
        } else {
            unmark(bin, id);
        }
        return run(block.region.index, block.slot * block.slices + (claimed & Segment.START));
    }

    /** Any thread, after slices of {@code block} were freed, leaving {@code free}: sets the bit it now has. */
    void slicesReleased(Segment block, long free) {
        int id = id(block);
        if (free == block.allFree) {
            mark(emptyMap, id);
        } else {
            int bin = block.bin;
            if (Segment.hasFit(free, binSlices[bin])) {
                mark(bin, id);
            }
        }
    }

    int id(Segment block) {
        return block.region.index << idShift | block.slot;
    }

    /** Whether {@code block} has its bit in {@code map}. Racy. */
    private boolean hasBitIn(int map, Segment block) {
        return map == emptyMap ? block.isEmpty() : block.bin == map && block.hasFit(binSlices[map]);
    }

    private Segment blockOf(Region[] regions, int id) {
        int index = id >>> idShift;
        if (index >= regions.length) {
            return null;
        }
        Region region = regions[index];
        int slot = id & (1 << idShift) - 1;
        return slot < region.slots ? region.blocks[slot] : null;
    }

    /** Sets bit {@code id} of {@code map}, unless set: again in the copy that replaced it meanwhile, if any. */
    void mark(int map, int id) {
        AtomicLongArray maps = this.maps;
        long bit = 1L << id;
        for (;;) {
            int i = word(maps, map, id);
            long word = maps.get(i);
            if ((word & bit) != 0 || maps.compareAndSet(i, word, word | bit)) {
                AtomicLongArray now = this.maps;
                if (now == maps) {
                    return;
                }
                maps = now;
            }
        }
    }

    /** Clears bit {@code id} of {@code map}, and returns whether it was set. A clear lost to a copy is a stale bit. */
    private boolean clear(int map, int id) {
        AtomicLongArray maps = this.maps;
        int i = word(maps, map, id);
        long bit = 1L << id;
        for (;;) {
            long word = maps.get(i);
            if ((word & bit) == 0) {
                return false;
            }
            if (maps.compareAndSet(i, word, word & ~bit)) {
                return true;
            }
        }
    }

    /** Clears bit {@code id} of {@code map}, found wrong, then sets it again if a release raced it back true. */
    private void unmark(int map, int id) {
        if (clear(map, id)) {
            Segment block = blockOf(regions, id);
            if (block != null && hasBitIn(map, block)) {
                mark(map, id);
            }
        }
    }

    /** Under {@link #lock}: grows the maps to hold region {@code index}'s block ids before it is published;
     *  a mark lost to a reader mid-copy is caught by copying twice. */
    private void growMaps(int index) {
        AtomicLongArray old = maps;
        int count = emptyMap + 1;
        int words = wordsPerMap(old);
        int needed = ((index + 1 << idShift) + Long.SIZE - 1) >>> 6;
        if (needed <= words) {
            return;
        }
        int grownWords = Math.max(needed, 2 * words);
        AtomicLongArray grown = new AtomicLongArray(grownWords * count);
        copyMaps(old, words, grown, grownWords);
        maps = grown;
        copyMaps(old, words, grown, grownWords);
    }

    private int wordsPerMap(AtomicLongArray maps) {
        return maps.length() / (emptyMap + 1);
    }

    private int word(AtomicLongArray maps, int map, int id) {
        return map * wordsPerMap(maps) + (id >>> 6);
    }

    private static void copyMaps(AtomicLongArray from, int words, AtomicLongArray to, int toWords) {
        for (int map = 0; map < from.length() / words; map++) {
            for (int w = 0; w < words; w++) {
                long bits = from.get(map * words + w);
                int i = map * toWords + w;
                for (;;) {
                    long word = to.get(i);
                    if ((word | bits) == word || to.compareAndSet(i, word, word | bits)) {
                        break;
                    }
                }
            }
        }
    }

    // Visible for testing: racy, whether the bit of block is set in map.
    boolean marked(int map, Segment block) {
        AtomicLongArray maps = this.maps;
        int id = id(block);
        return (maps.get(word(maps, map, id)) & 1L << id) != 0;
    }

    /** Any thread: claims {@code blocks} contiguous empty blocks of one region, else in a new region. Every slice
     *  is committed; give them back with {@link #releaseBlocks}. */
    long claimBlocks(int blocks) {
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
                    return run(i, first * config.slicesPerSegment());
                }
            }
            long claimed = addRegion(regions, false, 0, blocks);
            if (claimed >= 0) {
                commitBlocks(region(claimed), firstBlock(claimed), blocks);
                return claimed;
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
                region.unclaimBlocks(first + committed + 1, blocks - committed - 1);
                armPurge(now);
            }
        }
    }

    /** The claimer's run of {@code n} slices of {@code block} from {@code start}: slices with no memory behind
     *  them are charged and counted, all at once; on failure the run goes back, charged slices staying charged. */
    private void commitSlices(Segment block, int start, int n, boolean threadLocal) {
        int sliceSize = block.sliceSize;
        long fresh = block.fresh(block.bits(start, n));
        int freshCount = Long.bitCount(fresh);
        boolean committed = false;
        try {
            if (freshCount != 0) {
                PlatformDependent.incrementMemoryCounter(freshCount * sliceSize);
                block.commit(fresh);
                long address = block.address() + (long) start * sliceSize;
                allocator.memoryCommitted(address, freshCount * sliceSize, block.buffer.isDirect(), true, threadLocal);
            }
            committed = true;
        } finally {
            if (!committed) {
                block.releaseRun(start, n, System.nanoTime());
            }
        }
    }

    /** {@code region << 32 | slice}: the run {@link #claimSlices} and {@link #claimBlocks} return, built only here. */
    private static long run(int region, int slice) {
        return (long) region << 32 | slice;
    }

    Region region(long run) {
        return regions[(int) (run >>> 32)];
    }

    Segment block(long run) {
        return region(run).blocks[firstBlock(run)];
    }

    int start(long run) {
        return (int) run % config.slicesPerSegment();
    }

    int firstBlock(long run) {
        return (int) run / config.slicesPerSegment();
    }

    void releaseBlocks(Region region, int start, int slots) {
        long now = System.nanoTime();
        for (int slot = start; slot < start + slots; slot++) {
            Segment block = region.blocks[slot];
            block.releaseRun(0, block.slices, now);
        }
    }

    /** Unmaps every region: only when nothing can touch them any more (the allocator is unreachable). */
    void close() {
        lock.lock();
        try {
            closed = true;
            Region[] regions = this.regions;
            this.regions = NO_REGIONS;
            for (Region region : regions) {
                if (region.released) {
                    continue;
                }
                releaseRegion(region);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Credits {@code region}'s committed slices back, and its buffer goes to {@link Region#release()}. */
    private void releaseRegion(Region region) {
        if (region.source != null) {
            closeSlices(region);
        } else {
            allocator.memoryReleased(region.buffer._memoryAddress(), region.length, region.buffer.isDirect(), true);
        }
        long address = region.buffer._memoryAddress();
        AbstractPageStoreEvent event = PageStoreEvents.beginUnmap();
        Throwable failure = null;
        try {
            region.release();
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            PageStoreEvents.end(event, address, region.length, region.index, failure);
        }
    }

    private void closeSlices(Region region) {
        int sliceSize = config.sliceSize;
        for (Segment block : region.blocks) {
            int committed = block.takeCommitted();
            if (committed != 0) {
                PlatformDependent.decrementMemoryCounter(committed * sliceSize);
                allocator.memoryReleased(block.address(), committed * sliceSize, block.buffer.isDirect(), true);
            }
        }
    }

    /** Any thread, after it released a run at {@code now}: arms the purge, unless armed, so only one release writes. */
    void armPurge(long now) {
        if (armed == 0 && ARMED.compareAndSet(this, 0, 1)) {
            armedAt = now;
        }
    }

    /**
     * Any thread: at most once per {@link PageStoreConfig#purgeCheckNanos}, once armed {@link
     * PageStoreConfig#purgeDelayNanos} ago, and by one thread at a time, purges. Disarms before it scans, so a
     * release during the pass arms it again.
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

    /** Purger: arms the purge as if armed {@code waited} before {@code now}, unless armed to be due sooner. */
    private void rearm(long now, long waited) {
        if (armed == 0 || now - armedAt < waited) {
            armedAt = now - waited;
            armed = 1;
        }
    }

    /** Purger: free slices, or a region, free for {@code waited}, short of the delay, are left for a later pass. */
    void skip(long waited) {
        if (!skipped || waited > longestWait) {
            skipped = true;
            longestWait = waited;
        }
    }

    /** One pass: every region in turn, from where the last pass stopped, until its calls gave back
     *  {@link #PURGE_BYTES}, the call that reaches it made whole. */
    private void purge(long now) {
        skipped = false;
        releaseAbandoned(now);
        Region[] regions = this.regions;
        int count = regions.length;
        int first = nextRegion < count ? nextRegion : 0;
        int from = first == nextRegion ? nextBlock : 0;
        long budget = PURGE_BYTES;
        // Region first from block from on, every other region, then region first up to block from.
        for (int k = 0; k <= count && count != 0; k++) {
            int index = first + k < count ? first + k : first + k - count;
            Region region = regions[index];
            int start = k == 0 ? from : 0;
            // A region given back whole is visited once, as its first block.
            int end = Math.min(k < count ? region.slots : from, region.source != null ? region.slots : 1);
            for (int slot = start; slot < end && !region.released; slot++) {
                budget -= region.source != null ? purgeBlock(region.blocks[slot], now, budget) :
                        releaseIfIdle(region, now);
                if (budget <= 0) {
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

    /** Any thread, for a chunk whose heap died with buffers out: the purger owns it from now on (see
     *  {@link #releaseAbandoned}). */
    void abandon(SizeClassedChunk chunk) {
        SizeClassedChunk head;
        do {
            head = abandoned;
            chunk.nextAbandoned = head;
        } while (!ABANDONED.compareAndSet(this, head, chunk));
        armPurge(System.nanoTime());
    }

    /** Purger: chunks whose buffers are all back give their spans back; the rest wait for the pass this arms. */
    private void releaseAbandoned(long now) {
        SizeClassedChunk taken = abandoned == null ? null : ABANDONED.getAndSet(this, null);
        SizeClassedChunk kept = keepWaiting(waiting, null);
        kept = keepWaiting(taken, kept);
        waiting = kept;
        if (kept != null) {
            rearm(now, 0);
        }
    }

    /** Purger: releases what it can of the chain from {@code chunk}, and returns the rest pushed on {@code kept}. */
    private static SizeClassedChunk keepWaiting(SizeClassedChunk chunk, SizeClassedChunk kept) {
        while (chunk != null) {
            SizeClassedChunk next = chunk.nextAbandoned;
            if (chunk.returnSpanIfAllFree()) {
                chunk.nextAbandoned = null;
            } else {
                chunk.nextAbandoned = kept;
                kept = chunk;
            }
            chunk = next;
        }
        return kept;
    }

    /**
     * Shared slices: purges the free slices of {@code block} idle for {@link PageStoreConfig#purgeDelayNanos} or
     * more, one call per contiguous run, until the calls reach {@code budget} bytes, and returns their bytes. A
     * run's slices are claimed by CAS first and given back after, so a claim meanwhile finds every other free
     * slice; a claim never waits for the purger, since one that finds no fit maps a region instead.
     */
    private long purgeBlock(Segment block, long now, long budget) {
        long delay = config.purgeDelayNanos;
        long bytes = 0;
        long candidates = block.idleOf(block.free, now, delay);
        while (candidates != 0 && bytes < budget) {
            long run = lowestRun(candidates);
            candidates &= ~run;
            long claimed = block.claimFree(run);
            long exact = block.idleOf(claimed, now, delay);
            if (exact != claimed) {
                block.unclaim(claimed & ~exact);
            }
            while (exact != 0) {
                if (bytes >= budget) {
                    block.unclaim(exact);
                    break;
                }
                long bits = lowestRun(exact);
                exact &= ~bits;
                purgeSliceRun(block, bits);
                bytes += Long.bitCount(bits) * (long) block.sliceSize;
            }
        }
        return bytes;
    }

    /**
     * Gives back {@code region}, of a source that cannot purge part of a region, if all its slices stayed free for
     * the purge delay; its blocks are claimed whole by CAS first, so one no longer empty or idle goes back to use.
     */
    private long releaseIfIdle(Region region, long now) {
        long delay = config.purgeDelayNanos;
        if (!region.isEmpty()) {
            return 0;
        }
        long waited = region.shortestWait(now);
        if (waited < delay) {
            skip(waited);
            return 0;
        }
        int claimed = region.claimAll();
        if (claimed == region.slots && region.shortestWait(now) >= delay && releaseIdleRegion(region)) {
            return region.length;
        }
        region.unclaimBlocks(0, claimed);
        return 0;
    }

    /** {@code region}, which the purger holds whole, goes back to its source, unless the store was closed. */
    private boolean releaseIdleRegion(Region region) {
        lock.lock();
        try {
            if (closed) {
                return false;
            }
            releaseRegion(region);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Purges the contiguous slices {@code bits} of {@code block}, claimed by the purger, with one call. */
    private void purgeSliceRun(Segment block, long bits) {
        Region region = block.region;
        int sliceSize = block.sliceSize;
        int start = Long.numberOfTrailingZeros(bits);
        int n = Long.bitCount(bits);
        long address = block.address() + start * sliceSize;
        int length = n * sliceSize;
        boolean purged = false;
        try {
            AbstractPageStoreEvent event = PageStoreEvents.beginPurge();
            Throwable failure = null;
            try {
                region.source.purge(address, length);
                purged = true;
            } catch (Throwable t) {
                failure = t;
            }
            PageStoreEvents.end(event, address, length, region.index, failure);
            if (failure != null) {
                purgeFailed(failure);
                // Still purgeable: the next pass tries again.
                skip(config.purgeDelayNanos);
            }
        } finally {
            try {
                if (purged) {
                    block.uncommit(bits);
                    // Credited before a claim can find the slices uncommitted and charge them again.
                    PlatformDependent.decrementMemoryCounter(length);
                    allocator.memoryReleased(address, length, block.buffer.isDirect(), true);
                }
            } finally {
                block.unclaim(bits);
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

}
