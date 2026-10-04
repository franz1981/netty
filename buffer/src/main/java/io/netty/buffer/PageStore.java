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
 * Heaps own chunks, never blocks: any heap claims a run of slices from the regions' shared bitmaps by CAS, and
 * whichever thread frees the run gives it back by CAS ({@link #claimSlices}, {@link Segment#releaseRun}), as
 * mimalloc v3 claims a page's slices straight from its arena's bitmap
 * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L240-L246). A hole one heap leaves is reused by any.
 * Claims of one run length share blocks ({@link #binOf}), found through bitmaps over the blocks ({@link #maps}).
 * A buffer above a block takes a run of whole blocks ({@link #claimBlocks}). A new region is added under
 * this store's monitor, the only lock, when no block has a fit, and the claim that added it takes its run there
 * before any other thread sees the region.
 * <p>
 * The source is a detail of the memory, not of the ownership: {@code mmap} regions (256 MiB, {@link #mmap}) purge
 * the free slices idle for the purge delay in place, and charge and count a slice from the claim that finds no
 * memory behind it to its purge; regions of one block ({@link #memory}: a {@code malloc}'d 4 MiB, or a {@code byte[]}
 * of 4032 KiB for heap memory) are charged and counted whole by their allocation, and go back whole once all of them
 * stayed free for the delay. Once an {@code mmap} region cannot be mapped, new regions are one {@code malloc}'d
 * block each, next to the regions mapped so far. One purger at a time, driven by the heaps' ticks: see
 * {@link #purgeIfDue}.
 */
final class PageStore {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStore.class);
    private static final AtomicIntegerFieldUpdater<PageStore> PURGING =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "purging");
    private static final AtomicLongFieldUpdater<PageStore> SLICES_COMMITTED =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "slicesCommitted");
    private static final AtomicIntegerFieldUpdater<PageStore> ARMED =
            AtomicIntegerFieldUpdater.newUpdater(PageStore.class, "armed");
    private static final AtomicReferenceFieldUpdater<PageStore, SizeClassedChunk> ABANDONED =
            AtomicReferenceFieldUpdater.newUpdater(PageStore.class, SizeClassedChunk.class, "abandoned");
    private static final Region[] NO_REGIONS = new Region[0];
    /**
     * The bytes a purge pass gives back, {@code madvise}d or released whole, past which it makes no more calls (see
     * {@link #purge}): bytes, not calls, as a call costs about as much as the memory it gives back. Measured with
     * {@code mmap}, 1 GiB freed at once: passes of 8.6 ms (p50) and 10 ms (p90), 1 GiB back in 15.5 s; 32 MiB: 4.5
     * and 5.5 ms, 31 s; 128 MiB: 17 and 19.5 ms, 7 to 8 s.
     */
    static final long PURGE_BYTES = 64L << 20;

    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    /**
     * Per run length in slices, its bin: claims of one length share blocks (see {@link Segment#bin}), so that the short
     * runs do not cut the holes the long ones leave. A page kind's bin is its index in
     * {@link SizeClassTable#pageKinds}; every other length is in {@link #otherBin}. As mimalloc v3's
     * size bins of its bitmap chunks ({@code mi_chunkbin_of},
     * https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.h#L249-L257), by exact length.
     */
    final byte[] binOf;
    final int otherBin;
    /**
     * Per bin, the run length whose fit its bit in {@link #maps} stands for: the page kind; 1 for
     * {@link #otherBin}, whose claims have many lengths.
     */
    final int[] binSlices;
    /** The map of the empty blocks in {@link #maps}, after one per bin. */
    final int emptyMap;
    /** A block's id is its region's index shifted by this, or its slot: a region's blocks have consecutive ids. */
    private final int idShift;
    /**
     * Bitmaps over the block ids, {@code emptyMap + 1} of them, each {@code maps.length() / (emptyMap + 1)} words,
     * one after the other: per bin, a bit set when a block of that bin has a free run of the bin's length; and the
     * empty blocks, for any bin. Hints: the truth is each block's {@link Segment#free}. A release sets the bit
     * its block now has ({@link #slicesReleased}); a claim that finds a block without the fit its bit stood for
     * clears it, then reads the block again and sets it again if a fit came back meanwhile ({@link #unmark}). As
     * mimalloc v3's {@code chunkmap} and {@code chunkmap_bins} over its bitmap chunks
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.h#L261-L270). Replaced by a larger copy under
     * this store's monitor ({@link #growMaps}).
     */
    private volatile AtomicLongArray maps;
    final MemorySource memory;
    /** Where new regions are mapped, or {@code null}: one block from {@link #memory} instead. */
    volatile MmapRegionSource mmap;
    /** The blocks of a new region, and where it starts: 1 and 0 once {@link #mmap} fails and is given up. */
    private volatile int regionBlocks;
    private int regionAlignment;
    /** Replaced under this store's monitor, one longer or with a released region's place taken; read without it. */
    volatile Region[] regions = NO_REGIONS;
    private boolean closed;
    /** 1 while a thread purges: one purger at a time. */
    private volatile int purging;
    /** Set by tests. */
    volatile long lastPurgeNanos = System.nanoTime();
    // Read by every run release, written once per arming by a release and by the purger. HotSpot packs this object's
    // fields into about 150 bytes, so these share lines with regions, slicesCommitted and the purger's counters.
    /** 1 once a run was released since the purger last disarmed: see {@link #armPurge}. */
    private volatile int armed;
    /** When {@link #armed} was set, as {@link System#nanoTime()}: a pass waits the purge delay from then. */
    private volatile long armedAt;
    // Read by tests and dumps.
    /** Shared slices claimed while no memory backed them. */
    volatile long slicesCommitted;
    /** Chunks abandoned since the last pass, a stack linked through {@code nextInQueue}: see {@link #abandon}. */
    private volatile SizeClassedChunk abandoned;
    /** Purger only: abandoned chunks with buffers still out, linked through {@code nextInQueue}. */
    private SizeClassedChunk waiting;
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

    /**
     * Maps a new region, unless one was added since {@code seen} was read: three system calls for {@code mmap}, one
     * allocation for a region of one block. Claims the caller's run in it before publishing it: {@code blocks} whole
     * blocks, or if 0, {@code slices} slices of its first block. Published empty, a region could be given back
     * by the purger, idle, before the claim that mapped it scanned it, and the claim map another: with a short purge
     * delay, without end. Returns the run as {@link #claimSlices} and {@link #claimBlocks} encode theirs, not
     * committed, or -1 for the caller to scan again: a region was added meanwhile, the run does not fit a new
     * region, or a region cannot be had and this gave up {@link #mmap} for good.
     */
    private synchronized long addRegion(Region[] seen, boolean threadLocal, int slices, int blocks) {
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
        int index = 0;
        while (index < seen.length && !seen[index].released) {
            index++;
        }
        // A region charged whole has memory behind all of it, free since now.
        Region region = new Region(this, buffer, mmap, slots, config, mmap == null, System.nanoTime(), index);
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
        for (Segment block : region.blocks) {
            slicesReleased(block, block.free);
        }
        if (region.source == null) {
            // Its free slices have memory behind them, all of them if the claim failed.
            armPurge(System.nanoTime());
        }
        return first < 0 ? -1 : run(index, first);
    }

    /**
     * Shared slices, any thread: claims a run of {@code slices} free slices of one block, at most a block: the lowest
     * fit (first fit, as {@code mi_bchunk_try_find_and_clearNX},
     * https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L793-L849) in the lowest block of its bin that
     * has one, else the lowest empty block, which takes its bin, else a new region (see {@link #addRegion}).
     * Lowest first, so that the highest blocks drain and go back. As mimalloc v3's
     * {@code mi_bbitmap_try_find_and_clear_generic}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L1801-L1884), minus its other bins and its
     * start at the thread's sequence. Returns the region's index in the high half and the
     * run's first slice in the region in the low half. The run's slices are committed (see {@link #commitSlices});
     * give it back with {@link Segment#releaseRun}, from any thread.
     */
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

    /**
     * After a claim of {@code bin} in {@code block} that returned {@code claimed} (see {@link Segment#claimRun}): the
     * maps show the block as it now is to the claims of its bin, and as not empty if it was. Returns the run as
     * {@link #claimSlices} does.
     */
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

    /**
     * Any thread, after slices of {@code block} were freed, leaving {@code free}: sets the bit the block now has, if
     * not set, empty or a fit for its bin.
     */
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

    /** The block of {@code id} in {@code regions}, or {@code null} when it has none. */
    private Segment blockOf(Region[] regions, int id) {
        int index = id >>> idShift;
        if (index >= regions.length) {
            return null;
        }
        Region region = regions[index];
        int slot = id & (1 << idShift) - 1;
        return slot < region.slots ? region.blocks[slot] : null;
    }

    /**
     * Sets bit {@code id} of {@code map}, unless set, in the maps of now: again in the copy that replaced them
     * meanwhile, if any (see {@link #growMaps}).
     */
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

    /**
     * Clears bit {@code id} of {@code map}, which a claim found the block does not have, then reads the block again and
     * sets it again if it does: a release meanwhile may have found the bit still set. As mimalloc v3's
     * {@code mi_bbitmap_chunkmap_try_clear}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L1679-L1693): "a concurrent set may have
     * happened in between ... We check again".
     */
    private void unmark(int map, int id) {
        if (clear(map, id)) {
            Segment block = blockOf(regions, id);
            if (block != null && hasBitIn(map, block)) {
                mark(map, id);
            }
        }
    }

    /**
     * Under this store's monitor, before region {@code index} is published: the maps hold its blocks' ids, doubled if
     * they did not. A mark on the old maps after the copy read its word, by a thread that read {@link #maps} before
     * the new ones were published, is copied by the second pass; a later one sees the new maps and marks them too.
     */
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

    /** Words per map of {@code maps}: its length split {@link #emptyMap} + 1 ways. */
    private int wordsPerMap(AtomicLongArray maps) {
        return maps.length() / (emptyMap + 1);
    }

    /** The index into {@code maps} of {@code map}'s word holding bit {@code id}. */
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

    /**
     * Any thread: claims {@code blocks} contiguous empty blocks of one region, for a buffer larger than a block
     * (see {@link Region#claimBlocks}), else in a new region (see {@link #addRegion}). Returns the region's index in
     * {@link #regions} in the high half and the first block in the low half, or -1 when new regions hold fewer blocks.
     * Every slice is committed; give them back with {@link #releaseBlocks}.
     */
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

    /**
     * The claimer's run of {@code n} slices of {@code block} from {@code start}: its slices with no memory behind them
     * are charged to the direct memory limit and counted in the used memory, all at once. On failure the run goes back,
     * any slice charged meanwhile staying charged and free, until it is purged.
     */
    private void commitSlices(Segment block, int start, int n, boolean threadLocal) {
        int sliceSize = block.sliceSize;
        long fresh = block.fresh(block.bits(start, n));
        int freshCount = Long.bitCount(fresh);
        boolean committed = false;
        try {
            if (freshCount != 0) {
                PlatformDependent.incrementMemoryCounter(freshCount * sliceSize);
                block.commit(fresh);
                SLICES_COMMITTED.addAndGet(this, freshCount);
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

    /** The region of a run {@link #claimSlices} or {@link #claimBlocks} returned. */
    Region region(long run) {
        return regions[(int) (run >>> 32)];
    }

    /** The block of a run {@link #claimSlices} returned. */
    Segment block(long run) {
        return region(run).blocks[firstBlock(run)];
    }

    /** The first slice in its block of a run {@link #claimSlices} returned. */
    int start(long run) {
        return (int) run % config.slicesPerSegment();
    }

    /** The first block of a run {@link #claimSlices} or {@link #claimBlocks} returned. */
    int firstBlock(long run) {
        return (int) run / config.slicesPerSegment();
    }

    /** Any thread: the blocks {@link #claimBlocks} returned go back to the shared slices. */
    void releaseBlocks(Region region, int start, int slots) {
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
            releaseRegion(region);
        }
    }

    /**
     * Credits {@code region}'s committed slices back, and its buffer goes to {@link Region#release()}; one JFR
     * event. Shared by {@link #close()} and the purger's {@link #releaseIdleRegion}, both under this store's
     * monitor.
     */
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

    /** Credits every committed slice of {@code region}: the close only. */
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
     * Any thread, from a heap's purge tick (see {@code Heap#countAllocations}) or a one-shot buffer. At most once per
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
    void skip(long waited) {
        if (!skipped || waited > longestWait) {
            skipped = true;
            longestWait = waited;
        }
    }

    /**
     * One pass: the blocks of every region in turn, from where the last pass stopped and around to it, until its calls
     * ({@code madvise} or region releases) gave back {@link #PURGE_BYTES}; the call that reaches it is made whole, so
     * a pass makes one call at least. One that stops short arms the purge again, due at once, and the next pass, after
     * the cadence floor, goes on from where this one stopped. mimalloc v3 bounds a pass by arenas purged instead, a
     * quarter of them plus one, from an arena chosen by the thread's sequence
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/arena.c#L2433-L2450); one region of ours can need
     * thousands of calls.
     */
    private void purge(long now) {
        purges++;
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

    /**
     * Any thread, for a chunk whose heap died with buffers out: the purger owns it from now on, and gives its span back
     * at the first pass that finds every buffer back (see {@link #releaseAbandoned}).
     */
    void abandon(SizeClassedChunk chunk) {
        SizeClassedChunk head;
        do {
            head = abandoned;
            chunk.nextAbandoned = head;
        } while (!ABANDONED.compareAndSet(this, head, chunk));
        armPurge(System.nanoTime());
    }

    /**
     * Purger: the abandoned chunks whose buffers are all back give their spans back; the others wait for a later
     * pass, which this arms, a delay from now.
     */
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

    // Visible for testing: racy, the chunks abandoned and not given back yet.
    int abandonedCount() {
        int count = 0;
        for (SizeClassedChunk c = abandoned; c != null; c = c.nextAbandoned) {
            count++;
        }
        for (SizeClassedChunk c = waiting; c != null; c = c.nextAbandoned) {
            count++;
        }
        return count;
    }

    /**
     * Shared slices: purges the free slices of {@code block} with memory behind them freed
     * {@link PageStoreConfig#purgeDelayNanos} ago or earlier, one call per run of contiguous ones, until the calls
     * reached {@code budget} bytes, and returns their bytes. As mimalloc v3's {@code mi_arena_try_purge_range}
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
     * Gives back {@code region}, of a source that cannot purge part of a region, if all its slices stayed free for the
     * purge delay, and returns the bytes given back: its length if it did. Its blocks are claimed whole by CAS first,
     * as a purge claims its run, so that no claim can take a slice of it meanwhile; a region that is no longer empty
     * and idle once claimed goes back to use. A released region keeps its blocks claimed, so that a claim that
     * still sees it finds nothing there, and its place in {@link #regions} is taken by the next region mapped.
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
    private synchronized boolean releaseIdleRegion(Region region) {
        if (closed) {
            return false;
        }
        releaseRegion(region);
        regionsReleased++;
        return true;
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
                    purgeCalls++;
                    bytesPurged += length;
                    slicesPurged += n;
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
            for (Segment block : region.blocks) {
                block.addSliceCounts(counts);
            }
        }
        return counts;
    }

    int regionCount() {
        return regions.length;
    }

}
