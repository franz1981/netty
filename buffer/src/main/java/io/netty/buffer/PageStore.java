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
 * The memory behind every heap of one {@link AdaptivePoolingAllocator}, shared by all of them; the design follows
 * mimalloc v3's arenas. The units, largest first, and their names in the code:
 * <pre>
 * region  {@link Region}             memory mapped or allocated whole, cut into blocks
 *  block  {@link Segment}            up to 64 slices, one bit per slice in {@link Segment#free}
 *   slice                            what is claimed and freed; {@link PageStoreConfig#sliceSize} bytes
 *    span                            consecutive slices of one block, claimed and freed together: a chunk's, or
 *                                    one buffer's above the size classes (see
 *                                    {@link AdaptivePoolingAllocator.SharedSpanChunk SharedSpanChunk})
 *   chunk  {@link SizeClassedChunk}  a span cut into slots of one size class
 *    slot                            one buffer's place in a chunk
 * heap    {@link AdaptivePoolingAllocator.Heap}  a thread-local heap, or a stripe under a lock: the chunks and
 *                                    spans one thread, or one lock holder, allocates from
 * </pre>
 * No heap owns a block. A heap takes a span with one CAS on the block's bitmap ({@link #claimSlices}) and gives it
 * back with one ({@link Segment#releaseRun}), from any thread: the slices of a chunk one heap gave back are a
 * chunk of any heap's at once, so a heap that stops using a size class keeps no memory from the others. A block
 * holds spans of one length only, its bin, found through {@link #maps}: see {@link #binOf}.
 * <p>
 * Idle memory goes back to the OS from the allocation paths, with no thread of its own: a freed span arms the
 * purge ({@link #armPurge}); once {@link PageStoreConfig#purgeDelayNanos} passed, the next allocation that looks
 * runs one pass, as the only purger ({@link #purgeIfDue}). An {@code mmap} region ({@link #mmap}) has its free
 * slices purged in place ({@link #purgeBlock}); a region of one block from {@link #memory} is given back whole
 * once all its slices are free ({@link #releaseIfIdle}).
 * <p>
 * {@link #lock} is taken to add a region, to give one back whole and to {@link #close}; a claim or a release
 * never takes it, nor any lock.
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
    /**
     * The bytes one purge pass gives back before it stops, so that no allocation path runs a whole pass at once;
     * the next pass resumes where this one stopped ({@link #nextRegion}).
     */
    static final long PURGE_BYTES = 64L << 20;

    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    /**
     * Per span length in slices, its bin. A block holds spans of one length only: the length of the first span
     * taken from it while empty ({@link Segment#bin}). A freed span then leaves a gap that the next span of that
     * length fills exactly. If lengths were mixed, short spans would take parts of the long gaps, and a long span
     * would find no gap wide enough and need a new region. The lengths with a bin of their own are the page kinds
     * ({@link SizeClassTable#pageKinds}), the chunks' lengths; every other length, a large buffer's, shares
     * {@link #otherBin}.
     */
    final byte[] binOf;
    final int otherBin;
    /**
     * Per bin, the span length its bit in {@link #maps} promises room for: 1 for {@link #otherBin}, whose spans
     * are of any length.
     */
    final int[] binSlices;
    /** The index of the empty blocks' bitmap in {@link #maps}, after the bins' bitmaps. */
    final int emptyMap;
    /** A block's id, its bit in each of {@link #maps}, is its region's index shifted by this, or'ed with its slot. */
    private final int idShift;
    /**
     * Per bin, a bitmap over the block ids: a bit set when a block of that bin may have room for a span of the
     * bin's length ({@link #binSlices}); after them, a bitmap of the empty blocks, open to any bin. Hints only: the
     * truth is each block's {@link Segment#free}, so a claim that finds a bit wrong clears it ({@link #unmark}).
     * Replaced by a longer copy under {@link #lock} as regions are added ({@link #growMaps}); read without it.
     */
    private volatile AtomicLongArray maps;
    /** Where regions of one block come from, while {@link #mmap} is {@code null}. */
    final MemorySource memory;
    /**
     * Where regions are mapped; {@code null} when mapping is not available, or once it failed and was given up:
     * regions are then one block each, from {@link #memory}.
     */
    volatile MmapRegionSource mmap;
    /** The blocks of the next region, and its alignment: 1 and 0 once {@link #mmap} failed and was given up. */
    private volatile int regionBlocks;
    private int regionAlignment;
    /**
     * Replaced under {@link #lock}: one longer, or with a given-back region's place taken by the new one. Read
     * without it by every claim.
     */
    volatile Region[] regions = NO_REGIONS;
    /**
     * Taken only to add a region ({@link #addRegion}), to give one back whole ({@link #releaseIdleRegion}) and to
     * {@link #close}: a claim or a release never takes it. Claims that found no room wait on it rather than each
     * map a region of their own: the first maps one, the others find {@link #regions} changed and claim in it.
     */
    private final ReentrantLock lock = new ReentrantLock();
    /** Under {@link #lock}. */
    private boolean closed;
    /** 1 while a thread is the purger: see {@link #purgeIfDue}. */
    private volatile int purging;
    /** When the last pass started: written by the purger, read by every {@link #isDue}. */
    volatile long lastPurgeNanos = System.nanoTime();
    /**
     * 1 once a span was freed since the purger last disarmed: read by every release, written only by the one
     * that finds it 0 ({@link #armPurge}), so a release costs one read.
     */
    private volatile int armed;
    /** When {@link #armed} was set: the purge delay counts from here. */
    private volatile long armedAt;
    /** Chunks abandoned since the last pass, a stack linked through {@code nextAbandoned}: see {@link #abandon}. */
    private volatile SizeClassedChunk abandoned;
    /** Purger only: abandoned chunks with buffers still out, linked through {@code nextAbandoned}. */
    private SizeClassedChunk waiting;
    private long purgeFailures;
    /**
     * Purger only: whether this pass found free slices, or a region, short of the purge delay, and the longest any
     * of them had waited, so the pass can arm the next one for when they are due ({@link #skip}).
     */
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
     * Under {@link #lock}: adds a region, unless one was added since {@code seen} was read (then -1: the caller
     * claims in that one), and claims the caller's span, or {@code blocks} whole blocks, in it before publishing
     * it. Published empty, the purger could give it back before the caller's claim found it.
     */
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
                    // A region of one block (malloc, byte[]) that cannot be had, or the direct memory limit it is
                    // charged to: this allocation fails, a later region may fit.
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

    /**
     * Under {@link #lock}: makes {@code buffer} a region, claims the caller's span or blocks in it, and publishes
     * it in {@link #regions} and {@link #maps}; gives {@code buffer} back if any step throws, as nothing else could.
     */
    private long publish(Region[] seen, boolean threadLocal, int slices, int blocks, AbstractByteBuf buffer,
                         MmapRegionSource mmap, int slots, int size) {
        int index = 0;
        while (index < seen.length && !seen[index].released) {
            index++;
        }
        Region region = null;
        boolean published = false;
        try {
            // A region of one block has memory behind all of it from its allocation: committed, and free since now.
            region = new Region(this, buffer, mmap, slots, config, mmap == null, System.nanoTime(), index);
            if (region.source == null) {
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
                // Its free slices have memory behind them: the purger may give the region back once they are idle.
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

    /**
     * Any thread, no lock: claims a span of {@code slices} consecutive free slices of one block, in the lowest
     * block with room, so that the highest blocks drain and their regions go back; maps a region when none has
     * room. Returns where the span starts ({@link #run}); give it back with {@link Segment#releaseRun}, from any
     * thread.
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

    /** A span of {@code n} slices in the lowest block of {@code bin} whose bit in {@link #maps} is set, or -1. */
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
                    // A region published in the maps but not yet in the regions this claim read.
                    continue;
                }
                if (block.bin == bin) {
                    int claimed = block.claimRun(n, bin);
                    if (claimed >= 0) {
                        return claimed(block, id, bin, claimed);
                    }
                    if (length != n && block.hasFit(length)) {
                        // The other bin: no room for this span, but the bit promises only room for a shorter one.
                        continue;
                    }
                }
                unmark(bin, id);
            }
        }
        return -1;
    }

    /** The first {@code n} slices of the lowest empty block {@link #maps} shows, which becomes {@code bin}'s, or -1. */
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
     * After a claim in {@code block}: clears its empty bit if this claim found it empty, and sets or clears its
     * bin's bit by the room left. Returns where the span starts ({@link #run}).
     */
    private long claimed(Segment block, int id, int bin, int claimed) {
        if ((claimed & Segment.FIRST) != 0) {
            // This claim holds a span of the block: it cannot be empty again before the bit is clear.
            clear(emptyMap, id);
        }
        if (block.hasFit(binSlices[bin])) {
            mark(bin, id);
        } else {
            unmark(bin, id);
        }
        return run(block.region.index, block.slot * block.slices + (claimed & Segment.START));
    }

    /** Any thread, after it freed slices of {@code block}, leaving {@code free}: sets the bit the block now earns. */
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

    /** Whether {@code block} earns its bit in {@code map} right now. Racy. */
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

    /** Sets bit {@code id} of {@code map}; again in the longer copy if {@link #growMaps} replaced it meanwhile. */
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

    /**
     * Clears bit {@code id} of {@code map}, and returns whether it was set. Not retried in a copy that replaced
     * the array meanwhile: a clear lost that way leaves a stale bit, which the next claim corrects.
     */
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

    /** Clears bit {@code id} of {@code map}, found wrong by a claim; sets it again if a release meanwhile earned it. */
    private void unmark(int map, int id) {
        if (clear(map, id)) {
            Segment block = blockOf(regions, id);
            if (block != null && hasBitIn(map, block)) {
                mark(map, id);
            }
        }
    }

    /**
     * Under {@link #lock}: replaces {@link #maps} by a longer copy with room for region {@code index}'s block ids,
     * before the region is published. Copied once before and once after the switch: a {@link #mark} that landed in
     * the old array during the first copy is carried over by the second.
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

    /** Whether the bit of {@code block} is set in {@code map}. Racy; for the tests. */
    boolean marked(int map, Segment block) {
        AtomicLongArray maps = this.maps;
        int id = id(block);
        return (maps.get(word(maps, map, id)) & 1L << id) != 0;
    }

    /**
     * Any thread, no lock: for one buffer above a block, claims {@code blocks} consecutive empty blocks of one
     * region, mapping a region when none has them; -1 when a region cannot hold that many. Every slice of them is
     * committed; give them back with {@link #releaseBlocks}, from any thread.
     */
    long claimBlocks(int blocks) {
        for (;;) {
            if (blocks > regionBlocks) {
                // No region can hold them: regions are one block each now, or too small.
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

    /** Commits the {@code blocks} blocks the caller claimed from {@code first}; gives all of them back on a throw. */
    private void commitBlocks(Region region, int first, int blocks) {
        int committed = 0;
        try {
            for (; committed < blocks; committed++) {
                Segment block = region.blocks[first + committed];
                commitSlices(block, 0, block.slices, false);
            }
        } finally {
            if (committed < blocks) {
                // commitSlices gave back the block it failed on: the ones before it go back here, the ones after it
                // were never committed.
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
     * For the span the caller just claimed, {@code n} slices of {@code block} from {@code start}: the slices with
     * no memory behind them (never used, or purged) are charged to the direct memory limit and counted, all at
     * once. On a throw the span goes back; slices charged by then stay charged, the purger credits them.
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

    /**
     * Where a claim starts, as {@link #claimSlices} and {@link #claimBlocks} return it: the region's index in
     * {@link #regions} and the first slice's index in the region, packed as {@code region << 32 | slice}; read
     * back with {@link #region}, {@link #block}, {@link #start} and {@link #firstBlock}.
     */
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

    /** Any thread: the {@code slots} blocks from {@code start}, claimed by {@link #claimBlocks}, are free again. */
    void releaseBlocks(Region region, int start, int slots) {
        long now = System.nanoTime();
        for (int slot = start; slot < start + slots; slot++) {
            Segment block = region.blocks[slot];
            block.releaseRun(0, block.slices, now);
        }
    }

    /**
     * Under {@link #lock}: gives every region back, and lets no region be added after. Only once nothing can touch
     * them any more (the allocator is unreachable, or every buffer is back).
     */
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

    /**
     * Under {@link #lock}, holding every block of {@code region}: credits what is charged for it (its committed
     * slices, or the whole region), then unmaps or frees it ({@link Region#release()}).
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

    /** Any thread, after it freed slices at {@code now}: arms the purge, unless armed, so only one release writes. */
    void armPurge(long now) {
        if (armed == 0 && ARMED.compareAndSet(this, 0, 1)) {
            armedAt = now;
        }
    }

    /**
     * Any thread, from the allocation paths: runs one purge pass if one is due, as the only purger (the CAS on
     * {@link #purging}; a thread that loses it goes on allocating). Due: armed {@link PageStoreConfig#purgeDelayNanos}
     * ago or more, and {@link PageStoreConfig#purgeCheckNanos} since the last pass. Disarms before the pass, so a
     * release during it arms the next one.
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

    /** Purger: arms the next pass as if armed {@code waited} before {@code now}, unless armed to be due sooner. */
    private void rearm(long now, long waited) {
        if (armed == 0 || now - armedAt < waited) {
            armedAt = now - waited;
            armed = 1;
        }
    }

    /**
     * Purger: free slices, or a region, free for {@code waited}, short of the delay, were left for a later pass;
     * the pass arms the next one for when the longest-waiting of them is due ({@link #longestWait}).
     */
    void skip(long waited) {
        if (!skipped || waited > longestWait) {
            skipped = true;
            longestWait = waited;
        }
    }

    /**
     * One pass: first the abandoned chunks, then every block of every region in turn, from where the last pass
     * stopped, until the pass gave back {@link #PURGE_BYTES} (the call that reaches it is made whole). A pass that
     * stops short, or skipped slices, arms the next one.
     */
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
            // A region of one block is given back whole or not at all: visited once, as its first block.
            int end = Math.min(k < count ? region.slots : from, region.source != null ? region.slots : 1);
            for (int slot = start; slot < end && !region.released; slot++) {
                budget -= region.source != null ? purgeBlock(region.blocks[slot], now, budget) :
                        releaseIfIdle(region, now);
                if (budget <= 0) {
                    // This block may have more: the next pass starts on it.
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
     * Any thread, for a chunk whose heap closed while buffers of it were still out: pushes it on
     * {@link #abandoned} by CAS and arms the purge; the purger owns the chunk from now on ({@link #releaseAbandoned}).
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
     * Purger: takes the abandoned chunks whole; those whose buffers are all back give their spans back, the rest
     * go on {@link #waiting} for the next pass, which this arms.
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

    /** Purger: gives back the spans it can of the chain from {@code chunk}; returns the rest pushed on {@code kept}. */
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
     * Purger: purges the free slices of {@code block} that stayed free for {@link PageStoreConfig#purgeDelayNanos}
     * or more, one {@link MmapRegionSource#purge} call per group of consecutive slices, until the calls reach
     * {@code budget} bytes; returns the bytes purged. The slices are claimed by CAS before the call and freed after
     * it, so a claim meanwhile takes other free slices, or maps a region, and never waits for the purger.
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
     * Purger, for a region of one block, which cannot be purged in part: gives it back whole if every slice stayed
     * free for the purge delay; returns the bytes given back. Its blocks are claimed whole by CAS first, so a claim
     * meanwhile either took its span before, and the region stays, or finds the region gone and claims elsewhere.
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

    /** Purger, holding every block of {@code region}: gives it back under {@link #lock}, unless the store closed. */
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

    /**
     * Purger, holding the consecutive slices {@code bits} of {@code block}: purges them with one call, credits
     * them, and frees them. Credited before they are freed, so a claim cannot find them uncommitted and charge
     * them again first.
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
                // The slices stay committed and free: the next pass tries again.
                skip(config.purgeDelayNanos);
            }
        } finally {
            try {
                if (purged) {
                    block.uncommit(bits);
                    PlatformDependent.decrementMemoryCounter(length);
                    allocator.memoryReleased(address, length, block.buffer.isDirect(), true);
                }
            } finally {
                block.unclaim(bits);
            }
        }
    }

    /** The lowest group of consecutive set bits of {@code bits}, which is not 0: one purge call's slices. */
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
