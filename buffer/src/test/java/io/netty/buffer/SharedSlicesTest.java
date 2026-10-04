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

import io.netty.buffer.PageStoreTestSupport.CountingRegionSource;
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertSharedAccounted;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static io.netty.buffer.PageStoreTestSupport.purgeUntilDone;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The page store's shared slices (see {@link PageStore#claimSlices}): first fit within a block, never across one,
 * blocks shared by claims of one length, releases from any thread reused at once, whole-block runs, and a purge that
 * only takes free idle slices, coalesced across blocks. Then all of it at once from many threads.
 */
final class SharedSlicesTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;
    private static final int PER_BLOCK = SEGMENT_SIZE / SLICE;
    private static final int PER_REGION = REGION_SIZE / SLICE;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private final CountingMemorySource segments = new CountingMemorySource();
    private final CountingRegionSource regions = new CountingRegionSource();

    private PageStore store(long purgeDelayNanos) {
        assumeTrue(MmapRegionSource.isAvailable(), "shared slice tests need mmap");
        return closer.add(newSharedAllocator(segments, regions, REGION_SIZE, purgeDelayNanos)).pageStore;
    }

    /** The run's first slice in its region. */
    private static int slice(long run) {
        assertTrue(run >= 0, "no run");
        return (int) run;
    }

    private static Segment block(PageStore store, long run) {
        return store.regions[(int) (run >>> 32)].blocks[slice(run) / PER_BLOCK];
    }

    private static void release(PageStore store, long run, int n) {
        block(store, run).releaseRun(slice(run) % PER_BLOCK, n, System.nanoTime());
    }

    @Test
    void firstFitWithinABlockNeverAcrossOne() {
        PageStore store = store(INTERVAL);
        assertEquals(0, slice(store.claimSlices(40, false)));
        assertEquals(PER_BLOCK, slice(store.claimSlices(40, false)), "24 left in block 0: block 1");
        assertEquals(40, slice(store.claimSlices(24, false)), "the tail of block 0 fits");
        assertEquals(PER_BLOCK + 40, slice(store.claimSlices(2, false)));
        assertEquals(1, regions.regions.size());
        assertEquals(0, segments.segmentsAllocated(), "no segment of its own");
    }

    /**
     * Claims of one length share blocks: the lowest block of their bin with a fit, else the lowest wholly free block,
     * which takes their bin; a block wholly free again is any bin's.
     */
    @Test
    void claimsOfOneLengthShareBlocks() {
        PageStore store = store(INTERVAL);
        assertBins(store, 1, 8, 32);
        long a = store.claimSlices(1, false);
        assertEquals(0, slice(a));
        long b = store.claimSlices(8, false);
        assertEquals(PER_BLOCK, slice(b), "block 0 is the 1-slice bin's");
        assertEquals(1, slice(store.claimSlices(1, false)));
        long d = store.claimSlices(8, false);
        assertEquals(PER_BLOCK + 8, slice(d));
        release(store, b, 8);
        release(store, d, 8);
        assertEquals(PER_BLOCK, slice(store.claimSlices(32, false)), "the lowest wholly free block");
        assertEquals(2 * PER_BLOCK, slice(store.claimSlices(8, false)), "block 1 is the 32-slice bin's now");
        release(store, a, 1);
        assertEquals(0, slice(store.claimSlices(1, false)), "the lowest fit of the bin");
        assertMapsShowEveryFit(store);
    }

    /** A bit set for a block that does not earn it is cleared by the claim that finds it, which goes on. */
    @Test
    void aStaleBitIsClearedByTheClaimThatFindsIt() {
        PageStore store = store(INTERVAL);
        assertBins(store, 1, 8, 32);
        Segment eights = block(store, store.claimSlices(8, false));
        Segment ones = block(store, store.claimSlices(1, false));
        assertEquals(1, store.id(ones), "block 1 of region 0");
        int bin1 = store.binOf[1];
        store.mark(bin1, store.id(eights));
        assertEquals(PER_BLOCK + 1, slice(store.claimSlices(1, false)), "not in the 8-slice bin's block");
        assertFalse(store.marked(bin1, eights));
        store.mark(store.emptyMap, store.id(eights));
        assertEquals(2 * PER_BLOCK, slice(store.claimSlices(32, false)), "block 0 is not wholly free");
        assertFalse(store.marked(store.emptyMap, eights));
        assertMapsShowEveryFit(store);
    }

    /**
     * Threads claim runs of every bin, and release some, while the claims add regions and the maps grow under them:
     * no slice is claimed twice, and once the threads are done the maps show every fit and every wholly free block.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void theMapsGrowWhileThreadsClaimAndRelease() throws Exception {
        final PageStore store = store(INTERVAL);
        // 64 ids a map word: a region of 9 blocks takes 16, so this many regions grow the maps three times.
        final int regionsWanted = 17;
        final int[] sizes = {1, 2, 3, 5, 8, 9, 32, 63};
        final AtomicIntegerArray owners = new AtomicIntegerArray(64 * PER_REGION);
        final AtomicReference<String> failure = new AtomicReference<String>();
        final CountDownLatch start = new CountDownLatch(1);
        final List<List<long[]>> held = new ArrayList<List<long[]>>();
        List<Thread> threads = new ArrayList<Thread>();
        for (int t = 0; t < 8; t++) {
            final int id = t + 1;
            final List<long[]> mine = new ArrayList<long[]>();
            held.add(mine);
            threads.add(new Thread(() -> {
                SplittableRandom random = new SplittableRandom(id);
                try {
                    start.await();
                    while (store.regionCount() < regionsWanted && failure.get() == null) {
                        if (!mine.isEmpty() && random.nextInt(4) == 0) {
                            long[] run = mine.remove(random.nextInt(mine.size()));
                            own(owners, run, id, 0, failure);
                            release(store, run[0], (int) run[1]);
                        } else {
                            int n = sizes[random.nextInt(sizes.length)];
                            long[] run = {store.claimSlices(n, false), n};
                            own(owners, run, 0, id, failure);
                            mine.add(run);
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, "thread " + id + ": " + e);
                }
            }));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertNull(failure.get());
        assertTrue(store.regionCount() >= regionsWanted);
        assertMapsShowEveryFit(store);
        for (List<long[]> mine : held) {
            for (long[] run : mine) {
                release(store, run[0], (int) run[1]);
            }
        }
        assertEquals(0, store.sliceCounts()[0]);
        assertMapsShowEveryFit(store);
    }

    /** The slices of {@code run}, {run, slices}, go from owner {@code from} to {@code to}. */
    private static void own(AtomicIntegerArray owners, long[] run, int from, int to,
                            AtomicReference<String> failure) {
        int base = (int) (run[0] >>> 32) * PER_REGION + slice(run[0]);
        for (int s = base; s < base + run[1]; s++) {
            int owner = owners.getAndSet(s, to);
            if (owner != from) {
                failure.compareAndSet(null, "slice " + s + " held by " + owner + ", not " + from);
            }
        }
    }

    private static void assertBins(PageStore store, int... lengths) {
        for (int n : lengths) {
            assertTrue(store.binOf[n] != store.otherBin, n + " slices: not a page kind");
        }
    }

    /**
     * Once no thread claims nor releases: every wholly free block, and every block with a fit for its bin, has its
     * bit; a bit too many is only a hint.
     */
    static void assertMapsShowEveryFit(PageStore store) {
        for (Region region : store.regions) {
            if (region.released) {
                continue;
            }
            for (Segment block : region.blocks) {
                long free = block.free;
                if (free == block.allFree) {
                    assertTrue(store.marked(store.emptyMap, block), block + " of " + region + ": wholly free");
                } else if (Segment.firstFit(free, store.binSlices[block.bin]) >= 0) {
                    assertTrue(store.marked(block.bin, block), block + " of " + region + ": a fit of bin "
                            + block.bin);
                }
            }
        }
    }

    /** A run released by another thread is free for the next claim at once, and a second release throws. */
    @Test
    void aReleaseFromAnyThreadIsReusedAtOnce() throws Exception {
        final PageStore store = store(INTERVAL);
        final AtomicLong claimed = new AtomicLong(-1);
        Thread claimer = new Thread(() -> claimed.set(store.claimSlices(9, false)));
        claimer.start();
        claimer.join();
        Thread releaser = new Thread(() -> release(store, claimed.get(), 9));
        releaser.start();
        releaser.join();
        assertEquals(0, store.sliceCounts()[0]);
        long again = store.claimSlices(9, false);
        assertEquals(slice(claimed.get()), slice(again), "the same slices");
        release(store, again, 9);
        assertThrows(IllegalStateException.class, () -> release(store, again, 9));
    }

    /**
     * One-shot runs of whole blocks: never a block with a slice claimed, and given back whole. A slice run cannot
     * take a block claimed whole.
     */
    @Test
    void wholeBlocksSkipBlocksInUse() {
        PageStore store = store(INTERVAL);
        long first = store.claimBlocks(2);
        assertEquals(0, store.firstBlock(first));
        assertEquals(2 * PER_BLOCK, slice(store.claimSlices(1, false)), "blocks 0 and 1 are taken");
        long second = store.claimBlocks(2);
        assertEquals(3, store.firstBlock(second), "block 2 has a slice claimed");
        assertEquals(5, store.firstBlock(store.claimBlocks(1)));
        Region region = store.regions[0];
        store.releaseBlocks(region, 0, 2);
        assertEquals(0, store.firstBlock(store.claimBlocks(2)), "free again");
        store.releaseBlocks(region, 5, 1);
        assertEquals(5, store.firstBlock(store.claimBlocks(1)));
        // Blocks 0, 1, 3, 4 and 5, and a slice of block 2.
        assertArrayEquals(new int[] {5 * PER_BLOCK + 1, 0, PER_REGION - 5 * PER_BLOCK - 1}, store.sliceCounts());
    }

    /**
     * Only free slices idle for the delay are purged; a run of them is one call, within a block (a run that goes on in
     * the next block is two calls, as mimalloc's purge ranges never span two bitmap words); a purged slice claimed
     * again is committed again.
     */
    @Test
    void thePurgeTakesIdleFreeSlicesInRunsWithinBlocks() {
        PageStore store = store(INTERVAL);
        // 58, 6 and 10 slices: no chunk length, one bin.
        long used = store.claimSlices(58, false);
        long tail = store.claimSlices(6, false);
        long next = store.claimSlices(10, false);
        assertEquals(58, slice(tail));
        assertEquals(PER_BLOCK, slice(next));
        assertEquals(74, store.slicesCommitted);
        release(store, tail, 6);
        release(store, next, 10);
        long now = System.nanoTime();
        store.purgeIfDue(now + INTERVAL / 2);
        assertEquals(0, regions.purgeCalls(), "not idle long enough");
        store.purgeIfDue(now + 2 * INTERVAL);
        assertEquals(2, regions.purgeCalls(), "a run in block 0 and one in block 1");
        assertArrayEquals(new int[] {58 * SLICE, 6 * SLICE}, regions.purges.get(0));
        assertArrayEquals(new int[] {64 * SLICE, 10 * SLICE}, regions.purges.get(1));
        assertEquals(16, store.slicesPurged);
        assertArrayEquals(new int[] {58, 0, PER_REGION - 58}, store.sliceCounts());
        store.purgeIfDue(now + 4 * INTERVAL);
        assertEquals(2, regions.purgeCalls(), "nothing left with memory behind it");
        long again = store.claimSlices(14, false);
        assertEquals(PER_BLOCK, slice(again), "block 1 from its start: block 0 has 6 free");
        assertEquals(88, store.slicesCommitted, "purged slices are committed again");
        assertSharedAccounted(segments, store.allocator);
        release(store, used, 58);
        release(store, again, 14);
        assertSharedAccounted(segments, store.allocator);
        store.close();
        assertEquals(0, store.allocator.usedMemory());
    }

    /**
     * Threads claim runs and whole blocks, write their slices, check them and release them, while a purger purges
     * every free slice it can ({@code mmap}), or gives back every wholly free region ({@code malloc}, and
     * {@code heap}: one {@code byte[]} block of the heap allocator's size, 63 slices): no slice is ever claimed twice,
     * none in use is purged or given back (its owner would see zeroes or freed memory, and the source itself checks),
     * the memory accounting holds after, and every slice is free.
     */
    @ParameterizedTest(name = "source: {0}")
    @ValueSource(strings = {"mmap", "malloc", "heap"})
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void concurrentClaimsReleasesAndPurges(String kind) throws Exception {
        final int threads = 8;
        final int maxRegions = 256;
        final boolean mmap = "mmap".equals(kind);
        final boolean heap = "heap".equals(kind);
        if (mmap) {
            assumeTrue(MmapRegionSource.isAvailable(), "mmap regions need mmap");
        }
        final AtomicIntegerArray owners = new AtomicIntegerArray(maxRegions * PER_REGION);
        final AtomicReference<String> failure = new AtomicReference<String>();
        final Set<Region> claimedIn = ConcurrentHashMap.newKeySet();
        // malloc'd and byte[] regions are one block each, as the direct and heap allocators', with no region source
        // at all: the store falls back to one-block regions from the memory source directly.
        final CountingMemorySource segments = heap ? new CountingMemorySource(true) : this.segments;
        final CheckingRegionSource source = mmap ? new CheckingRegionSource(regions, claimedIn, owners, failure) :
                null;
        final PageStore store;
        if (heap) {
            int block = PageStoreConfig.HEAP_SEGMENT_SIZE_BYTES;
            segments.regionSize = block;
            store = closer.add(new AdaptivePoolingAllocator(segments, true, null,
                    new PageStoreConfig(block, SLICE, 1, 0, 0, block).withMallocRegions())).pageStore;
        } else {
            store = closer.add(newSharedAllocator(segments, source, mmap ? REGION_SIZE : SEGMENT_SIZE, 1)).pageStore;
        }
        if (source != null) {
            source.store = store;
        }
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicLong operations = new AtomicLong();
        List<Thread> workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            final int id = t + 1;
            workers.add(new Thread(() -> {
                try {
                    start.await();
                    new Worker(store, owners, claimedIn, failure, id).run(deadline, operations);
                } catch (Throwable e) {
                    failure.compareAndSet(null, "worker " + id + ": " + e);
                }
            }));
        }
        Thread purger = new Thread(() -> {
            try {
                start.await();
                while (System.nanoTime() - deadline < 0 && failure.get() == null) {
                    store.purgeIfDue(System.nanoTime());
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, "purger: " + e);
            }
        });
        for (Thread worker : workers) {
            worker.start();
        }
        purger.start();
        start.countDown();
        for (Thread worker : workers) {
            worker.join();
        }
        purger.join();
        assertNull(failure.get());
        assertTrue(store.regionCount() <= maxRegions);
        assertTrue(operations.get() > 1000, "too few claims to mean anything: " + operations.get());
        assertEquals(0, store.sliceCounts()[0], "a slice is still claimed");
        assertMapsShowEveryFit(store);
        assertEquals(heap, store.regions[0].buffer.hasArray(), "byte[] regions for heap memory only");
        assertSharedAccounted(segments, store.allocator);
        if (!mmap) {
            assertEquals(0, segments.chunks.size(), "no chunk, every region a segment");
            // All free and idle now: every region goes back, in passes of a budget each.
            purgeUntilDone(store, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
            assertEquals(0, countLive(store), "a wholly free region stayed");
            assertEquals(segments.segmentsAllocated(), store.regionsReleased, "every region mapped went back");
            assertEquals(0, store.allocator.usedMemory());
        } else {
            assertEquals(0, segments.segmentsAllocated());
            assertTrue(store.slicesPurged > 0, "the purger purged nothing");
        }
        store.close();
        assertEquals(0, store.allocator.usedMemory());
    }

    private static int countLive(PageStore store) {
        int live = 0;
        for (Region region : store.regions) {
            live += region.released ? 0 : 1;
        }
        return live;
    }

    /**
     * A {@code malloc}'d region goes back whole once all of it stayed free for the purge delay, never before nor while
     * a slice is claimed; the next region mapped takes its place.
     */
    @Test
    void aWhollyIdleMallocRegionGoesBackAndItsPlaceIsTaken() {
        PageStore store = closer.add(newSharedAllocator(segments, regions, SEGMENT_SIZE, INTERVAL)).pageStore;
        long first = store.claimSlices(9, false);
        assertEquals(SEGMENT_SIZE, store.allocator.usedMemory(), "counted whole");
        release(store, first, 9);
        store.purgeIfDue(System.nanoTime() + INTERVAL / 2);
        assertEquals(0, store.regionsReleased, "freed less than a delay ago");
        first = store.claimSlices(9, false);
        store.purgeIfDue(System.nanoTime() + 2 * INTERVAL);
        assertEquals(0, store.regionsReleased, "a slice is claimed");
        release(store, first, 9);
        store.purgeIfDue(System.nanoTime() + 4 * INTERVAL);
        assertEquals(1, store.regionsReleased);
        AbstractByteBuf releasedBuffer = segments.segments.get(0);
        assertEquals(0, releasedBuffer.refCnt(), "freed");
        assertEquals(0, store.allocator.usedMemory());
        assertEquals(0, store.purgeCalls, "no part of a malloc'd region is purged");
        long again = store.claimSlices(9, false);
        assertEquals(0, (int) (again >>> 32), "the released region's place");
        assertEquals(1, store.regionCount());
        assertEquals(2, segments.segmentsAllocated());
        assertTrue(store.regions[0].buffer != releasedBuffer);
        release(store, again, 9);
        store.close();
        assertEquals(0, store.allocator.usedMemory());
        assertEquals(0, segments.segmentsLive());
    }

    private static final class Worker {
        private static final int[] SIZES = {1, 2, 3, 4, 5, 8, 9, 17, 24, 32, 63};
        private final PageStore store;
        private final AtomicIntegerArray owners;
        private final Set<Region> claimedIn;
        private final AtomicReference<String> failure;
        private final int id;
        private final SplittableRandom random;
        private final int perBlock;
        /** Held runs: {region, first slice, slices, whole blocks (0 or 1)}. */
        private final List<int[]> held = new ArrayList<int[]>();
        private long stamp;

        Worker(PageStore store, AtomicIntegerArray owners, Set<Region> claimedIn, AtomicReference<String> failure,
               int id) {
            this.store = store;
            perBlock = store.config.slicesPerSegment();
            this.owners = owners;
            this.claimedIn = claimedIn;
            this.failure = failure;
            this.id = id;
            random = new SplittableRandom(id);
        }

        void run(long deadline, AtomicLong operations) {
            while (System.nanoTime() - deadline < 0 && failure.get() == null) {
                if (held.size() < 4 && (held.isEmpty() || random.nextBoolean())) {
                    claim();
                    operations.incrementAndGet();
                } else {
                    release(held.remove(random.nextInt(held.size())));
                }
            }
            while (!held.isEmpty()) {
                release(held.remove(held.size() - 1));
            }
        }

        private void claim() {
            int[] run;
            if (random.nextInt(16) == 0) {
                int blocks = 1 + random.nextInt(Math.min(2, store.config.segmentsPerRegion()));
                long taken = store.claimBlocks(blocks);
                assertTrue(taken >= 0);
                run = new int[] {(int) (taken >>> 32), (int) taken, blocks * perBlock, 1};
            } else {
                int n = SIZES[random.nextInt(SIZES.length)];
                long claimed = store.claimSlices(n, false);
                assertTrue(claimed >= 0);
                run = new int[] {(int) (claimed >>> 32), (int) claimed, n, 0};
            }
            for (int s = run[1]; s < run[1] + run[2]; s++) {
                int owner = owners.getAndSet(run[0] * PER_REGION + s, id);
                if (owner != 0) {
                    failure.compareAndSet(null, "slice " + s + " of region " + run[0] + " claimed by " + id
                            + " while " + owner + " holds it");
                }
            }
            Region region = store.regions[run[0]];
            claimedIn.add(region);
            AbstractByteBuf memory = region.buffer;
            long value = (long) id << 40 | ++stamp;
            for (int s = run[1]; s < run[1] + run[2]; s++) {
                memory.setLong(s * SLICE, value);
                memory.setLong(s * SLICE + SLICE - 8, value);
            }
            held.add(new int[] {run[0], run[1], run[2], run[3], (int) (value >>> 32), (int) value});
        }

        private void release(int[] run) {
            AbstractByteBuf memory = store.regions[run[0]].buffer;
            long value = (long) run[4] << 32 | run[5] & 0xFFFFFFFFL;
            for (int s = run[1]; s < run[1] + run[2]; s++) {
                if (memory.getLong(s * SLICE) != value || memory.getLong(s * SLICE + SLICE - 8) != value) {
                    failure.compareAndSet(null, "slice " + s + " of region " + run[0] + " lost the data of " + id);
                }
                // Disowned before the release: once released, another thread may own it.
                if (owners.getAndSet(run[0] * PER_REGION + s, 0) != id) {
                    failure.compareAndSet(null, "slice " + s + " of region " + run[0] + " not owned by " + id);
                }
            }
            Region region = store.regions[run[0]];
            if (run[3] == 1) {
                store.releaseBlocks(region, run[1] / perBlock, run[2] / perBlock);
            } else {
                region.blocks[run[1] / perBlock].releaseRun(run[1] % perBlock, run[2], System.nanoTime());
            }
        }
    }

    /** Checks that no purged slice has an owner, then purges: {@code mmap} regions only. */
    private static final class CheckingRegionSource extends MmapRegionSource {
        private final CountingRegionSource delegate;
        private final Set<Region> claimedIn;
        private final AtomicIntegerArray owners;
        private final AtomicReference<String> failure;
        volatile PageStore store;

        CheckingRegionSource(CountingRegionSource delegate, Set<Region> claimedIn, AtomicIntegerArray owners,
                             AtomicReference<String> failure) {
            super(UnpooledByteBufAllocator.DEFAULT);
            this.delegate = delegate;
            this.claimedIn = claimedIn;
            this.owners = owners;
            this.failure = failure;
        }

        @Override
        AbstractByteBuf allocateRegion(int size, int alignment) {
            return delegate.allocateRegion(size, alignment);
        }

        @Override
        void releaseRegion(AbstractByteBuf region) {
            Region r = check(region, 0, REGION_SIZE, "given back");
            if (r != null && !claimedIn.contains(r)) {
                // Mapped for a claim, and given back before that claim had its run.
                failure.compareAndSet(null, "region " + r.index + " given back before any claim had a run in it");
            }
            delegate.releaseRegion(region);
        }

        @Override
        void purge(AbstractByteBuf region, int offset, int length) {
            check(region, offset, length, "purged");
            delegate.purge(region, offset, length);
        }

        /** The region of {@code region}, or {@code null} at the close. */
        private Region check(AbstractByteBuf region, int offset, int length, String what) {
            Region found = null;
            for (Region r : store.regions) {
                if (r.buffer == region) {
                    found = r;
                }
            }
            if (found == null) {
                return null; // the close: nothing claims any more
            }
            int index = found.index;
            for (int s = offset / SLICE; s < (offset + length) / SLICE; s++) {
                int owner = owners.get(index * PER_REGION + s);
                if (owner != 0) {
                    failure.compareAndSet(null, "slice " + s + " of region " + index + ' ' + what + " while "
                            + owner + " holds it");
                }
            }
            return found;
        }
    }
}
