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
import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The page store's shared slices (see {@link PageStore#claimSlices}): first fit within a block, never across one,
 * heaps spread by sequence, releases from any thread reused at once, whole-block runs, and a purge that only takes
 * free idle slices, coalesced across blocks. Then all of it at once from many threads.
 */
final class SharedSlicesTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;
    private static final int PER_BLOCK = SEGMENT_SIZE / SLICE;
    private static final int PER_REGION = REGION_SIZE / SLICE;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    private PageStore store(long purgeDelayNanos) {
        return newSharedAllocator(segments, regions, REGION_SIZE, purgeDelayNanos).pageStore;
    }

    /** The run's first slice in its region. */
    private static int slice(long run) {
        assertTrue(run >= 0, "no run");
        return (int) run;
    }

    private static Segment block(PageStore store, long run) {
        return store.region((int) (run >>> 32)).block(slice(run) / PER_BLOCK);
    }

    private static void release(PageStore store, long run, int n) {
        store.releaseSlices(block(store, run), slice(run) % PER_BLOCK, n);
    }

    @Test
    void firstFitWithinABlockNeverAcrossOne() {
        PageStore store = store(INTERVAL);
        assertEquals(0, slice(store.claimSlices(40, 0, PageStore.NO_HEAP)));
        assertEquals(PER_BLOCK, slice(store.claimSlices(40, 0, PageStore.NO_HEAP)), "24 left in block 0: block 1");
        assertEquals(40, slice(store.claimSlices(24, 0, PageStore.NO_HEAP)), "the tail of block 0 fits");
        assertEquals(PER_BLOCK + 40, slice(store.claimSlices(1, 0, PageStore.NO_HEAP)));
        assertEquals(1, regions.regions.size());
        assertEquals(0, segments.segmentsAllocated(), "no segment of its own");
    }

    /** As mimalloc's thread sequence: the blocks claimed in so far are visited from {@code seq} modulo their count. */
    @Test
    void heapsStartInTheBlockOfTheirSequence() {
        PageStore store = store(INTERVAL);
        for (int block = 0; block < 3; block++) {
            assertEquals(block * PER_BLOCK, slice(store.claimSlices(60, 0, PageStore.NO_HEAP)));
        }
        assertEquals(PER_BLOCK + 60, slice(store.claimSlices(2, 4, PageStore.NO_HEAP)), "4 % 3: block 1");
        assertEquals(2 * PER_BLOCK + 60, slice(store.claimSlices(2, 5, PageStore.NO_HEAP)), "5 % 3: block 2");
        assertEquals(60, slice(store.claimSlices(2, 3, PageStore.NO_HEAP)), "3 % 3: block 0");
        assertEquals(PER_BLOCK + 62, slice(store.claimSlices(2, 4, PageStore.NO_HEAP)));
        assertEquals(2 * PER_BLOCK + 62, slice(store.claimSlices(2, 4, PageStore.NO_HEAP)), "block 1 full: on");
        assertEquals(3 * PER_BLOCK, slice(store.claimSlices(3, 4, PageStore.NO_HEAP)), "all full: a new block");
    }

    /** A run released by another thread is free for the next claim at once, and a second release throws. */
    @Test
    void aReleaseFromAnyThreadIsReusedAtOnce() throws Exception {
        final PageStore store = store(INTERVAL);
        final AtomicLong claimed = new AtomicLong(-1);
        Thread claimer = new Thread(() -> claimed.set(store.claimSlices(9, 3, PageStore.NO_HEAP)));
        claimer.start();
        claimer.join();
        Thread releaser = new Thread(() -> release(store, claimed.get(), 9));
        releaser.start();
        releaser.join();
        assertEquals(0, store.sliceCounts()[0]);
        long again = store.claimSlices(9, 7, PageStore.NO_HEAP);
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
        long first = store.takeRun(2);
        assertEquals(0, (int) first);
        assertEquals(2 * PER_BLOCK, slice(store.claimSlices(1, 0, PageStore.NO_HEAP)), "blocks 0 and 1 are taken");
        long second = store.takeRun(2);
        assertEquals(3, (int) second, "block 2 has a slice claimed");
        assertEquals(5, (int) store.takeRun(1));
        Region region = store.region(0);
        store.freeRun(region, 0, 2);
        assertEquals(0, (int) store.takeRun(2), "free again");
        store.freeRun(region, 5, 1);
        assertEquals(5, (int) store.takeRun(1));
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
        long used = store.claimSlices(60, 0, PageStore.NO_HEAP);
        long tail = store.claimSlices(4, 0, PageStore.NO_HEAP);
        long next = store.claimSlices(10, 0, PageStore.NO_HEAP);
        assertEquals(60, slice(tail));
        assertEquals(PER_BLOCK, slice(next));
        assertEquals(74, store.slicesCommitted);
        release(store, tail, 4);
        release(store, next, 10);
        long now = System.nanoTime();
        store.purgeIfDue(now + INTERVAL / 2);
        assertEquals(0, regions.purgeCalls(), "not idle long enough");
        store.purgeIfDue(now + 2 * INTERVAL);
        assertEquals(2, regions.purgeCalls(), "a run in block 0 and one in block 1");
        assertArrayEquals(new int[] {60 * SLICE, 4 * SLICE}, regions.purges.get(0));
        assertArrayEquals(new int[] {64 * SLICE, 10 * SLICE}, regions.purges.get(1));
        assertEquals(14, store.slicesPurged);
        assertArrayEquals(new int[] {60, 0, PER_REGION - 60}, store.sliceCounts());
        store.purgeIfDue(now + 4 * INTERVAL);
        assertEquals(2, regions.purgeCalls(), "nothing left with memory behind it");
        long again = store.claimSlices(14, 1, PageStore.NO_HEAP);
        assertEquals(PER_BLOCK, slice(again), "block 1 from its start: block 0 has 4 free");
        assertEquals(88, store.slicesCommitted, "purged slices are committed again");
        assertSharedAccounted(segments, store.allocator);
        release(store, used, 60);
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
        final AtomicIntegerArray owners = new AtomicIntegerArray(maxRegions * PER_REGION);
        final AtomicReference<String> failure = new AtomicReference<String>();
        final CheckingRegionSource source = new CheckingRegionSource(mmap ? regions :
                new CountingRegionSource(true, "heap".equals(kind)), owners, failure);
        // malloc'd and byte[] regions are one block each, as the direct and heap allocators'.
        final boolean heap = "heap".equals(kind);
        final CountingSegmentSource segments = heap ? new CountingSegmentSource(true) : this.segments;
        final PageStore store;
        if (heap) {
            int block = PageStoreConfig.HEAP_SEGMENT_SIZE_BYTES;
            store = new AdaptivePoolingAllocator(segments, true, segments, source,
                    new PageStoreConfig(block, SLICE, 1, 0, 0, block).withMallocRegions()).pageStore;
        } else {
            store = newSharedAllocator(segments, source, mmap ? REGION_SIZE : SEGMENT_SIZE, 1).pageStore;
        }
        source.store = store;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicLong operations = new AtomicLong();
        List<Thread> workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            final int id = t + 1;
            workers.add(new Thread(() -> {
                try {
                    start.await();
                    new Worker(store, owners, failure, id).run(deadline, operations);
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
        assertEquals(0, segments.segmentsAllocated());
        assertEquals(heap, source.delegate.regions.get(0).hasArray(), "byte[] regions for heap memory only");
        assertSharedAccounted(segments, store.allocator);
        if (!mmap) {
            // All free and idle now: every region goes back.
            store.purgeIfDue(System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
            assertEquals(0, countLive(store), "a wholly free region stayed");
            assertEquals(source.mapped(), store.regionsReleased, "every region mapped went back");
            assertEquals(0, store.allocator.usedMemory());
        } else {
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
        CountingRegionSource malloc = new CountingRegionSource(true);
        PageStore store = newSharedAllocator(segments, malloc, REGION_SIZE, INTERVAL).pageStore;
        long first = store.claimSlices(9, 0, PageStore.NO_HEAP);
        assertEquals(REGION_SIZE, store.allocator.usedMemory(), "counted whole");
        release(store, first, 9);
        store.purgeIfDue(System.nanoTime() + INTERVAL / 2);
        assertEquals(0, store.regionsReleased, "freed less than a delay ago");
        first = store.claimSlices(9, 0, PageStore.NO_HEAP);
        store.purgeIfDue(System.nanoTime() + 2 * INTERVAL);
        assertEquals(0, store.regionsReleased, "a slice is claimed");
        release(store, first, 9);
        store.purgeIfDue(System.nanoTime() + 4 * INTERVAL);
        assertEquals(1, store.regionsReleased);
        assertEquals(1, malloc.released.size());
        assertEquals(0, malloc.released.get(0).refCnt(), "freed");
        assertEquals(0, store.allocator.usedMemory());
        assertEquals(0, store.purgeCalls, "no part of a malloc'd region is purged");
        long again = store.claimSlices(9, 0, PageStore.NO_HEAP);
        assertEquals(0, (int) (again >>> 32), "the released region's place");
        assertEquals(1, store.regionCount());
        assertEquals(2, malloc.regions.size());
        assertTrue(store.region(0).buffer != malloc.released.get(0));
        release(store, again, 9);
        store.close();
        assertEquals(0, store.allocator.usedMemory());
        assertEquals(0, malloc.live());
    }

    private static final class Worker {
        private static final int[] SIZES = {1, 2, 3, 4, 5, 8, 9, 17, 24, 32, 63};
        private final PageStore store;
        private final AtomicIntegerArray owners;
        private final AtomicReference<String> failure;
        private final int id;
        private final SplittableRandom random;
        private final int perBlock;
        /** Held runs: {region, first slice, slices, whole blocks (0 or 1)}. */
        private final List<int[]> held = new ArrayList<int[]>();
        private long stamp;

        Worker(PageStore store, AtomicIntegerArray owners, AtomicReference<String> failure, int id) {
            this.store = store;
            perBlock = store.config.slicesPerSegment();
            this.owners = owners;
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
                long taken = store.takeRun(blocks);
                assertTrue(taken >= 0);
                run = new int[] {(int) (taken >>> 32), (int) taken * perBlock, blocks * perBlock, 1};
            } else {
                int n = SIZES[random.nextInt(SIZES.length)];
                long claimed = store.claimSlices(n, id, PageStore.NO_HEAP);
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
            AbstractByteBuf memory = store.region(run[0]).buffer;
            long value = (long) id << 40 | ++stamp;
            for (int s = run[1]; s < run[1] + run[2]; s++) {
                memory.setLong(s * SLICE, value);
                memory.setLong(s * SLICE + SLICE - 8, value);
            }
            held.add(new int[] {run[0], run[1], run[2], run[3], (int) (value >>> 32), (int) value});
        }

        private void release(int[] run) {
            AbstractByteBuf memory = store.region(run[0]).buffer;
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
            Region region = store.region(run[0]);
            if (run[3] == 1) {
                store.freeRun(region, run[1] / perBlock, run[2] / perBlock);
            } else {
                store.releaseSlices(region.block(run[1] / perBlock), run[1] % perBlock, run[2]);
            }
        }
    }

    /** Checks that no purged slice has an owner, then purges. */
    private static final class CheckingRegionSource implements RegionSource {
        private final CountingRegionSource delegate;
        private final AtomicIntegerArray owners;
        private final AtomicReference<String> failure;
        volatile PageStore store;

        CheckingRegionSource(CountingRegionSource delegate, AtomicIntegerArray owners,
                             AtomicReference<String> failure) {
            this.delegate = delegate;
            this.owners = owners;
            this.failure = failure;
        }

        @Override
        public AbstractByteBuf allocateRegion(int size, int alignment) {
            return delegate.allocateRegion(size, alignment);
        }

        @Override
        public boolean canPurgeSlices() {
            return delegate.canPurgeSlices();
        }

        @Override
        public void releaseRegion(AbstractByteBuf region) {
            check(region, 0, REGION_SIZE, "given back");
            delegate.releaseRegion(region);
        }

        @Override
        public void purge(AbstractByteBuf region, int offset, int length) {
            check(region, offset, length, "purged");
            delegate.purge(region, offset, length);
        }

        int mapped() {
            return delegate.regions.size();
        }

        private void check(AbstractByteBuf region, int offset, int length, String what) {
            int index = -1;
            for (Region r : store.regions) {
                if (r.buffer == region) {
                    index = r.index;
                }
            }
            if (index < 0) {
                return; // the close: nothing claims any more
            }
            for (int s = offset / SLICE; s < (offset + length) / SLICE; s++) {
                int owner = owners.get(index * PER_REGION + s);
                if (owner != 0) {
                    failure.compareAndSet(null, "slice " + s + " of region " + index + ' ' + what + " while "
                            + owner + " holds it");
                }
            }
        }
    }
}
