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

import io.netty.buffer.AdaptivePoolingAllocator.IdleDecay;
import io.netty.buffer.AdaptivePoolingAllocator.IntStack;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassChunkRecycler;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.MpscIntQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class SizeClassChunkRecyclerTest {
    // 2048 and 4096 share the MIN_CHUNK_SIZE pool; 8192 has a chunk size of its own (asserted below).
    private static final int SMALL = AdaptivePoolingAllocator.sizeClassIndexOf(2048);
    private static final int SMALL2 = AdaptivePoolingAllocator.sizeClassIndexOf(4096);
    private static final int LARGE = AdaptivePoolingAllocator.sizeClassIndexOf(8192);

    private static int chunkSize(int sizeClassIndex) {
        return AdaptivePoolingAllocator.chunkSizeOf(AdaptivePoolingAllocator.getSizeClasses()[sizeClassIndex],
                PageStoreConfig.SLICE_SIZE_BYTES, PageStoreConfig.MAX_SEGMENT_SIZE_BYTES);
    }

    private static MpscIntQueue freeList(int capacity) {
        return MpscIntQueue.create(capacity, -1);
    }

    private static IntStack localFreeList(int capacity) {
        return new IntStack(new int[capacity]);
    }

    private static AdaptivePoolingAllocator newAllocator() {
        return AdaptiveByteBufAllocatorTest.heap(new AdaptiveByteBufAllocator(false));
    }

    private static SizeClassChunkRecycler newRecycler() {
        return new SizeClassChunkRecycler(newAllocator());
    }

    @Test
    public void poolSharingFollowsChunkSize() {
        assertEquals(chunkSize(SMALL), chunkSize(SMALL2));
        assertNotEquals(chunkSize(SMALL2), chunkSize(LARGE));
    }

    @Test
    public void freeListsComeBackTogether() {
        SizeClassChunkRecycler recycler = newRecycler();
        MpscIntQueue fl = freeList(64);
        IntStack local = localFreeList(64);

        assertTrue(recycler.offer(fl, local, SMALL));
        assertEquals(1, recycler.size(SMALL));

        assertTrue(recycler.poll(SMALL));
        assertSame(fl, recycler.takeFreeList());
        assertSame(local, recycler.takeLocalFreeList());
        assertEquals(0, recycler.size(SMALL));
        assertFalse(recycler.poll(SMALL));
    }

    @Test
    public void sizeClassesWithTheSameChunkSizeShareOnePool() {
        SizeClassChunkRecycler recycler = newRecycler();
        MpscIntQueue fl = freeList(64);

        assertTrue(recycler.offer(fl, localFreeList(64), SMALL));
        assertEquals(1, recycler.size(SMALL2));
        assertEquals(0, recycler.size(LARGE));

        assertTrue(recycler.poll(SMALL2));
        assertSame(fl, recycler.takeFreeList());
        recycler.takeLocalFreeList();
    }

    @Test
    public void poolIsBoundedPerChunkSize() {
        SizeClassChunkRecycler recycler = newRecycler();
        int capacity = recycler.poolCapacity(SMALL);
        assertTrue(capacity > 1);
        List<MpscIntQueue> offered = new ArrayList<MpscIntQueue>();
        for (int i = 0; i < capacity; i++) {
            MpscIntQueue fl = freeList(64);
            offered.add(fl);
            assertTrue(recycler.offer(fl, localFreeList(64), SMALL));
        }
        assertFalse(recycler.offer(freeList(64), localFreeList(64), SMALL));
        assertEquals(capacity, recycler.size(SMALL));

        // LIFO: the lists given up last are the first to be reused.
        for (int i = capacity - 1; i >= 0; i--) {
            assertTrue(recycler.poll(SMALL));
            assertSame(offered.get(i), recycler.takeFreeList());
            recycler.takeLocalFreeList();
        }
        assertFalse(recycler.poll(SMALL));
    }

    @Test
    public void oneByteBudgetBoundsAllThePools() {
        SizeClassChunkRecycler recycler = newRecycler();
        // Fill the budget with the lists of one chunk size.
        int capacity = recycler.poolCapacity(LARGE);
        for (int i = 0; i < capacity; i++) {
            assertTrue(recycler.offer(freeList(64), localFreeList(64), LARGE));
        }
        // The budget is per recycler, not per chunk size: the lists of another chunk size are refused too.
        assertFalse(recycler.offer(freeList(64), localFreeList(64), SMALL));
        // Taking one out makes room again.
        assertTrue(recycler.poll(LARGE));
        recycler.takeFreeList();
        recycler.takeLocalFreeList();
        assertTrue(recycler.offer(freeList(64), localFreeList(64), SMALL));
        recycler.freeAll();
    }

    @Test
    public void freeAllDropsTheLists() {
        SizeClassChunkRecycler recycler = newRecycler();
        assertTrue(recycler.offer(freeList(64), localFreeList(64), SMALL));
        assertTrue(recycler.offer(freeList(32), localFreeList(32), LARGE));
        assertEquals(chunkSize(SMALL) + chunkSize(LARGE), recycler.retainedBytes());

        recycler.freeAll();

        assertEquals(0, recycler.retainedBytes());
        assertEquals(0, recycler.size(SMALL));
        assertEquals(0, recycler.size(LARGE));
        assertFalse(recycler.poll(SMALL));
    }

    /** Offer the lists of {@code count} chunks of {@code sizeClassIndex}, oldest first. */
    private static List<MpscIntQueue> offer(SizeClassChunkRecycler recycler, int sizeClassIndex, int count) {
        List<MpscIntQueue> offered = new ArrayList<MpscIntQueue>();
        for (int i = 0; i < count; i++) {
            MpscIntQueue fl = freeList(64);
            assertTrue(recycler.offer(fl, localFreeList(64), sizeClassIndex));
            offered.add(fl);
        }
        return offered;
    }

    /** Dropping the oldest lists moves the others down their pool: each pair must still come out together. */
    @Test
    public void listsStayPairedAcrossADecay() {
        SizeClassChunkRecycler recycler = newRecycler();
        List<MpscIntQueue> external = new ArrayList<MpscIntQueue>();
        List<IntStack> local = new ArrayList<IntStack>();
        for (int i = 0; i < 6; i++) {
            external.add(freeList(64));
            local.add(localFreeList(64));
            assertTrue(recycler.offer(external.get(i), local.get(i), LARGE));
        }
        recycler.decay();
        recycler.decay();
        assertEquals(3, recycler.size(LARGE));
        for (int i = external.size() - 1; i >= 3; i--) {
            assertTrue(recycler.poll(LARGE));
            assertSame(external.get(i), recycler.takeFreeList(), "newest first");
            assertSame(local.get(i), recycler.takeLocalFreeList(), "with its own local free list");
        }
        assertFalse(recycler.poll(LARGE));
    }

    /**
     * Half of the cold lists, rounded up once over all the pools: two pools with one cold pair each give up one,
     * not two.
     */
    @Test
    public void halfIsRoundedUpOnceOverAllThePools() {
        SizeClassChunkRecycler recycler = newRecycler();
        offer(recycler, SMALL, 1);
        offer(recycler, LARGE, 1);
        recycler.decay();
        recycler.decay();
        assertEquals(1, recycler.size(SMALL) + recycler.size(LARGE), "ceil(2 / 2) = 1 dropped in total");
    }

    /**
     * Lists that sat in a pool through a whole interval are dropped half at a time, oldest first: lists offered
     * during an interval survive the decay that ends it, and a pool of 8 then keeps 4, 2, 1, 0 over consecutive
     * intervals.
     */
    @Test
    public void coldListsAreDroppedHalfAtATimeOldestFirst() {
        SizeClassChunkRecycler recycler = newRecycler();
        List<MpscIntQueue> offered = offer(recycler, LARGE, 8);
        int chunk = chunkSize(LARGE);
        recycler.decay();
        assertEquals(8, recycler.size(LARGE), "offered during this interval: not cold yet");

        int[] expected = {4, 2, 1, 0, 0};
        for (int remaining : expected) {
            recycler.decay();
            assertEquals(remaining, recycler.size(LARGE));
            assertEquals((long) remaining * chunk, recycler.retainedBytes());
        }

        // The newest are the ones kept: a pool of 8 keeps the last 4 offered after one cold decay.
        recycler = newRecycler();
        offered = offer(recycler, LARGE, 8);
        recycler.decay();
        recycler.decay();
        for (int i = offered.size() - 1; i >= 4; i--) {
            assertTrue(recycler.poll(LARGE));
            assertSame(offered.get(i), recycler.takeFreeList(), "the oldest are dropped first, list " + i);
            recycler.takeLocalFreeList();
        }
        assertFalse(recycler.poll(LARGE));
    }

    /** Lists taken during an interval are not cold, even when they come back before the decay. */
    @Test
    public void listsTakenDuringTheIntervalAreNotCold() {
        SizeClassChunkRecycler recycler = newRecycler();
        offer(recycler, LARGE, 8);
        recycler.decay();

        // Three are taken and given back: five sat untouched.
        List<MpscIntQueue> taken = new ArrayList<MpscIntQueue>();
        for (int i = 0; i < 3; i++) {
            assertTrue(recycler.poll(LARGE));
            taken.add(recycler.takeFreeList());
            recycler.takeLocalFreeList();
        }
        for (MpscIntQueue fl : taken) {
            assertTrue(recycler.offer(fl, localFreeList(64), LARGE));
        }

        recycler.decay();
        assertEquals(8 - 3, recycler.size(LARGE), "half of the five cold ones, rounded up, are dropped");
        for (int i = taken.size() - 1; i >= 0; i--) {
            assertTrue(recycler.poll(LARGE));
            assertSame(taken.get(i), recycler.takeFreeList(), "a list taken during the interval survives");
            recycler.takeLocalFreeList();
        }
    }

    /**
     * The heap's {@link IdleDecay} looks at the clock once per {@link IdleDecay#DECAY_MIN_ALLOCATIONS} allocations,
     * and decays only when the interval passed by then; the purge ticks that feed it are far more frequent.
     */
    @Test
    public void idleDecayLooksAtTheClockOncePerCountAndDecaysOnlyAfterTheInterval() {
        AdaptivePoolingAllocator allocator = newAllocator();
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler(allocator);
        IdleDecay idleDecay = new IdleDecay(allocator.pageStore);
        idleDecay.recycler = recycler;
        offer(recycler, LARGE, 2);
        // Both lists are cold from here, and the last decay was long ago.
        idleDecay.decay(System.nanoTime() - 2 * IdleDecay.DECAY_INTERVAL_NANOS);

        idleDecay.count(IdleDecay.DECAY_MIN_ALLOCATIONS - 1);
        assertEquals(2, recycler.size(LARGE), "the interval passed, but not enough allocations");
        idleDecay.count(1);
        assertEquals(1, recycler.size(LARGE), "both passed: half of the cold ones are dropped");
        idleDecay.count(2 * IdleDecay.DECAY_MIN_ALLOCATIONS);
        assertEquals(1, recycler.size(LARGE), "enough allocations, but the interval started again");
        // That look at the clock started a new count: the interval passing now is not enough on its own.
        idleDecay.lastDecayNanos -= 2 * IdleDecay.DECAY_INTERVAL_NANOS;
        idleDecay.count(IdleDecay.DECAY_MIN_ALLOCATIONS - 1);
        assertEquals(1, recycler.size(LARGE), "the interval passed, but the count restarted at the last look");
    }

    /**
     * Whatever the chunk sizes are, two size classes share a recycler pool exactly when their chunks have the same
     * size, adjacent or not.
     */
    @Test
    public void sizeClassesShareAPoolExactlyWhenTheirChunkSizesAreEqual() {
        AdaptivePoolingAllocator allocator = newAllocator();
        int[] sizeClasses = AdaptivePoolingAllocator.getSizeClasses();
        for (int a = 0; a < sizeClasses.length; a++) {
            for (int b = 0; b < sizeClasses.length; b++) {
                int poolA = allocator.sizeClassToChunkPool[a];
                int poolB = allocator.sizeClassToChunkPool[b];
                boolean sameChunkSize = allocator.chunkSizes[poolA] == allocator.chunkSizes[poolB];
                assertEquals(sameChunkSize, poolA == poolB, sizeClasses[a] + " and " + sizeClasses[b]);
            }
        }
    }

    /**
     * The same property for size class tables other than today's: a changed table is how the pools of size classes
     * that are not adjacent were once left unshared. The tables are random selections of the size classes in random
     * order, so that equal chunk sizes are rarely adjacent.
     */
    @Test
    public void anyTableOfSizeClassesSharesAPoolExactlyWhenChunkSizesAreEqual() {
        int slice = PageStoreConfig.SLICE_SIZE_BYTES;
        int max = PageStoreConfig.MAX_SEGMENT_SIZE_BYTES;
        int[] all = AdaptivePoolingAllocator.getSizeClasses();
        Random random = new Random(42);
        for (int round = 0; round < 1000; round++) {
            int[] sizeClasses = new int[1 + random.nextInt(all.length)];
            for (int i = 0; i < sizeClasses.length; i++) {
                sizeClasses[i] = all[random.nextInt(all.length)];
            }
            int[] chunkSizes = AdaptivePoolingAllocator.distinctChunkSizes(sizeClasses, slice, max);
            byte[] pools = AdaptivePoolingAllocator.chunkPools(sizeClasses, chunkSizes, slice, max);

            Set<Integer> distinct = new HashSet<Integer>();
            for (int chunkSize : chunkSizes) {
                assertTrue(distinct.add(chunkSize), "chunk size listed twice: " + chunkSize);
            }
            assertEquals(sizeClasses.length, pools.length);
            for (int a = 0; a < sizeClasses.length; a++) {
                int chunkSizeA = AdaptivePoolingAllocator.chunkSizeOf(sizeClasses[a], slice, max);
                assertEquals(chunkSizeA, chunkSizes[pools[a]]);
                for (int b = 0; b < sizeClasses.length; b++) {
                    boolean sameChunkSize =
                            chunkSizeA == AdaptivePoolingAllocator.chunkSizeOf(sizeClasses[b], slice, max);
                    assertEquals(sameChunkSize, pools[a] == pools[b], sizeClasses[a] + " and " + sizeClasses[b]);
                }
            }
        }
    }

    /**
     * End to end through the allocator: chunks of one size class are given up past the cache floor, so their free
     * lists go to the recycler, and a different size class with the same chunk size must reuse them - smaller than
     * it needs (4096 then 32), larger (32 then 4096), or one of each (1152: 113 segments, an external list rounded up
     * to 128 entries and a local one of exactly 113; then 1024). Size classes that are not adjacent share chunks too:
     * 16896 then 67584. From 16 KiB up a whole family (2^n, and 2^n plus header) shares one chunk size: 131072 then
     * 16384.
     */
    @ParameterizedTest
    @CsvSource({
            "4096, 32, false", "32, 4096, false", "1152, 1024, false", "16896, 67584, false", "131072, 16384, false",
            "4096, 32, true", "32, 4096, true", "1152, 1024, true", "16896, 67584, true", "131072, 16384, true",
    })
    public void chunksAreReusedAcrossSizeClassesWithTheirFreeLists(int freedSize, int reusingSize, boolean threadLocal)
            throws Exception {
        Field lowMem = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        lowMem.setAccessible(true);
        assumeFalse(lowMem.getBoolean(null) && Math.max(freedSize, reusingSize) > 16896,
                "low-memory mode pools the size classes up to 16896 only");
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, threadLocal);
        Runnable test = () -> assertReusedAcrossSizeClasses(allocator, freedSize, reusingSize);
        if (threadLocal) {
            FastThreadLocalThread.runWithFastThreadLocal(test);
        } else {
            test.run();
        }
    }

    private static SizeClassedChunk chunkOf(ByteBuf buf) {
        while (!(buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf)) {
            buf = buf.unwrap();
        }
        return (SizeClassedChunk) ((AdaptivePoolingAllocator.AdaptiveByteBuf) buf).chunk;
    }

    private static void assertReusedAcrossSizeClasses(AdaptiveByteBufAllocator allocator, int freedSize,
                                                      int reusingSize) {
        int chunkSize = AdaptivePoolingAllocator.chunkSizeOf(freedSize);
        assertEquals(chunkSize, AdaptivePoolingAllocator.chunkSizeOf(reusingSize));
        // 64 chunks: well past the ones a size class keeps, so the lists of the rest go to the recycler.
        int chunks = 64;

        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < chunks * (chunkSize / freedSize); i++) {
            bufs.add(allocator.heapBuffer(freedSize, freedSize));
        }
        SizeClassChunkRecycler recycler = chunkOf(bufs.get(0)).owningCache.chunkRecycler;
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        int reusingClass = AdaptivePoolingAllocator.sizeClassIndexOf(reusingSize);
        int pooled = recycler.size(reusingClass);
        assertTrue(pooled > 0, "the first size class gave up no free lists");

        IdentityHashMap<byte[], Set<Integer>> segments = new IdentityHashMap<byte[], Set<Integer>>();
        int count = chunks * (chunkSize / reusingSize);
        for (int i = 0; i < count; i++) {
            ByteBuf buf = allocator.heapBuffer(reusingSize, reusingSize);
            buf.writeInt(i);
            Set<Integer> offsets = segments.get(buf.array());
            if (offsets == null) {
                offsets = new HashSet<Integer>();
                segments.put(buf.array(), offsets);
            }
            assertTrue(offsets.add(buf.arrayOffset()), "segment handed out twice");
            bufs.add(buf);
        }
        assertTrue(recycler.size(reusingClass) < pooled, "no free list of the first size class was reused");
        for (int i = 0; i < bufs.size(); i++) {
            assertEquals(i, bufs.get(i).readInt());
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }
}
