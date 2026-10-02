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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertSharedAccounted;
import static io.netty.buffer.PageStoreTestSupport.giveBack;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link PageStore} as heaps see it: aligned blocks of the regions, accounting through a random workload of several
 * heaps and the close, the fallback of one allocation per block once no region can be mapped, and no regions at all.
 */
final class PageStoreTest {
    private static final int SLOTS = REGION_SIZE / SEGMENT_SIZE;
    private static final int PER_BLOCK = SEGMENT_SIZE / SLICE_SIZE_BYTES;
    private static final int SPAN = PER_BLOCK - 1;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** Where regions are mapped, a region starts at a multiple of 2 MiB, and so does each of its (4 MiB) blocks. */
    @Test
    void regionsAreAligned() {
        assumeTrue(regions.mmap != null, "aligned regions need mmap");
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        for (int i = 0; i < 3; i++) {
            Segment block = heap.claim(63);
            assertEquals(0, heap.claimedStart());
            assertEquals(0, block.memoryAddress() & REGION_ALIGNMENT - 1, "block " + i);
        }
        assertEquals(0, allocator.pageStore.regions[0].buffer.memoryAddress() & REGION_ALIGNMENT - 1);
    }

    /**
     * Several heaps claiming and releasing at random, with their decays driving the purge: at every step the
     * allocator's used memory is exactly the blocks allocated on their own and not freed plus the committed slices;
     * once the heaps are freed and the store closed, nothing is left.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void accountingMatchesTheSourcesThroughARandomWorkload(boolean withRegions) {
        AdaptivePoolingAllocator allocator = withRegions ?
                newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT) :
                newAllocator(segments, SEGMENT_SIZE);
        PageStore store = allocator.pageStore;
        HeapSegments[] heaps = new HeapSegments[3];
        for (int i = 0; i < heaps.length; i++) {
            heaps[i] = new HeapSegments(store, null, Thread.currentThread());
        }
        int capacity = 48;
        HeapSegments[] owners = new HeapSegments[capacity];
        Segment[] spans = new Segment[capacity];
        int[] starts = new int[capacity];
        int[] lengths = new int[capacity];
        int live = 0;
        long now = System.nanoTime();
        Random random = new Random(11);
        for (int op = 0; op < 10_000; op++) {
            int dice = random.nextInt(16);
            if (dice == 0) {
                heaps[random.nextInt(heaps.length)].decay(now += INTERVAL / 2);
            } else if (live == capacity || live > 0 && dice < 8) {
                int k = random.nextInt(live);
                owners[k].release(spans[k], starts[k], lengths[k]);
                live--;
                owners[k] = owners[live];
                spans[k] = spans[live];
                starts[k] = starts[live];
                lengths[k] = lengths[live];
            } else {
                HeapSegments heap = heaps[random.nextInt(heaps.length)];
                int n = 1 + random.nextInt(16);
                owners[live] = heap;
                spans[live] = heap.claim(n);
                starts[live] = heap.claimedStart();
                lengths[live] = n;
                live++;
            }
            assertAccounted(allocator, withRegions);
        }
        while (live > 0) {
            live--;
            owners[live].release(spans[live], starts[live], lengths[live]);
        }
        for (HeapSegments heap : heaps) {
            assertEquals(0, heap.count, "no span out: reserved segments at most");
            heap.markFreed();
            heap.afterFree();
        }
        assertEquals(0, segments.segmentsLive());
        assertEquals(withRegions, !regions.regions.isEmpty());
        assertEquals(withRegions, store.slicesPurged > 0, "the decays drove the purge of idle slices");
        assertAccounted(allocator, withRegions);
        store.close();
        assertEquals(0, allocator.usedMemory());
        assertEquals(0, regions.live(), "the close unmaps every region");
        assertEquals(0, store.regionCount());
    }

    private void assertAccounted(AdaptivePoolingAllocator allocator, boolean withRegions) {
        if (withRegions) {
            assertSharedAccounted(segments, allocator);
        } else {
            PageStoreTestSupport.assertAccounted(segments, allocator);
        }
    }

    /** The close unmaps every region, with runs still claimed, and accounts all of them as freed. */
    @Test
    void closeUnmapsEveryRegion() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        for (int i = 0; i < SLOTS + 2; i++) {
            heap.claim(PER_BLOCK);
        }
        assertEquals(2, regions.live());
        assertEquals((SLOTS + 2L) * SEGMENT_SIZE, allocator.usedMemory());
        store.close();
        assertEquals(0, regions.live());
        assertEquals(0, allocator.usedMemory());
        assertThrows(IllegalStateException.class, () -> heap.claim(1));
    }

    /**
     * A region that cannot be mapped turns the mapping off for good: the free slices of the regions mapped so far are
     * still claimed, a heap's own blocks first once it has some, then blocks are allocated on their own.
     */
    @Test
    void aRegionThatCannotBeMappedFallsBackToOneAllocationPerBlock() {
        final int[] calls = new int[1];
        RegionSource failing = new RegionSource() {
            @Override
            public AbstractByteBuf allocateRegion(int size, int alignment) {
                if (++calls[0] > 1) {
                    throw new OutOfMemoryError("mmap(2) failed to map " + size + " bytes: errno 12");
                }
                return regions.allocateRegion(size, alignment);
            }

            @Override
            public void purge(AbstractByteBuf region, int offset, int length) {
                regions.purge(region, offset, length);
            }
        };
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(segments, true, segments, failing,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT));
        PageStore store = allocator.pageStore;
        // A heap's own block cannot be claimed whole: spans of all slices but one.
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        Segment[] blocks = new Segment[SLOTS];
        for (int i = 0; i < SLOTS; i++) {
            blocks[i] = heap.claim(SPAN);
            assertNotNull(blocks[i].region);
        }
        Segment own = heap.claim(SPAN);
        assertNull(own.region, "no region: allocated on its own");
        assertEquals(2, calls[0]);
        assertFalse(store.mapsRegions);
        heap.release(blocks[4], 0, SPAN);
        assertSame(blocks[4], heap.claim(SPAN), "the free slices of the mapped region");
        Segment ownAgain = heap.claim(SPAN);
        assertNull(ownAgain.region);
        assertEquals(2, calls[0], "never tried again");
        assertEquals(1, store.regionCount());
        assertEquals(2, segments.segmentsAllocated());
        assertSharedAccounted(segments, allocator);
        giveBack(heap, own, 0, SPAN);
        giveBack(heap, ownAgain, 0, SPAN);
        assertEquals(0, segments.segmentsLive());
        store.close();
        assertEquals(0, allocator.usedMemory());
    }

    /**
     * Regions off, by a config without them or without a region source: one allocation per block, freed when it is
     * given back.
     */
    @Test
    void withoutRegionsSegmentsAreAllocatedOneByOne() {
        AdaptivePoolingAllocator noRegions = newAllocator(segments, regions, 0, 0);
        assertNull(noRegions.pageStore.regionSource);
        AdaptivePoolingAllocator noSource = new AdaptivePoolingAllocator(segments, true, segments, null,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT));
        assertNull(noSource.pageStore.regionSource);
        for (AdaptivePoolingAllocator allocator : new AdaptivePoolingAllocator[] {noRegions, noSource}) {
            HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
            Segment segment = heap.claim(10);
            assertNull(segment.region);
            assertNotNull(segment.buffer);
            assertEquals(SEGMENT_SIZE, allocator.usedMemory());
            giveBack(heap, segment, 0, 10);
            assertEquals(0, allocator.usedMemory());
        }
        assertEquals(2, segments.segmentsAllocated());
        assertEquals(0, segments.segmentsLive());
        assertEquals(0, regions.regions.size());
    }
}
