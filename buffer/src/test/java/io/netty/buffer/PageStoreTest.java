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
     * Several heaps claiming and releasing at random, with their ticks driving the purge, on regions of many blocks or
     * of one: at every step the allocator's used memory is exactly the committed slices, or the regions of one block
     * not given back; once the store is closed, nothing is left.
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
                store.purgeIfDue(now += INTERVAL / 2);
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
            assertSharedAccounted(segments, allocator);
        }
        while (live > 0) {
            live--;
            owners[live].release(spans[live], starts[live], lengths[live]);
        }
        assertEquals(0, store.sliceCounts()[0], "no span out");
        assertEquals(withRegions, !regions.regions.isEmpty());
        assertEquals(withRegions, store.slicesPurged > 0, "the ticks drove the purge of idle slices");
        assertEquals(!withRegions, store.regionsReleased > 0, "and gave back idle regions of one block");
        store.purgeIfDue(now + 2 * INTERVAL);
        assertEquals(0, segments.segmentsLive());
        assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(0, allocator.usedMemory());
        assertEquals(0, regions.live(), "the close unmaps every region");
        assertEquals(0, store.regionCount());
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
     * A region that cannot be mapped switches the store to regions of one block from its fallback source, for good:
     * the free slices of the regions mapped so far are still claimed first, a run of blocks no longer fits.
     */
    @Test
    void aRegionThatCannotBeMappedFallsBackToOneBlockRegions() {
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
        CountingRegionSource malloc = new CountingRegionSource(true);
        segments.fallback = malloc;
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(segments, true, segments, failing,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT,
                        SEGMENT_SIZE));
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        Segment[] blocks = new Segment[SLOTS];
        for (int i = 0; i < SLOTS; i++) {
            blocks[i] = heap.claim(SPAN);
            assertSame(failing, blocks[i].region.source);
        }
        Segment first = heap.claim(SPAN);
        assertEquals(2, calls[0]);
        assertSame(malloc, store.regionSource);
        assertSame(malloc, first.region.source);
        assertEquals(1, first.region.slots, "a region of one block");
        heap.release(blocks[4], 0, SPAN);
        assertSame(blocks[4], heap.claim(SPAN), "the free slices of the mapped region");
        Segment second = heap.claim(SPAN);
        assertSame(malloc, second.region.source);
        assertEquals(2, calls[0], "never tried again");
        assertEquals(3, store.regionCount());
        assertEquals(2, malloc.regions.size());
        assertEquals(-1, store.takeRun(2), "new regions hold one block");
        assertEquals(0, segments.segmentsAllocated());
        assertSharedAccounted(segments, allocator);
        for (Segment block : new Segment[] {first, second}) {
            heap.release(block, 0, SPAN);
        }
        for (Segment block : blocks) {
            heap.release(block, 0, SPAN);
        }
        store.purgeIfDue(System.nanoTime() + 4 * INTERVAL);
        assertEquals(2, store.regionsReleased, "the idle one-block regions went back, the mapped one stays");
        assertEquals(0, malloc.live());
        assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(0, allocator.usedMemory());
    }

    /** A page store needs regions: a config without them, or no region source, is rejected. */
    @Test
    void withoutRegionsThereIsNoPageStore() {
        assertThrows(IllegalArgumentException.class, () -> newAllocator(segments, regions, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new AdaptivePoolingAllocator(segments, true, segments,
                null, new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT)));
        assertEquals(0, segments.segmentsAllocated());
        assertEquals(0, regions.regions.size());
    }
}
