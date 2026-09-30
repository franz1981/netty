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

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.giveBack;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PageStore}: the order a heap's segments come from (fullest region, new region), and the accounting of
 * segments and regions against what the sources handed out, through a random workload and the heaps' free.
 */
final class PageStoreTest {
    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** A free slot of the fullest region, its lowest; then a new region. */
    @Test
    void takesFromTheFullestRegionThenANewRegion() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        Segment a = heap.claim(63);
        heap.claim(63);
        giveBack(heap, a, 0, 63);
        Segment again = store.take(heap);
        assertSame(a, again, "slot 0 is free again: the lowest");
        assertSame(heap, again.owner);
        Segment fromRegion = store.take(heap);
        assertSame(a.region, fromRegion.region);
        assertEquals(2, fromRegion.slot, "the lowest free slot of the fullest region");
        for (int slot = 3; slot < REGION_SIZE / SEGMENT_SIZE; slot++) {
            assertEquals(slot, store.take(heap).slot);
        }
        assertEquals(1, store.regionPool.regionCount());
        Segment fresh = store.take(heap);
        assertNotSame(a.region, fresh.region, "no free slot left: a new region");
        assertEquals(0, fresh.slot);
        assertEquals(2, store.regionPool.regionCount());
        store.free(releaseOwnership(again));
        assertAccounted(segments, regions, allocator);
    }

    /** A segment taken by take() and never claimed from is given back as {@link HeapSegments#afterFree} would. */
    private static Segment releaseOwnership(Segment segment) {
        assertTrue(segment.isWhollyFree());
        assertTrue(Segment.OWNER.compareAndSet(segment, segment.owner, null));
        return segment;
    }

    /**
     * Several heaps claiming and releasing at random, with their spares ageing: at every step the allocator's used
     * memory is exactly what the sources handed out and did not get back, and once the heaps are freed nothing is left.
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
            assertAccounted(segments, regions, allocator);
        }
        while (live > 0) {
            live--;
            owners[live].release(spans[live], starts[live], lengths[live]);
        }
        for (HeapSegments heap : heaps) {
            assertEquals(0, heap.count, "no span out: a spare at most");
            heap.markFreed();
            heap.afterFree();
        }
        assertEquals(0, allocator.usedMemory());
        assertEquals(0, segments.segmentsLive());
        assertEquals(0, regions.live());
        assertEquals(withRegions, !regions.regions.isEmpty());
        assertAccounted(segments, regions, allocator);
    }
}
