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
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.giveBack;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Segments carved out of regions ({@link RegionPool}): where a segment comes from, where it goes back, when a region is
 * freed, and how regions are accounted.
 */
final class RegionPoolTest {
    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    private static boolean addressesReadable() {
        return PlatformDependent.hasUnsafe() || PlatformDependent.hasAlignDirectByteBuffer();
    }

    /**
     * The first segments come from one region, slot after slot, and are views of it: no segment is allocated on its
     * own. The used memory is the region, for what was allocated for it. A tenth segment needs a second region.
     */
    @Test
    void segmentsComeFromRegions() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        RegionPool pool = allocator.pageStore.regionPool;
        assertNotNull(pool);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < 9; i++) {
            Segment segment = heap.claim(63);
            taken.add(segment);
            assertNotNull(segment.region);
            assertEquals(i, segment.slot);
            assertSame(taken.get(0).region, segment.region);
            assertEquals(1, pool.regionCount());
        }
        assertEquals(0, segments.segmentsAllocated(), "no segment of its own");
        assertEquals(1, regions.regions.size());
        Region region = taken.get(0).region;
        assertEquals(regions.allocatedBytes.get(0).intValue(), region.capacity());
        assertTrue(region.capacity() == REGION_SIZE || region.capacity() == REGION_SIZE + REGION_ALIGNMENT,
                "" + region.capacity());
        assertEquals(region.capacity(), allocator.usedMemory());
        assertEquals(0, region.freeSlots);
        long base = region.buffer.memoryAddress();
        for (Segment segment : taken) {
            assertEquals(base + (long) segment.slot * SEGMENT_SIZE, segment.memoryAddress());
            segment.buffer.setLong(SEGMENT_SIZE - 8, segment.slot);
        }
        for (Segment segment : taken) {
            assertEquals(segment.slot, region.buffer.getLong(segment.slot * SEGMENT_SIZE + SEGMENT_SIZE - 8));
        }
        Segment tenth = heap.claim(63);
        assertNotSame(region, tenth.region);
        assertEquals(0, tenth.slot);
        assertEquals(2, pool.regionCount());
        assertEquals(2, regions.regions.size());
        assertAccounted(segments, regions, allocator);
    }

    /** With alignment, a region starts at a multiple of 2 MiB, and so does each of its (4 MiB) segments. */
    @Test
    void regionsAreAligned() {
        org.junit.jupiter.api.Assumptions.assumeTrue(addressesReadable());
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        for (int i = 0; i < 3; i++) {
            Segment segment = heap.claim(63);
            assertEquals(0, segment.memoryAddress() & (REGION_ALIGNMENT - 1), "segment " + i);
        }
        assertEquals(0, allocator.pageStore.regionPool.regions[0].buffer.memoryAddress() & (REGION_ALIGNMENT - 1));
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A segment comes from the fullest region with a free slot, its lowest free slot; the oldest of equally full
     * regions wins. A segment given back by its heap goes straight back to its region, and a region whose segments
     * are all back is freed at once.
     */
    @Test
    void fullestRegionFirstAndFreedWhenAllBack() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        RegionPool pool = allocator.pageStore.regionPool;
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < 18; i++) {
            taken.add(heap.claim(63));
        }
        assertEquals(2, pool.regionCount());
        Region a = taken.get(0).region;
        Region b = taken.get(9).region;
        assertNotSame(a, b);
        // a: slots 2, 4, 6 back (3 free); b: slots 1, 3, 5, 7, 8 back (5 free).
        for (int slot : new int[] {2, 4, 6}) {
            giveBack(heap, taken.get(slot), 0, 63);
        }
        for (int slot : new int[] {1, 3, 5, 7, 8}) {
            giveBack(heap, taken.get(9 + slot), 0, 63);
        }
        assertEquals(3, a.freeSlotCount());
        assertEquals(5, b.freeSlotCount());
        Segment next = heap.claim(63);
        assertSame(a, next.region, "a is fuller");
        assertEquals(2, next.slot, "its lowest free slot");
        heap.claim(63); // a: slot 4, 1 free left
        assertEquals(1, a.freeSlotCount());
        // Four more of a back: a and b both have 5 free slots.
        giveBack(heap, taken.get(0), 0, 63);
        giveBack(heap, taken.get(1), 0, 63);
        giveBack(heap, taken.get(3), 0, 63);
        giveBack(heap, taken.get(5), 0, 63);
        assertEquals(5, a.freeSlotCount());
        assertEquals(5, b.freeSlotCount());
        Segment tie = heap.claim(63);
        assertSame(a, tie.region, "equally full: the older region");
        assertEquals(0, tie.slot);
        giveBack(heap, tie, 0, 63);
        assertEquals(2, regions.live());

        // b drains: once its last segment is back, it is freed, and accounted as such.
        long before = allocator.usedMemory();
        for (int slot : new int[] {0, 2, 4, 6}) {
            giveBack(heap, taken.get(9 + slot), 0, 63);
        }
        assertEquals(1, pool.regionCount());
        assertEquals(1, regions.live());
        assertEquals(0, b.buffer.refCnt());
        assertEquals(before - b.capacity(), allocator.usedMemory());
        assertEquals(1, pool.freed);
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A segment that empties stays with its heap as the spare, and its region stays; the heap's decays give it back
     * once it stayed unused a whole interval, and the region is freed with it when it was the last one out.
     */
    @Test
    void spareKeepsItsRegionUntilAged() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        RegionPool pool = allocator.pageStore.regionPool;
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment a = heap.claim(10);
        Segment b = heap.claim(60);
        assertSame(a.region, b.region);
        heap.release(a, 0, 10);
        heap.release(b, 0, 60);
        assertSame(b, heap.spare);
        assertEquals(8, a.region.freeSlotCount(), "a went back, b is the spare");
        assertEquals(1, regions.live());
        long now = System.nanoTime();
        heap.decay(now);
        assertEquals(1, pool.regionCount());
        heap.decay(now + INTERVAL);
        assertNull(heap.spare);
        assertEquals(0, pool.regionCount(), "the last segment back frees the region");
        assertEquals(0, regions.live());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(segments, regions, allocator);
    }

    /** A segment taken again from its region is the same one, wholly free, with its span buffers. */
    @Test
    void aSegmentBackInItsRegionIsReused() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment keep = heap.claim(63); // keeps the region
        Segment a = heap.claim(63);
        giveBack(heap, a, 0, 63);
        Segment again = heap.claim(63);
        assertSame(a, again);
        assertSame(heap, again.owner);
        giveBack(heap, again, 0, 63);
        giveBack(heap, keep, 0, 63);
        assertEquals(0, regions.live());
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A segment of a freed heap emptied by another thread's release goes straight to its region from that thread,
     * which frees the region when it was the last one out.
     */
    @Test
    void foreignReleaseGivesTheSegmentBackToItsRegion() throws Exception {
        final AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        final HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        final Segment segment = heap.claim(10);
        heap.markFreed();
        heap.afterFree();
        Thread releaser = new Thread(() -> heap.release(segment, 0, 10));
        releaser.start();
        releaser.join();
        assertEquals(0, regions.live());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(segments, regions, allocator);
    }

    /** Regions off: a config without regions, or no region source, allocates one buffer per segment as before. */
    @Test
    void regionsOffAllocatesSegmentsOneByOne() {
        AdaptivePoolingAllocator noRegions = newAllocator(segments, regions, 0, 0);
        assertNull(noRegions.pageStore.regionPool);
        AdaptivePoolingAllocator noSource = new AdaptivePoolingAllocator(segments, true, segments, null,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT));
        assertNull(noSource.pageStore.regionPool);
        for (AdaptivePoolingAllocator allocator : new AdaptivePoolingAllocator[] {noRegions, noSource}) {
            HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
            Segment segment = heap.claim(10);
            assertNull(segment.region);
            assertEquals(SEGMENT_SIZE, allocator.usedMemory());
            giveBack(heap, segment, 0, 10);
            assertEquals(0, allocator.usedMemory());
        }
        assertEquals(2, segments.segmentsAllocated());
        assertEquals(0, regions.regions.size());
    }
}
