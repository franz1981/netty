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

import java.util.Random;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The packing rule of {@link HeapSegments} (fullest segment with room first, oldest on a tie, then the reserve, a new
 * one only when none fits), the reserve of wholly free segments and its ageing, and the hand-over after the heap is
 * freed.
 */
final class HeapSegmentsTest {
    /**
     * A span comes from the fullest segment with a run long enough; a segment with enough free slices but no run long
     * enough is passed over; a new segment is taken only when none fits.
     */
    @Test
    void claimsFromTheFullestSegmentWithRoom() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment a = heap.claim(60);
        assertEquals(0, heap.claimedStart());
        Segment b = heap.claim(10);
        assertNotSame(a, b, "4 slices left in a");
        assertSame(b, heap.claim(40), "b has room");
        assertEquals(10, heap.claimedStart());
        assertEquals(2, source.segmentsAllocated());
        // a: 60 used, b: 50 used. Both fit 2: a is the fuller.
        assertSame(a, heap.claim(2));
        assertEquals(60, heap.claimedStart());
        // a: 2 free. Room for 3 only in b.
        assertSame(b, heap.claim(3));
        assertEquals(50, heap.claimedStart());
        assertEquals(2, source.segmentsAllocated());

        // x: eight spans of 8, then three of them freed: 24 free slices in runs of 8.
        HeapSegments fragmented = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment x = fragmented.claim(8);
        for (int i = 1; i < 8; i++) {
            assertSame(x, fragmented.claim(8));
            assertEquals(8 * i, fragmented.claimedStart());
        }
        fragmented.release(x, 8, 8);
        fragmented.release(x, 24, 8);
        fragmented.release(x, 40, 8);
        assertEquals(24, x.freeSlices());
        Segment y = fragmented.claim(9);
        assertNotSame(x, y, "24 free slices in x, but no run of 9");
        assertEquals(4, source.segmentsAllocated(), "a, b, x, y");
        // x (40 used) is fuller than y (9 used) and has runs of 8: the lowest one.
        assertSame(x, fragmented.claim(8));
        assertEquals(8, fragmented.claimedStart());
        assertAccounted(source, allocator);
    }

    /**
     * Equally full segments: the oldest wins. A wholly free segment leaves the list for the heap's reserve, which a
     * claim that fits no segment takes from before the store.
     */
    @Test
    void oldestOfEquallyFullSegmentsWinsAndEmptySegmentsBecomeTheSpare() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment a = heap.claim(40);
        Segment b = heap.claim(40);
        assertNotSame(a, b);
        Segment c = heap.claim(40);
        assertNotSame(b, c);
        assertEquals(3, heap.count);
        // a and b hold 40 each, c 40: all equally full, a is the oldest.
        assertSame(a, heap.claim(8));
        assertEquals(40, heap.claimedStart());
        // Now b and c are equally full and less full than a (48): a still fits 8 more and is the fullest.
        assertSame(a, heap.claim(8));
        // a: 56 used, 8 free; b, c: 40 used. Room for 9: b, the older of the two.
        assertSame(b, heap.claim(9));
        heap.release(c, 0, 40);
        assertEquals(2, heap.count, "c left the list");
        assertSame(c, heap.reserve[0]);
        assertEquals(1, heap.reserved);
        assertSame(heap, c.owner, "a reserved segment is still the heap's");
        // No room for 32 in a (8 free) nor b (15 free): the reserved one, before the store.
        assertSame(c, heap.claim(32));
        assertEquals(0, heap.reserved);
        assertEquals(3, heap.count);
        assertEquals(3, source.segmentsAllocated());
        assertAccounted(source, allocator);
    }

    /**
     * Without regions, a segment that empties stays whole in the heap's reserve, up to 8 of 4 MiB: taken again, newest
     * first, it is the same segment with the same span buffers, and nothing is allocated. A full reserve gives back
     * its oldest.
     */
    @Test
    void reservedSegmentsAreReusedWhole() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        assertEquals(8, allocator.pageStore.reserveLimit());
        Segment[] taken = new Segment[10];
        AbstractByteBuf[] spans = new AbstractByteBuf[taken.length];
        for (int i = 0; i < taken.length; i++) {
            taken[i] = heap.claim(63);
            spans[i] = taken[i].span(source, 0, 63);
        }
        for (Segment segment : taken) {
            heap.release(segment, 0, 63);
        }
        assertEquals(8, heap.reserved);
        assertNull(taken[0].owner, "the oldest two made room");
        assertNull(taken[1].owner);
        assertEquals(8, source.segmentsLive());
        assertAccounted(source, allocator);
        for (int i = taken.length - 1; i >= 2; i--) {
            assertSame(taken[i], heap.claim(63), "newest first");
            assertSame(spans[i], taken[i].span(source, 0, 63), "the span buffer is reused");
        }
        assertEquals(0, heap.reserved);
        assertEquals(10, source.segmentsAllocated(), "nothing allocated for the reused ones");
        assertNotSame(taken[0], heap.claim(63));
        assertEquals(11, source.segmentsAllocated(), "an empty reserve: the store");
        assertAccounted(source, allocator);
    }

    /**
     * An evacuee that empties and goes back to the store leaves its mark behind: a segment in the store, or taken by
     * another heap since, is not this heap's to mark or unmark.
     */
    @Test
    void anEvacueeGivenBackKeepsNoMarkOfThisHeap() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(SEGMENT_SIZE, PageStoreConfig.SLICE_SIZE_BYTES, INTERVAL, 1));
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment full = heap.claim(60);
        Segment sparse = heap.claim(60);
        Segment other = heap.claim(60);
        assertEquals(3, heap.count);
        assertNotSame(full, sparse);
        assertNotSame(sparse, other);
        // Two sparse segments: 4 slices used each, by chunks without buffers.
        heap.release(sparse, 4, 56);
        heap.release(other, 4, 56);
        sparse.movableSlices = sparse.usedSlices();
        other.movableSlices = other.usedSlices();
        assertTrue(heap.markEvacuees());
        assertTrue(sparse.evacuate);
        // Its chunk freed, the evacuee goes to the reserve; the next one emptied pushes it out to the store.
        heap.release(sparse, 0, 4);
        assertSame(sparse, heap.reserve[0]);
        heap.release(other, 0, 4);
        assertNull(sparse.owner);
        assertFalse(sparse.evacuate, "unmarked when given back");
        // Another heap takes and marks it before this heap's decay ends.
        HeapSegments otherHeap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment.OWNER.set(sparse, otherHeap);
        sparse.evacuate = true;
        heap.clearEvacuees();
        assertTrue(sparse.evacuate, "the other heap's mark");
    }

    /**
     * Each decay gives back half, rounded up, of the reserved segments that stayed unused since the previous one,
     * oldest first. Taking one makes the cold count no larger than what is left; the ones reserved since are not cold.
     */
    @Test
    void decaysGiveBackHalfTheColdReservedSegments() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment[] taken = new Segment[9];
        for (int i = 0; i < taken.length; i++) {
            taken[i] = heap.claim(63);
        }
        for (int i = 0; i < 8; i++) {
            heap.release(taken[i], 0, 63);
        }
        long now = System.nanoTime();
        heap.decay(now += INTERVAL);
        assertEquals(8, heap.reserved, "none was reserved through a whole interval");
        heap.decay(now += INTERVAL);
        assertEquals(4, heap.reserved, "half of 8");
        for (int i = 0; i < 4; i++) {
            assertNull(taken[i].owner, "the oldest went back: " + i);
        }
        assertSame(taken[7], heap.claim(63));
        heap.decay(now += INTERVAL);
        assertEquals(1, heap.reserved, "half of the 3 cold ones left, rounded up");
        assertSame(taken[6], heap.reserve[0]);
        heap.release(taken[7], 0, 63);
        heap.decay(now += INTERVAL);
        assertEquals(1, heap.reserved, "the cold one went back, not the one reserved since");
        assertNull(taken[6].owner);
        assertSame(taken[7], heap.reserve[0]);
        heap.decay(now += INTERVAL);
        assertEquals(0, heap.reserved);
        assertEquals(1, source.segmentsLive(), "the one still in use");
        assertAccounted(source, allocator);
        heap.release(taken[8], 0, 63);
        heap.markFreed();
        heap.afterFree();
        assertEquals(0, heap.reserved, "a freed heap gives its reserve back");
        assertEquals(0, source.segmentsLive());
        assertEquals(0, allocator.usedMemory());
    }

    /**
     * With regions, the reserve is one segment, which behaves as the heap's one spare: the newest wholly free
     * segment is kept and the previous one goes back to its region; it goes back when a decay finds it already seen
     * by the previous one, and one taken meanwhile starts over.
     */
    @Test
    void withRegionsTheReserveIsOneSpare() {
        CountingRegionSource regions = new CountingRegionSource();
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, regions, REGION_SIZE, REGION_ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        assertEquals(1, allocator.pageStore.reserveLimit());
        Segment a = heap.claim(63);
        Segment b = heap.claim(63);
        Segment c = heap.claim(63);
        heap.release(a, 0, 63);
        assertSame(a, heap.reserve[0]);
        heap.release(b, 0, 63);
        assertEquals(1, heap.reserved);
        assertSame(b, heap.reserve[0], "the newest is kept");
        assertNull(a.owner);
        assertEquals(REGION_SIZE / SEGMENT_SIZE - 2, a.region.freeSlotCount(), "a went back to its region");
        long now = System.nanoTime();
        heap.decay(now);
        assertSame(b, heap.reserve[0], "seen once");
        assertSame(b, heap.claim(63), "taken: its age starts over");
        heap.release(b, 0, 63);
        heap.decay(now += INTERVAL);
        assertSame(b, heap.reserve[0]);
        heap.decay(now += INTERVAL);
        assertEquals(0, heap.reserved, "unused a whole interval");
        assertNull(b.owner);
        heap.release(c, 0, 63);
        heap.markFreed();
        heap.afterFree();
        assertEquals(0, heap.reserved);
        assertEquals(REGION_SIZE / SEGMENT_SIZE, a.region.freeSlotCount());
        assertEquals(0, source.segmentsAllocated());
        assertAccounted(source, regions, allocator);
    }

    /**
     * Once its heap is freed, a segment goes back to its source with the release that empties it, from any thread,
     * exactly once: {@link HeapSegments#afterFree} and the last release may both see it wholly free.
     */
    @Test
    void segmentsOfAFreedHeapGoWithTheirLastSpan() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        final Segment a = heap.claim(10);
        final Segment b = heap.claim(50);
        final int bStart = heap.claimedStart();
        assertSame(a, b);
        Segment c = heap.claim(60);
        assertNotSame(a, c);
        heap.markFreed();
        heap.release(c, 0, 60);
        assertNull(c.owner, "emptied after the heap was freed: given back, not kept");
        assertEquals(0, heap.reserved);
        assertEquals(1, source.segmentsLive());
        heap.afterFree();
        assertEquals(0, heap.count);
        assertEquals(1, source.segmentsLive(), "c is not given back twice");
        assertSame(heap, a.owner, "a still has spans out");
        final HeapSegments dead = heap;
        Thread t = new Thread(() -> {
            dead.release(a, 0, 10);
            dead.release(b, bStart, 50);
        });
        t.start();
        t.join();
        assertEquals(0, source.segmentsLive());
        assertNull(a.owner);
        assertAccounted(source, allocator);
    }

    /**
     * Random claims and releases on one heap: no wholly free segment stays in its list, every segment in it is its
     * own, their used slices are exactly the live spans, and the rest of its segments are reserved, wholly free, up to
     * the limit. The accounting matches the
     * source at every step.
     */
    @Test
    void randomClaimsAndReleasesKeepOnlyUsedSegments() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment[] segments = new Segment[128];
        int[] starts = new int[128];
        int[] lengths = new int[128];
        int live = 0;
        long liveSlices = 0;
        Random random = new Random(7);
        for (int op = 0; op < 20_000; op++) {
            if (live == segments.length || live > 0 && random.nextBoolean()) {
                int k = random.nextInt(live);
                heap.release(segments[k], starts[k], lengths[k]);
                liveSlices -= lengths[k];
                live--;
                segments[k] = segments[live];
                starts[k] = starts[live];
                lengths[k] = lengths[live];
                segments[live] = null;
            } else {
                int n = 1 + random.nextInt(9);
                segments[live] = heap.claim(n);
                starts[live] = heap.claimedStart();
                lengths[live] = n;
                live++;
                liveSlices += n;
            }
            long used = 0;
            for (int i = 0; i < heap.count; i++) {
                Segment segment = heap.segments[i];
                assertFalse(segment.isWhollyFree(), "op " + op);
                assertSame(heap, segment.owner);
                used += segment.usedSlices();
            }
            assertEquals(liveSlices, used, "op " + op);
            assertTrue(heap.reserved <= allocator.pageStore.reserveLimit());
            for (int i = 0; i < heap.reserved; i++) {
                assertTrue(heap.reserve[i].isWhollyFree());
                assertSame(heap, heap.reserve[i].owner);
            }
            assertEquals(heap.count + heap.reserved, source.segmentsLive(), "op " + op);
            assertAccounted(source, allocator);
        }
    }
}
