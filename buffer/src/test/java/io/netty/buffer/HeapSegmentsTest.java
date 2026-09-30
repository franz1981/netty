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

import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The packing rule of {@link HeapSegments} (fullest segment with room first, oldest on a tie, a new one only when none
 * fits), segments leaving the heap once wholly free, and the hand-over after the heap is freed.
 */
final class HeapSegmentsTest {
    /**
     * A span comes from the fullest segment with a run long enough; a segment with enough free slices but no run long
     * enough is passed over; a new segment is taken only when none fits.
     */
    @Test
    void claimsFromTheFullestSegmentWithRoom() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 0);
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

    /** Equally full segments: the oldest wins. A wholly free segment leaves the heap for the cache. */
    @Test
    void oldestOfEquallyFullSegmentsWinsAndEmptySegmentsLeave() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 2 * SEGMENT_SIZE);
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
        assertEquals(2, heap.count, "c left the heap");
        assertNull(c.owner);
        assertEquals(1, allocator.pageStore.segmentCache.size());
        // A heap taking a segment takes c from the cache: no new one is allocated.
        HeapSegments other = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        assertSame(c, other.claim(32));
        assertSame(other, c.owner);
        assertEquals(3, source.segmentsAllocated());
        assertAccounted(source, allocator);
    }

    /**
     * Once its heap is freed, a segment goes to the cache with the release that empties it, from any thread, exactly
     * once: {@link HeapSegments#afterFree} and the last release may both see it wholly free.
     */
    @Test
    void segmentsOfAFreedHeapGoWithTheirLastSpan() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        final Segment a = heap.claim(10);
        final Segment b = heap.claim(50);
        final int bStart = heap.claimedStart();
        assertSame(a, b);
        Segment c = heap.claim(60);
        assertNotSame(a, c);
        heap.markFreed();
        heap.release(c, 0, 60);
        assertEquals(1, allocator.pageStore.segmentCache.size(), "emptied after the heap was freed");
        heap.afterFree();
        assertEquals(0, heap.count);
        assertEquals(1, allocator.pageStore.segmentCache.size(), "c is not offered twice");
        assertSame(heap, a.owner, "a still has spans out");
        final HeapSegments dead = heap;
        Thread t = new Thread(() -> {
            dead.release(a, 0, 10);
            dead.release(b, bStart, 50);
        });
        t.start();
        t.join();
        assertEquals(2, allocator.pageStore.segmentCache.size());
        assertNull(a.owner);
        assertAccounted(source, allocator);
    }

    /**
     * Random claims and releases on one heap: no wholly free segment stays in it, every segment in it is its own, and
     * their used slices are exactly the live spans. The accounting matches the source at every step.
     */
    @Test
    void randomClaimsAndReleasesKeepOnlyUsedSegments() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 4 * SEGMENT_SIZE);
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
            assertAccounted(source, allocator);
        }
    }
}
