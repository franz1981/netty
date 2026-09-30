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

import io.netty.buffer.AdaptivePoolingAllocator.HeapSegments;
import io.netty.buffer.AdaptivePoolingAllocator.IdleDecay;
import io.netty.buffer.AdaptivePoolingAllocator.Segment;
import io.netty.buffer.AdaptivePoolingAllocator.SegmentCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.AdaptivePoolingAllocator.SLICE_SHIFT;
import static io.netty.buffer.AdaptivePoolingAllocator.SLICE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The page store of a direct {@link AdaptivePoolingAllocator}: the slice map of a {@link Segment}, the packing rule
 * of {@link HeapSegments}, the bound and the aging of the {@link SegmentCache}, and the accounting of segments.
 */
public class AdaptiveSegmentsTest {
    static final int SEGMENT_SIZE = 4 * 1024 * 1024;
    private static final long INTERVAL = IdleDecay.DECAY_INTERVAL_NANOS;

    /**
     * Direct segments counted as they are allocated and freed; also the chunk allocator of the allocator under test,
     * for the chunks that are not carved from segments.
     */
    static final class CountingSegmentSource
            implements AdaptivePoolingAllocator.SegmentSource, AdaptivePoolingAllocator.ChunkAllocator {
        final List<AbstractByteBuf> segments = new ArrayList<AbstractByteBuf>();
        final List<AbstractByteBuf> chunks = new ArrayList<AbstractByteBuf>();

        @Override
        public synchronized AbstractByteBuf allocateSegment(int size) {
            AbstractByteBuf buf = UnsafeByteBufUtil.newDirectByteBuf(UnpooledByteBufAllocator.DEFAULT, size, size);
            segments.add(buf);
            return buf;
        }

        @Override
        public AbstractByteBuf span(AbstractByteBuf segment, int offset, int length) {
            return AdaptiveByteBufAllocator.directSpan(UnpooledByteBufAllocator.DEFAULT, segment, offset, length);
        }

        @Override
        public synchronized AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            AbstractByteBuf buf = UnsafeByteBufUtil.newDirectByteBuf(
                    UnpooledByteBufAllocator.DEFAULT, initialCapacity, maxCapacity);
            chunks.add(buf);
            return buf;
        }

        synchronized int segmentsAllocated() {
            return segments.size();
        }

        synchronized int segmentsLive() {
            int live = 0;
            for (AbstractByteBuf buf : segments) {
                if (buf.refCnt() > 0) {
                    live++;
                }
            }
            return live;
        }

        /** What the allocator holds, seen from outside: every buffer handed out and not released. */
        synchronized long unreleasedBytes() {
            long bytes = 0;
            for (AbstractByteBuf buf : segments) {
                bytes += buf.refCnt() > 0 ? buf.capacity() : 0;
            }
            for (AbstractByteBuf buf : chunks) {
                bytes += buf.refCnt() > 0 ? buf.capacity() : 0;
            }
            return bytes;
        }
    }

    static AdaptivePoolingAllocator newAllocator(CountingSegmentSource source, int segmentSize, int cacheBytes) {
        return new AdaptivePoolingAllocator(source, true, source, segmentSize, cacheBytes);
    }

    static void assertAccounted(CountingSegmentSource source, AdaptivePoolingAllocator allocator) {
        assertEquals(source.unreleasedBytes(), allocator.usedMemory(), "usedMemory() and the segment source disagree");
    }

    private static Segment segment(int slices) {
        return new Segment((AbstractByteBuf) Unpooled.buffer(slices << SLICE_SHIFT), slices);
    }

    /** Every length and every start of a lone span in an empty segment, both segment sizes. */
    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    void claimsTheLowestRunOfEveryLength(int slices) {
        Segment segment = segment(slices);
        for (int n = 1; n <= 32; n++) {
            assertEquals(0, segment.claim(n), "n=" + n);
            assertEquals(slices - n, segment.freeSlices());
            segment.release(0, n);
            assertTrue(segment.isWhollyFree());
            // Every start: occupy the slices below it, so the lowest run of n is there.
            for (int start = 1; start + n <= slices; start++) {
                assertEquals(0, segment.claim(start));
                assertEquals(start, segment.claim(n), "n=" + n + " start=" + start);
                segment.release(0, start);
                segment.release(start, n);
                assertTrue(segment.isWhollyFree());
            }
        }
    }

    /** A run never reaches past the last slice; a full segment and a too long request claim nothing. */
    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    void claimsNothingPastTheLastSlice(int slices) {
        Segment segment = segment(slices);
        assertEquals(0, segment.claim(slices - 3));
        assertEquals(-1, segment.claim(4), "3 slices left");
        assertEquals(slices - 3, segment.claim(3));
        assertEquals(0, segment.freeSlices());
        assertEquals(-1, segment.claim(1));
        segment.release(slices - 3, 3);
        assertEquals(slices - 1, segment.claim(1) + 2, "lowest of the three at the top");
    }

    /** Freed spans merge with their free neighbours: a longer run fits where three spans were. */
    @Test
    void freedSpansCoalesce() {
        Segment segment = segment(64);
        assertEquals(0, segment.claim(2));
        assertEquals(2, segment.claim(3));
        assertEquals(5, segment.claim(2));
        assertEquals(7, segment.claim(57));
        assertEquals(-1, segment.claim(4));
        segment.release(0, 2);
        assertEquals(-1, segment.claim(4), "two free slices only");
        segment.release(5, 2);
        assertEquals(-1, segment.claim(4), "two runs of two, not adjacent");
        segment.release(2, 3);
        assertEquals(0, segment.claim(7), "0..6 merged");
        segment.release(0, 7);
        segment.release(7, 57);
        assertTrue(segment.isWhollyFree());
    }

    @Test
    void releasingFreeSlicesThrows() {
        Segment segment = segment(64);
        assertEquals(0, segment.claim(4));
        segment.release(0, 4);
        assertThrows(IllegalStateException.class, () -> segment.release(0, 4));
        assertThrows(IllegalStateException.class, () -> segment.release(2, 1));
    }

    /**
     * A span comes from the fullest segment with a run long enough; a segment with enough free slices but no run long
     * enough is passed over; a new segment is taken only when none fits.
     */
    @Test
    void claimsFromTheFullestSegmentWithRoom() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 0);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
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
        HeapSegments fragmented = new HeapSegments(allocator, null, Thread.currentThread());
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
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
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
        assertEquals(1, allocator.segmentCache.size());
        // A heap taking a segment takes c from the cache: no new one is allocated.
        HeapSegments other = new HeapSegments(allocator, null, Thread.currentThread());
        assertSame(c, other.claim(32));
        assertSame(other, c.owner);
        assertEquals(3, source.segmentsAllocated());
        assertAccounted(source, allocator);
    }

    /**
     * The cache keeps up to its bound and frees a segment beyond it at once; every segment is in the used memory from
     * its allocation to its free, wherever it is.
     */
    @Test
    void cacheIsBoundedAndSegmentsAreAccountedWherever() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 2 * SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        List<Segment> segments = new ArrayList<Segment>();
        for (int i = 0; i < 4; i++) {
            segments.add(heap.claim(64 - 1));
        }
        assertEquals(4, source.segmentsAllocated());
        assertEquals(4L * SEGMENT_SIZE, allocator.usedMemory());
        for (Segment segment : segments) {
            heap.release(segment, 0, 63);
            assertAccounted(source, allocator);
        }
        SegmentCache cache = allocator.segmentCache;
        assertEquals(2, cache.size());
        assertEquals(2, source.segmentsLive());
        assertEquals(2L * SEGMENT_SIZE, allocator.usedMemory());
        assertEquals(2, cache.returned);
        assertEquals(2, cache.freed);
        // Newest first.
        assertSame(segments.get(1), heap.claim(1));
        assertEquals(1, cache.taken);
        assertAccounted(source, allocator);
    }

    /**
     * The cache ages at most once per interval, driven by any heap's decay: each time it frees half, rounded up, of
     * the segments that stayed in it through the whole interval, oldest first; a segment taken and given back starts
     * over. Decays are forced with a clock that advances by an interval each time.
     */
    @Test
    void cacheFreesHalfOfItsColdSegmentsPerInterval() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        List<Segment> segments = new ArrayList<Segment>();
        for (int i = 0; i < 5; i++) {
            segments.add(heap.claim(64 - 1));
        }
        for (Segment segment : segments) {
            heap.release(segment, 0, 63);
        }
        SegmentCache cache = allocator.segmentCache;
        assertEquals(5, cache.size());
        long now = System.nanoTime() + INTERVAL;
        // The first decay starts the interval in which the five are candidates: they arrived during the last one.
        heap.decay(now);
        assertEquals(5, cache.size());
        // Not an interval later: nothing happens.
        heap.decay(now + INTERVAL / 2);
        assertEquals(5, cache.size());
        now += INTERVAL;
        heap.decay(now);
        assertEquals(2, cache.size(), "half of 5 cold, rounded up, freed");
        for (int i = 0; i < 5; i++) {
            assertEquals(i < 3 ? 0 : 1, segments.get(i).buffer.refCnt(), "the oldest three are freed: " + i);
        }
        assertAccounted(source, allocator);
        // Take the newest and give it back: it starts over, only the other one is cold.
        Segment taken = heap.claim(63);
        assertSame(segments.get(4), taken);
        heap.release(taken, 0, 63);
        now += INTERVAL;
        heap.decay(now);
        assertEquals(1, cache.size());
        assertEquals(0, segments.get(3).buffer.refCnt());
        assertSame(taken, cache.poll());
        assertAccounted(source, allocator);
    }

    @Test
    void closedCacheFreesEverything() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        Segment a = heap.claim(10);
        Segment b = heap.claim(60);
        heap.release(a, 0, 10);
        assertEquals(1, allocator.segmentCache.size());
        allocator.segmentCache.close();
        assertEquals(0, allocator.segmentCache.size());
        heap.release(b, 0, 60);
        assertEquals(0, source.segmentsLive());
        assertEquals(0, allocator.usedMemory());
    }

    /**
     * Once its heap is freed, a segment goes to the cache with the release that empties it, from any thread, exactly
     * once: {@link HeapSegments#afterFree} and the last release may both see it wholly free.
     */
    @Test
    void segmentsOfAFreedHeapGoWithTheirLastSpan() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        final Segment a = heap.claim(10);
        final Segment b = heap.claim(50);
        final int bStart = heap.claimedStart();
        assertSame(a, b);
        Segment c = heap.claim(60);
        assertNotSame(a, c);
        heap.markFreed();
        heap.release(c, 0, 60);
        assertEquals(1, allocator.segmentCache.size(), "emptied after the heap was freed");
        heap.afterFree();
        assertEquals(0, heap.count);
        assertEquals(1, allocator.segmentCache.size(), "c is not offered twice");
        assertSame(heap, a.owner, "a still has spans out");
        final HeapSegments dead = heap;
        Thread t = new Thread(() -> {
            dead.release(a, 0, 10);
            dead.release(b, bStart, 50);
        });
        t.start();
        t.join();
        assertEquals(2, allocator.segmentCache.size());
        assertNull(a.owner);
        assertAccounted(source, allocator);
    }

    @Test
    void usedAndFreeSlicesAddUp() {
        Segment segment = segment(64);
        segment.claim(9);
        segment.claim(2);
        assertEquals(11, segment.usedSlices());
        assertEquals(53, segment.freeSlices());
        assertEquals(64L * SLICE_SIZE, segment.capacity());
    }
}
