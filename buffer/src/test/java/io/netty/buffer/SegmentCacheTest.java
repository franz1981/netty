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

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The {@link SegmentCache}: its bound (0 included), newest-first takes, the ageing of cold segments by the configured
 * interval and fraction, the close, and the accounting of segments wherever they are.
 */
final class SegmentCacheTest {
    /**
     * The cache keeps up to its bound and frees a segment beyond it at once; every segment is in the used memory from
     * its allocation to its free, wherever it is.
     */
    @Test
    void cacheIsBoundedAndSegmentsAreAccountedWherever() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 2 * SEGMENT_SIZE);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
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
        SegmentCache cache = allocator.pageStore.segmentCache;
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
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        List<Segment> segments = new ArrayList<Segment>();
        for (int i = 0; i < 5; i++) {
            segments.add(heap.claim(64 - 1));
        }
        for (Segment segment : segments) {
            heap.release(segment, 0, 63);
        }
        SegmentCache cache = allocator.pageStore.segmentCache;
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
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment a = heap.claim(10);
        Segment b = heap.claim(60);
        heap.release(a, 0, 10);
        assertEquals(1, allocator.pageStore.segmentCache.size());
        allocator.pageStore.segmentCache.close();
        assertEquals(0, allocator.pageStore.segmentCache.size());
        heap.release(b, 0, 60);
        assertEquals(0, source.segmentsLive());
        assertEquals(0, allocator.usedMemory());
    }

    /** A cache of 0 bytes keeps nothing: a segment that empties is given back at once. */
    @Test
    void noCacheGivesAWhollyFreeSegmentBackAtOnce() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 0);
        assertEquals(0, allocator.pageStore.segmentCache.capacity());
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        Segment a = heap.claim(10);
        assertEquals(SEGMENT_SIZE, allocator.usedMemory());
        heap.release(a, 0, 10);
        assertEquals(0, a.buffer.refCnt());
        assertEquals(0, allocator.pageStore.segmentCache.size());
        assertEquals(1, allocator.pageStore.segmentCache.freed);
        assertEquals(0, allocator.usedMemory());
        assertAccounted(source, allocator);
    }

    /** The cache's interval and fraction are the allocator's: 1 s and all of them frees every cold segment at once. */
    @Test
    void cacheAgesByTheInstanceIntervalAndFraction() {
        CountingSegmentSource source = new CountingSegmentSource();
        long second = 1000L * 1000 * 1000;
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, 64 * 1024 * 1024, second, 1));
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        List<Segment> segments = new ArrayList<Segment>();
        for (int i = 0; i < 3; i++) {
            segments.add(heap.claim(63));
        }
        for (Segment segment : segments) {
            heap.release(segment, 0, 63);
        }
        SegmentCache cache = allocator.pageStore.segmentCache;
        assertEquals(3, cache.size());
        long now = System.nanoTime() + second;
        heap.decay(now); // starts the interval in which the three are cold
        assertEquals(3, cache.size());
        heap.decay(now + second / 2);
        assertEquals(3, cache.size(), "not an interval later");
        heap.decay(now + second);
        assertEquals(0, cache.size(), "all of the cold ones");
        assertEquals(0, allocator.usedMemory());
        assertAccounted(source, allocator);
    }

    /** Several decays in one interval age the cache once: only the first after the interval runs. */
    @Test
    void oneAgeingPerInterval() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * MIB);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        List<Segment> segments = new ArrayList<Segment>();
        for (int i = 0; i < 4; i++) {
            segments.add(heap.claim(63));
        }
        for (Segment segment : segments) {
            heap.release(segment, 0, 63);
        }
        SegmentCache cache = allocator.pageStore.segmentCache;
        long now = System.nanoTime() + INTERVAL;
        cache.decayIfDue(now);
        cache.decayIfDue(now + INTERVAL);
        assertEquals(2, cache.size());
        cache.decayIfDue(now + INTERVAL);
        cache.decayIfDue(now + INTERVAL + INTERVAL / 2);
        assertEquals(2, cache.size());
        assertEquals(2, cache.freed);
        assertAccounted(source, allocator);
    }
}
