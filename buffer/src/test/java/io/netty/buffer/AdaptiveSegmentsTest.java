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

import io.netty.buffer.AdaptivePoolingAllocator.AdaptiveByteBuf;
import io.netty.buffer.AdaptivePoolingAllocator.IdleDecay;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.StampedLock;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

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
            implements SegmentSource, AdaptivePoolingAllocator.ChunkAllocator {
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
        return new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(segmentSize, SLICE_SIZE, cacheBytes, INTERVAL, 0.5));
    }

    static void assertAccounted(CountingSegmentSource source, AdaptivePoolingAllocator allocator) {
        assertEquals(source.unreleasedBytes(), allocator.usedMemory(), "usedMemory() and the segment source disagree");
    }

    private static Segment segment(int slices) {
        return new Segment((AbstractByteBuf) Unpooled.buffer(slices * SLICE_SIZE), SLICE_SIZE);
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

    // ---- Size-class chunks as spans (the allocator's own heaps) ----

    private static final int[] SIZE_CLASSES = AdaptivePoolingAllocator.getSizeClasses();

    /** Slices of the chunk of each size class, as the page-store plan's table has them. */
    private static int expectedSlices(int sizeClass) {
        if (sizeClass <= 4096) {
            return 2;
        }
        switch (sizeClass) {
            case 4352: return 3;
            case 8192: return 4;
            case 8704: return 5;
            case 16896: case 33792: case 67584: case 135168: return 9;
            default: return 8; // 16384, 32768, 65536, 131072
        }
    }

    private static boolean isLowMemory() throws Exception {
        Field f = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        f.setAccessible(true);
        return f.getBoolean(null);
    }

    private static int pooledSizeClassesCount() throws Exception {
        Field f = AdaptivePoolingAllocator.class.getDeclaredField("POOLED_SIZE_CLASSES_COUNT");
        f.setAccessible(true);
        return f.getInt(null);
    }

    static Object field(Object o, String name) throws Exception {
        for (Class<?> c = o.getClass(); c != null; c = c.getSuperclass()) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(o);
            } catch (NoSuchFieldException ignore) {
                // the superclass
            }
        }
        throw new NoSuchFieldException(name);
    }

    /** The stripe with size-class magazines: the one the calling (plain) thread allocated from. */
    private static Object usedStripe(AdaptivePoolingAllocator allocator) throws Exception {
        Object found = null;
        for (Object stripe : (Object[]) field(allocator, "stripedHeaps")) {
            if (field(stripe, "magazines") != null) {
                assertNull(found, "one stripe only");
                found = stripe;
            }
        }
        assertNotNull(found);
        return found;
    }

    /** The calling thread's thread-local heap. */
    private static Object threadLocalHeap(AdaptivePoolingAllocator allocator) throws Exception {
        FastThreadLocal<?> ftl = (FastThreadLocal<?>) field(allocator, "threadLocalSizeClassHeap");
        return ftl.get();
    }

    private static IdleDecay idleDecay(Object heap) throws Exception {
        return (IdleDecay) field(heap, "idleDecay");
    }

    /** Run a forced decay of a stripe under its lock, as its allocations would. */
    private static void decayStripe(Object stripe, long now) throws Exception {
        StampedLock lock = (StampedLock) field(stripe, "lock");
        long stamp = lock.writeLock();
        try {
            idleDecay(stripe).decay(now);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private static SizeClassedChunk chunkOf(ByteBuf buf) {
        ByteBuf adaptive = buf instanceof AdaptiveByteBuf ? buf : buf.unwrap(); // under a leak-aware wrapper
        return (SizeClassedChunk) ((AdaptiveByteBuf) adaptive).chunk;
    }

    private static <T> T onFastThreadLocalThread(Callable<T> task) throws Exception {
        FutureTask<T> future = new FutureTask<T>(task);
        Thread thread = new FastThreadLocalThread(future);
        thread.start();
        T result = future.get();
        thread.join();
        return result;
    }

    /**
     * Every pooled size class gets a chunk that is a span of a segment of its heap, of the size of the plan's table,
     * holding as many segments as fit; the buffer's memory lies inside the segment, at the span's offset. No chunk
     * buffer is allocated on its own, and the used memory is the segments, whole.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void directSizeClassChunksAreSpansOfSegments(final boolean threadLocal) throws Exception {
        assumeFalse(isLowMemory() && threadLocal, "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource();
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        final int pooled = pooledSizeClassesCount();
        Callable<List<ByteBuf>> work = () -> {
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < pooled; i++) {
                int size = SIZE_CLASSES[i];
                ByteBuf buf = allocator.allocate(size, size);
                bufs.add(buf);
                SizeClassedChunk chunk = chunkOf(buf);
                assertNotNull(chunk.segment, "size " + size);
                assertEquals(expectedSlices(size) * SLICE_SIZE, chunk.capacity(), "chunk of size " + size);
                assertEquals(chunk.capacity() / size, field(chunk, "segments"), "segments of size " + size);
                long base = chunk.segment.memoryAddress();
                long address = buf.memoryAddress();
                assertTrue(address >= base + (long) chunk.spanStart * SLICE_SIZE
                        && address + size <= base + ((long) chunk.spanStart + expectedSlices(size)) * SLICE_SIZE,
                        "size " + size + " outside its span");
                buf.setLong(0, 0x0123456789ABCDEFL);
                assertEquals(0x0123456789ABCDEFL, buf.getLong(0));
            }
            return bufs;
        };
        List<ByteBuf> bufs = threadLocal ? onFastThreadLocalThread(work) : work.call();
        assertTrue(source.chunks.isEmpty(), "no chunk buffer of its own");
        assertEquals((long) source.segmentsAllocated() * SEGMENT_SIZE, allocator.usedMemory());
        assertAccounted(source, allocator);
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }

    /** Heap buffers are untouched: their chunks are allocated one by one, at the sizes of old. */
    @Test
    void heapBuffersKeepTheirChunks() throws Exception {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, false);
        AdaptivePoolingAllocator heap = (AdaptivePoolingAllocator) field(allocator, "heap");
        assertNull(heap.segmentSource);
        assertNull(heap.segmentCache);
        ByteBuf small = allocator.heapBuffer(1024, 1024);
        assertEquals(AdaptivePoolingAllocator.MIN_CHUNK_SIZE, allocator.metric().usedHeapMemory());
        ByteBuf headered = allocator.heapBuffer(4352, 4352);
        assertEquals(AdaptivePoolingAllocator.MIN_CHUNK_SIZE + 136 * 1024, allocator.metric().usedHeapMemory());
        assertNull(chunkOf(headered).segment);
        assertEquals(0, allocator.metric().usedDirectMemory());
        small.release();
        headered.release();
    }

    /**
     * Chunks given up by one size class free their spans, which other classes of the same heap reuse, of any chunk
     * size: no new segment. Then decays give everything back: an idle class gives up its chunks, the emptied segments
     * leave the heap for the cache, and the cache frees half of its cold segments per interval, down to nothing.
     */
    @Test
    void spansAreReusedAcrossClassesAndDecaysGiveSegmentsBack() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        int size = isLowMemory() ? 16384 : 65536; // 8-slice chunks either way
        int perChunk = 8 * SLICE_SIZE / size;
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 10 * perChunk; i++) {
            bufs.add(allocator.allocate(size, size));
        }
        // 10 chunks of 8 slices: the first segment is filled (8 of them), then a second one.
        assertEquals(2, source.segmentsAllocated());
        Object stripe = usedStripe(allocator);
        HeapSegments heapSegments = idleDecay(stripe).heapSegments;
        assertEquals(2, heapSegments.count);
        assertEquals(64, heapSegments.segments[0].usedSlices());
        assertEquals(16, heapSegments.segments[1].usedSlices());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        assertAccounted(source, allocator);
        // Every chunk ran out of segments, so none is active; the class keeps the last one to empty (its floor), in
        // the second segment. The first segment emptied and went to the cache.
        assertEquals(1, heapSegments.count);
        assertEquals(8, heapSegments.segments[0].usedSlices());
        assertEquals(1, allocator.segmentCache.size());
        // Another class, another chunk size: from the free slices.
        int other = 1024; // 2-slice chunks
        for (int i = 0; i < 20 * (2 * SLICE_SIZE / other); i++) {
            bufs.add(allocator.allocate(other, other));
        }
        assertEquals(2, source.segmentsAllocated(), "40 slices fit in the free ones");
        assertEquals(1, heapSegments.count, "the fullest segment with room: the heap's own, not the cached one");
        assertEquals(1, allocator.segmentCache.size());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(source, allocator);

        // Decays, forced, one interval apart. Nothing allocates in between, so both classes go idle.
        long now = System.nanoTime();
        SegmentCache cache = allocator.segmentCache;
        for (int decay = 1; decay <= 8; decay++) {
            now += INTERVAL;
            decayStripe(stripe, now);
            assertAccounted(source, allocator);
            assertTrue(cache.size() <= cache.capacity());
        }
        assertEquals(0, heapSegments.count, "every segment left the heap");
        assertEquals(0, cache.size(), "and the cache gave them all back");
        assertEquals(2, cache.returned);
        assertEquals(0, allocator.usedMemory());
        assertEquals(2, source.segmentsAllocated());
    }

    /**
     * On a thread-local heap every other thread's release is a note. The chunks such releases empty are applied by
     * the owner's decays: an idle class gives them up, and the segment they emptied goes to the cache.
     */
    @Test
    void foreignReleasesEmptyASegmentThroughTheNotes() throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource();
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        final int size = 65536;
        final int perChunk = 8 * SLICE_SIZE / size;
        onFastThreadLocalThread(() -> {
            final List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < 3 * perChunk; i++) {
                bufs.add(allocator.allocate(size, size));
            }
            Object heap = threadLocalHeap(allocator);
            HeapSegments heapSegments = idleDecay(heap).heapSegments;
            assertEquals(1, heapSegments.count);
            Thread releaser = new Thread(() -> {
                for (ByteBuf buf : bufs) {
                    buf.release();
                }
            });
            releaser.start();
            releaser.join();
            // Nothing applied yet: the segment still holds its three spans.
            assertEquals(24, heapSegments.segments[0].usedSlices());
            assertEquals(0, allocator.segmentCache.size());
            long now = System.nanoTime();
            idleDecay(heap).decay(now + INTERVAL); // the class allocated since the last decay: not idle yet
            assertEquals(1, heapSegments.count);
            idleDecay(heap).decay(now + 2 * INTERVAL); // idle: its chunks, applied from the notes, are given up
            assertEquals(0, heapSegments.count);
            assertEquals(1, allocator.segmentCache.size());
            assertAccounted(source, allocator);
            return null;
        });
        assertEquals(1, source.segmentsAllocated());
        assertAccounted(source, allocator);
    }

    /**
     * A thread-local heap freed (its thread ended) while buffers are still out: its segments stay accounted, and the
     * release of the last buffer of a segment, on another thread, hands the segment to the cache.
     */
    @Test
    void segmentsOfAnEndedThreadGoToTheCacheWithTheirLastBuffer() throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource();
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 64 * 1024 * 1024);
        List<ByteBuf> bufs = onFastThreadLocalThread(() -> {
            List<ByteBuf> out = new ArrayList<ByteBuf>();
            for (int i = 0; i < 20; i++) {
                out.add(allocator.allocate(65536, 65536));
                out.add(allocator.allocate(1024, 1024));
            }
            // Some released before the thread ends.
            for (int i = 0; i < 10; i++) {
                out.remove(out.size() - 1).release();
            }
            return out;
        });
        assertEquals(1, source.segmentsAllocated());
        assertEquals(SEGMENT_SIZE, allocator.usedMemory());
        assertEquals(0, allocator.segmentCache.size(), "spans are still out");
        Segment segment = chunkOf(bufs.get(0)).segment;
        assertNotNull(segment.owner, "still the ended heap's");
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertTrue(segment.isWhollyFree());
        assertNull(segment.owner);
        assertEquals(1, allocator.segmentCache.size());
        assertAccounted(source, allocator);
    }

    /** 2 MiB segments: 32 slices, the same chunks, three 9-slice chunks per segment. */
    @Test
    void twoMebibyteSegments() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, 2 * 1024 * 1024, 8 * 1024 * 1024);
        int size = 16896;
        int perChunk = 9 * SLICE_SIZE / size; // 34
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 4 * perChunk; i++) {
            bufs.add(allocator.allocate(size, size));
        }
        assertEquals(2, source.segmentsAllocated(), "three 9-slice chunks per 32-slice segment");
        assertEquals(4L * 1024 * 1024, allocator.usedMemory());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(source, allocator);
    }

    /**
     * The defaults: 4 MiB segments, a 64 MiB cache and 36 MiB regions; 2 MiB, 8 MiB and no regions in low-memory mode,
     * where the single stripe carves its direct chunks out of segments too; heap buffers never. The memory accounted
     * is a region (with what aligning it took), or a segment without regions.
     */
    @Test
    void segmentDefaultsFollowTheMemoryMode() throws Exception {
        boolean lowMemory = isLowMemory();
        assumeFalse(System.getProperty("io.netty.allocator.segmentSize") != null
                || System.getProperty("io.netty.allocator.segmentCacheBytes") != null
                || System.getProperty("io.netty.allocator.segmentRegionSize") != null, "set explicitly");
        assertEquals(lowMemory ? 2 * 1024 * 1024 : 4 * 1024 * 1024, PageStoreConfig.SEGMENT_SIZE);
        assertEquals(lowMemory ? 8 * 1024 * 1024 : 64 * 1024 * 1024, PageStoreConfig.SEGMENT_CACHE_BYTES);
        assertEquals(lowMemory ? 0 : 36 * 1024 * 1024, PageStoreConfig.SEGMENT_REGION_SIZE);
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false);
        assertEquals(PageStoreConfig.SEGMENT_SIZE, AdaptiveByteBufAllocatorTest.directSegmentSize(allocator));
        boolean regions = !lowMemory && PlatformDependent.directAllocationLeavesMemoryUntouched();
        assertEquals(regions, AdaptiveByteBufAllocatorTest.directRegions(allocator));
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        long unit = AdaptiveByteBufAllocatorTest.directPageStoreUnit(allocator);
        if (!regions) {
            assertEquals(PageStoreConfig.SEGMENT_SIZE, unit);
        } else {
            int region = PageStoreConfig.SEGMENT_REGION_SIZE;
            assertTrue(unit == region || unit == region + PageStoreConfig.REGION_ALIGNMENT,
                    "a region, aligned by aligned_alloc or by over-allocating: " + unit);
            assertNotNull(chunkOf(buf).segment.region);
        }
        assertEquals(unit, allocator.metric().usedDirectMemory());
        assertNotNull(chunkOf(buf).segment);
        ByteBuf heap = allocator.heapBuffer(1024, 1024);
        assertNull(chunkOf(heap).segment);
        assertEquals(AdaptivePoolingAllocator.MIN_CHUNK_SIZE, allocator.metric().usedHeapMemory());
        buf.release();
        heap.release();
        assertEquals(unit, allocator.metric().usedDirectMemory(),
                "the segment stays with the chunk its size class keeps");
    }

    // ---- Per-allocator parameters ----

    /** Bad parameters are rejected: too many slices, a partial slice, no room for the largest chunk, bad retention. */
    @Test
    void pageStoreConfigRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(65 * SLICE_SIZE, SLICE_SIZE, 0, INTERVAL, 0.5), "65 slices");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE + 4096, SLICE_SIZE, 0, INTERVAL, 0.5), "partial slice");
        final CountingSegmentSource source = new CountingSegmentSource();
        assertThrows(IllegalArgumentException.class, () -> new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(512 * 1024, 8 * 1024, 0, INTERVAL, 0.5)), "576 KiB chunks do not fit");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, 0, 0, INTERVAL, 0.5), "no slice");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, -1, INTERVAL, 0.5), "negative cache");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, 0, 0, 0.5), "no interval");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, 0, INTERVAL, 0), "frees nothing");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, 0, INTERVAL, 1.5), "more than all");
        PageStoreConfig ok = new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, 0, INTERVAL, 1);
        assertEquals(64, ok.slicesPerSegment());
        assertEquals(0, ok.toFree(0));
        assertEquals(5, ok.toFree(5));
        PageStoreConfig half = new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, 0, INTERVAL, 0.5);
        assertEquals(3, half.toFree(5));
        assertEquals(1, half.toFree(1));
        assertEquals(2, half.toFree(4));
    }

    /** The direct defaults: 64 KiB slices, the property-driven segment size and cache bound, 10 s, half. */
    @Test
    void directDefaults() {
        PageStoreConfig direct = PageStoreConfig.directDefaults();
        assertEquals(SLICE_SIZE, direct.sliceSize);
        assertEquals(64 * 1024, direct.sliceSize);
        assertEquals(PageStoreConfig.SEGMENT_SIZE, direct.segmentSize);
        assertEquals(PageStoreConfig.SEGMENT_CACHE_BYTES, direct.segmentCacheBytes);
        assertEquals(IdleDecay.DECAY_INTERVAL_NANOS, direct.decayIntervalNanos);
        assertEquals(0.5, direct.decayFraction);
    }

    /**
     * The chunk-to-span table comes from the allocator's own slice size: with 32 KiB slices the 4352-byte class gets
     * 160 KiB chunks (136 KiB rounded up to 5 slices), with 64 KiB slices 192 KiB; and each allocator's spans are
     * slices of its own size.
     */
    @Test
    void chunkSizesFollowTheInstanceSliceSize() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator small = new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(2 * 1024 * 1024, 32 * 1024, 0, INTERVAL, 0.5));
        AdaptivePoolingAllocator large = newAllocator(source, SEGMENT_SIZE, 0);
        ByteBuf a = small.allocate(4352, 4352);
        ByteBuf b = large.allocate(4352, 4352);
        try {
            assertEquals(160 * 1024, chunkOf(a).capacity());
            assertEquals(32 * 1024, chunkOf(a).segment.sliceSize);
            assertEquals(64, chunkOf(a).segment.slices);
            assertEquals(192 * 1024, chunkOf(b).capacity());
            assertEquals(SLICE_SIZE, chunkOf(b).segment.sliceSize);
            assertEquals(2L * 1024 * 1024, small.usedMemory());
            assertEquals(SEGMENT_SIZE, large.usedMemory());
        } finally {
            a.release();
            b.release();
        }
    }

    /** A cache of 0 bytes keeps nothing: a segment that empties is given back at once. */
    @Test
    void noCacheGivesAWhollyFreeSegmentBackAtOnce() {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE, 0);
        assertEquals(0, allocator.segmentCache.capacity());
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        Segment a = heap.claim(10);
        assertEquals(SEGMENT_SIZE, allocator.usedMemory());
        heap.release(a, 0, 10);
        assertEquals(0, a.buffer.refCnt());
        assertEquals(0, allocator.segmentCache.size());
        assertEquals(1, allocator.segmentCache.freed);
        assertEquals(0, allocator.usedMemory());
        assertAccounted(source, allocator);
    }

    /** The cache's interval and fraction are the allocator's: 1 s and all of them frees every cold segment at once. */
    @Test
    void cacheAgesByTheInstanceIntervalAndFraction() {
        CountingSegmentSource source = new CountingSegmentSource();
        long second = 1000L * 1000 * 1000;
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE, 64 * 1024 * 1024, second, 1));
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        List<Segment> segments = new ArrayList<Segment>();
        for (int i = 0; i < 3; i++) {
            segments.add(heap.claim(63));
        }
        for (Segment segment : segments) {
            heap.release(segment, 0, 63);
        }
        SegmentCache cache = allocator.segmentCache;
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
}
