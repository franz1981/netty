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
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Buffers above the size classes as spans of their heap's segments (see {@code SpanMagazine}): sized to whole
 * slices, one chunk object per segment, released by the owner at once and by other threads through the owner, and by
 * anyone once the heap is gone.
 */
final class AdaptiveLargeSpansTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    @BeforeEach
    void pooledAboveTheSizeClasses() {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
    }

    private AdaptivePoolingAllocator allocator(boolean withRegions) {
        return withRegions ? newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT)
                : newAllocator(segments, SEGMENT_SIZE);
    }

    private static AdaptivePoolingAllocator.AdaptiveByteBuf adaptive(ByteBuf buf) {
        return (AdaptivePoolingAllocator.AdaptiveByteBuf) (buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf ?
                buf : buf.unwrap());
    }

    /** Up to half a segment: a span of whole slices; above: a whole segment of the store. */
    @Test
    void spansAreSizedToWholeSlicesUpToHalfASegment() {
        AdaptivePoolingAllocator allocator = allocator(false);
        int[] sizes = {140 * 1024, 192 * 1024, 600 * 1024, MIB + 1, 3 * MIB / 2, SEGMENT_SIZE / 2};
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int size : sizes) {
            ByteBuf buf = allocator.allocate(size, Integer.MAX_VALUE);
            bufs.add(buf);
            assertEquals(size, buf.capacity());
            int wholeSlices = (size + SLICE - 1) / SLICE * SLICE;
            int fast = buf.maxFastWritableBytes() + buf.writerIndex();
            assertTrue(fast >= size && fast <= wholeSlices && (wholeSlices - fast) % 64 == 0
                    && wholeSlices - fast <= Math.min(4032, wholeSlices - size),
                    "a span of whole slices, less its colour, for " + size + ": " + fast);
            assertTrue(adaptive(buf).chunk instanceof AdaptivePoolingAllocator.SpanChunk, "a span for " + size);
        }
        ByteBuf whole = allocator.allocate(SEGMENT_SIZE / 2 + 1, SEGMENT_SIZE / 2 + 1);
        assertTrue(!(adaptive(whole).chunk instanceof AdaptivePoolingAllocator.SpanChunk), "above half a segment");
        assertEquals(segments.segmentsAllocated(), segments.segmentsLive(), "a whole segment of its own");
        whole.release();
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertEquals(0, segments.chunks.size(), "nothing from the chunk allocator");
        assertAccounted(segments, allocator);
    }

    private static long colourOf(ByteBuf buf) {
        AdaptivePoolingAllocator.SpanChunk chunk = (AdaptivePoolingAllocator.SpanChunk) adaptive(buf).chunk;
        return (buf.memoryAddress() - chunk.segment.memoryAddress()) % SLICE;
    }

    /**
     * Large buffers start 64 bytes further into their span each time, round robin over as many colours as the span's
     * unused tail allows, 64 at most: none for an exact fit, four for a 200-byte tail. Released, they give back
     * exactly the slices they claimed.
     */
    @Test
    void largeSpansRotateTheirColourThroughTheirTail() {
        AdaptivePoolingAllocator allocator = allocator(false);
        int[][] cases = {{256 * 1024 - 8192, 64}, {256 * 1024, 1}, {256 * 1024 - 200, 4}};
        Set<Segment> seen = new HashSet<Segment>();
        for (int[] c : cases) {
            int size = c[0];
            int colours = c[1];
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < 70; i++) {
                bufs.add(allocator.allocate(size, size));
            }
            for (int i = 0; i < bufs.size(); i++) {
                ByteBuf buf = bufs.get(i);
                long colour = colourOf(buf);
                assertTrue(colour % 64 == 0 && colour < colours * 64L, "size " + size + " colour " + colour);
                if (colours > 1 && i > 0) {
                    assertEquals((colourOf(bufs.get(i - 1)) + 64) % (colours * 64L), colour, "round robin");
                }
                AdaptivePoolingAllocator.SpanChunk chunk = (AdaptivePoolingAllocator.SpanChunk) adaptive(buf).chunk;
                long spanEnd = chunk.segment.memoryAddress() + (buf.memoryAddress() - chunk.segment.memoryAddress())
                        / SLICE * SLICE + 4L * SLICE;
                assertTrue(buf.memoryAddress() + buf.maxFastWritableBytes() + buf.writerIndex() <= spanEnd,
                        "inside its span");
                buf.setByte(size - 1, 42);
                seen.add(chunk.segment);
            }
            for (ByteBuf buf : bufs) {
                buf.release();
            }
            assertAccounted(segments, allocator);
        }
        for (Segment segment : seen) {
            assertEquals(0, segment.usedSlices(), "every claimed slice went back: " + segment);
        }
    }

    /** A coloured buffer grows in place up to the end of its span, and away from it beyond, freeing the span. */
    @Test
    void aColouredBufferGrowsWithinItsSpan() {
        AdaptivePoolingAllocator allocator = allocator(false);
        int size = 256 * 1024 - 8192;
        allocator.allocate(size, Integer.MAX_VALUE).release(); // colour 0 taken: the next is coloured
        ByteBuf buf = allocator.allocate(size, Integer.MAX_VALUE);
        long colour = colourOf(buf);
        assertEquals(64, colour);
        long address = buf.memoryAddress();
        int fast = buf.maxFastWritableBytes();
        assertEquals(4 * SLICE - colour, fast);
        buf.writerIndex(0).writeZero(fast);
        assertEquals(address, buf.memoryAddress(), "grown in place");
        assertEquals(fast, buf.capacity());
        Segment segment = ((AdaptivePoolingAllocator.SpanChunk) adaptive(buf).chunk).segment;
        assertEquals(4, segment.usedSlices());
        buf.writeByte(1);
        assertTrue(buf.memoryAddress() != address, "beyond its span: moved");
        buf.release();
        assertEquals(0, segment.usedSlices(), "the old span went back whole");
        assertAccounted(segments, allocator);
    }

    /**
     * The spans of one segment share one chunk    /**
     * The spans of one segment share one chunk for as long as the segment stays in the heap, its reserve included:
     * allocating and releasing large buffers makes no chunk, even when each release empties the segment. The chunk
     * ends when the segment goes back to the store.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void spansOfASegmentShareItsChunk(boolean withRegions) {
        AdaptivePoolingAllocator allocator = allocator(withRegions);
        ByteBuf first = allocator.allocate(256 * 1024, 256 * 1024);
        AdaptivePoolingAllocator.SpanChunk chunk = (AdaptivePoolingAllocator.SpanChunk) adaptive(first).chunk;
        Segment segment = chunk.segment;
        first.release();
        for (int i = 0; i < 1000; i++) {
            ByteBuf buf = allocator.allocate(512 * 1024, 512 * 1024);
            assertSame(chunk, adaptive(buf).chunk, "the segment comes back from the reserve with its chunk");
            buf.release();
        }
        assertSame(chunk, segment.spanChunk);
        allocator.pageStore.close();
    }

    /** When the heap gives the segment back to the store, its chunk is retired and the segment forgets it. */
    @Test
    void theChunkEndsWithTheSegmentsStayInTheHeap() throws Exception {
        final AdaptivePoolingAllocator allocator = allocator(true);
        final AtomicReference<AdaptivePoolingAllocator.SpanChunk> chunk =
                new AtomicReference<AdaptivePoolingAllocator.SpanChunk>();
        Thread owner = new FastThreadLocalThread(() -> {
            ByteBuf buf = allocator.allocate(256 * 1024, 256 * 1024);
            chunk.set((AdaptivePoolingAllocator.SpanChunk) adaptive(buf).chunk);
            buf.release();
        });
        owner.start();
        owner.join();
        // The thread-local heap died: its reserve went back to the store.
        assertTrue(chunk.get().retired);
        assertNull(chunk.get().segment.spanChunk);
        assertNull(chunk.get().segment.owner);
    }

    /**
     * Heaps that die while other threads still hold, and release, their buffers: every span comes back exactly once
     * (a second release of a slice throws), and every segment goes back to the store or its source, none pinned.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void heapsDyingWithReleasesInFlightPinNothing(boolean withRegions) throws Exception {
        final AdaptivePoolingAllocator allocator = allocator(withRegions);
        final int releasers = 3;
        final BlockingQueue<ByteBuf> handOff = new ArrayBlockingQueue<ByteBuf>(64);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final ByteBuf poison = Unpooled.buffer(1);
        final Thread.UncaughtExceptionHandler record = (t, e) -> failure.compareAndSet(null, e);
        List<Thread> threads = new ArrayList<Thread>();
        for (int r = 0; r < releasers; r++) {
            final SplittableRandom random = new SplittableRandom(r);
            Thread releaser = new Thread(() -> {
                try {
                    for (;;) {
                        ByteBuf buf = handOff.take();
                        if (buf == poison) {
                            return;
                        }
                        if (random.nextInt(8) == 0) {
                            Thread.yield();
                        }
                        check(buf);
                        buf.release();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            releaser.setUncaughtExceptionHandler(record);
            releaser.start();
            threads.add(releaser);
        }
        for (int round = 0; round < 40 && failure.get() == null; round++) {
            final SplittableRandom random = new SplittableRandom(1000 + round);
            // A thread-local heap, or a stripe: a thread that cleans up its thread-locals has its own heap.
            Runnable owner = () -> {
                try {
                    List<ByteBuf> mine = new ArrayList<ByteBuf>();
                    for (int i = 0; i < 64; i++) {
                        int size = 136 * 1024 + random.nextInt(SEGMENT_SIZE / 2 - 136 * 1024);
                        ByteBuf buf = allocator.allocate(size, size);
                        stamp(buf);
                        if (random.nextBoolean()) {
                            handOff.put(buf);
                        } else {
                            mine.add(buf);
                        }
                        if (random.nextInt(4) == 0 && !mine.isEmpty()) {
                            ByteBuf last = mine.remove(mine.size() - 1);
                            check(last);
                            last.release();
                        }
                    }
                    // Half of what is left dies with the heap, handed off: released after it is gone.
                    for (ByteBuf buf : mine) {
                        if (random.nextBoolean()) {
                            handOff.put(buf);
                        } else {
                            check(buf);
                            buf.release();
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            };
            Thread thread = round % 2 == 0 ? new FastThreadLocalThread(owner) : new Thread(owner);
            thread.setUncaughtExceptionHandler(record);
            thread.start();
            thread.join();
        }
        for (int r = 0; r < releasers; r++) {
            handOff.put(poison);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        poison.release();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        // A stripe applies the notes other threads left at its next allocation or decay: here, now.
        drainStripes(allocator);
        if (withRegions) {
            // Stripes live as long as the allocator and keep their reserve of one segment each.
            assertEquals(stripesHoldingSegments(allocator), allocator.pageStore.slotCounts()[0],
                    "slots in heaps: only the stripes' reserves");
            assertAccounted(segments, regions, allocator);
        } else {
            assertEquals(stripesHoldingSegments(allocator), segments.segmentsLive(),
                    "segments live: only the stripes' reserves");
            assertAccounted(segments, allocator);
        }
        // What the stripes still hold is only their reserves: no span is out, no segment is in a heap's list.
        assertEquals(0, segmentsInStripeLists(allocator));
    }

    /**
     * Writes the buffer's own identity at both ends: a span handed out twice while in use would carry another
     * buffer's stamp by the time {@link #check} reads it.
     */
    private static void stamp(ByteBuf buf) {
        long id = System.identityHashCode(buf) ^ (long) buf.memoryAddress() << 20;
        buf.setLong(0, id);
        buf.setLong(buf.capacity() - 8, ~id);
    }

    private static void check(ByteBuf buf) {
        long id = System.identityHashCode(buf) ^ (long) buf.memoryAddress() << 20;
        assertEquals(id, buf.getLong(0), "the start of a live span was overwritten");
        assertEquals(~id, buf.getLong(buf.capacity() - 8), "the end of a live span was overwritten");
    }

    private static void drainStripes(AdaptivePoolingAllocator allocator) throws Exception {
        java.lang.reflect.Field stripesField = AdaptivePoolingAllocator.class.getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        for (Object stripe : (Object[]) stripesField.get(allocator)) {
            java.lang.reflect.Field lockField = stripe.getClass().getDeclaredField("lock");
            java.lang.reflect.Field magField = stripe.getClass().getDeclaredField("spanMagazine");
            lockField.setAccessible(true);
            magField.setAccessible(true);
            StampedLock lock = (StampedLock) lockField.get(stripe);
            long stamp = lock.writeLock();
            try {
                Object mag = magField.get(stripe);
                if (mag != null) {
                    java.lang.reflect.Method drain = mag.getClass().getDeclaredMethod("drainPending");
                    drain.setAccessible(true);
                    drain.invoke(mag);
                }
            } finally {
                lock.unlockWrite(stamp);
            }
        }
    }

    private static int stripesHoldingSegments(AdaptivePoolingAllocator allocator) throws Exception {
        int n = 0;
        for (HeapSegments heap : stripeHeapSegments(allocator)) {
            n += heap.reserved;
        }
        return n;
    }

    private static int segmentsInStripeLists(AdaptivePoolingAllocator allocator) throws Exception {
        int n = 0;
        for (HeapSegments heap : stripeHeapSegments(allocator)) {
            n += heap.count;
        }
        return n;
    }

    private static List<HeapSegments> stripeHeapSegments(AdaptivePoolingAllocator allocator) throws Exception {
        java.lang.reflect.Field stripesField = AdaptivePoolingAllocator.class.getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        Object[] stripes = (Object[]) stripesField.get(allocator);
        List<HeapSegments> heaps = new ArrayList<HeapSegments>();
        for (Object stripe : stripes) {
            java.lang.reflect.Field f = stripe.getClass().getDeclaredField("heapSegments");
            f.setAccessible(true);
            HeapSegments heap = (HeapSegments) f.get(stripe);
            if (heap != null) {
                heaps.add(heap);
            }
        }
        return heaps;
    }
}
