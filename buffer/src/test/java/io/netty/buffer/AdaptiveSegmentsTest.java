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
import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.StampedLock;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static io.netty.buffer.PageStoreTestSupport.offsetIn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The page store inside an {@link AdaptivePoolingAllocator}, of direct or heap memory: size-class chunks as spans of
 * the heaps' segments, reused across size classes, given back by the heaps' decays, and the defaults per memory mode.
 */
public class AdaptiveSegmentsTest {
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

    /** The buffers a chunk hands out: an exact fit of 32 or more gives one up for its colours. */
    private static int expectedBuffers(int sizeClass) {
        int span = expectedSlices(sizeClass) * SLICE_SIZE_BYTES;
        int fit = span / sizeClass;
        int room = span - fit * sizeClass;
        // An exact fit gives up a buffer only when that buffer makes room for a second colour: not the 32-byte class.
        return room < 64 && fit >= 32 && room + sizeClass >= 64 ? fit - 1 : fit;
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
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void sizeClassChunksAreSpansOfSegments(boolean heap, final boolean threadLocal) throws Exception {
        assumeFalse(isLowMemory() && threadLocal, "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource(heap);
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        final int pooled = pooledSizeClassesCount();
        Callable<List<ByteBuf>> work = () -> {
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < pooled; i++) {
                int size = SIZE_CLASSES[i];
                ByteBuf buf = allocator.allocate(size, size);
                bufs.add(buf);
                SizeClassedChunk chunk = chunkOf(buf);
                assertNotNull(chunk.segment, "size " + size);
                assertSame(chunk.segment.buffer, field(chunk, "delegate"), "reads the segment's own buffer: no view");
                assertEquals(expectedSlices(size) * SLICE_SIZE_BYTES, chunk.capacity(), "chunk of size " + size);
                assertEquals(expectedBuffers(size), field(chunk, "segments"), "segments of size " + size);
                long offset = offsetIn(buf, chunk.segment);
                assertTrue(offset >= (long) chunk.spanStart * SLICE_SIZE_BYTES
                        && offset + size <= ((long) chunk.spanStart + expectedSlices(size)) * SLICE_SIZE_BYTES,
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

    /**
     * Consecutive chunks of a class whose segments leave a tail start 64 bytes further into their span each time,
     * as many times as the tail allows, then wrap; a class that fits its chunk exactly always starts at the span.
     * Buffers stay inside the span, and a released chunk gives back exactly the slices it claimed, recycled or not.
     */
    @ParameterizedTest
    @CsvSource({"false, 640", "false, 1024", "false, 2048", "false, 2304", "false, 4096",
            "true, 640", "true, 1024", "true, 2048", "true, 2304", "true, 4096"})
    void spanChunksRotateTheirStartThroughTheirTail(boolean heapMemory, int size) throws Exception {
        CountingSegmentSource source = new CountingSegmentSource(heapMemory);
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        int slices = expectedSlices(size);
        int perChunk = expectedBuffers(size);
        int colours = Math.min(16, (slices * SLICE_SIZE_BYTES - perChunk * size) / 64 + 1);
        assertEquals(size == 640 ? 9 : 16, colours);
        int chunks = 40;
        Object stripe = null;
        for (int round = 0; round < 2; round++) {
            // The second round re-creates the chunks with the free lists the first one gave up.
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            List<SizeClassedChunk> seen = new ArrayList<SizeClassedChunk>();
            long[] firstOffsets = new long[chunks];
            for (int i = 0; i < chunks * perChunk; i++) {
                ByteBuf buf = allocator.allocate(size, size);
                bufs.add(buf);
                SizeClassedChunk chunk = chunkOf(buf);
                long offset = offsetInSpan(buf, chunk);
                int c = firstOffset(seen, chunk, firstOffsets);
                firstOffsets[c] = Math.min(firstOffsets[c], offset);
                assertTrue(offset >= 0 && offset + size <= (long) slices * SLICE_SIZE_BYTES,
                        "size " + size + " outside its span");
                buf.setLong(size - 8, 0x0123456789ABCDEFL);
            }
            assertEquals(chunks, seen.size());
            stripe = usedStripe(allocator);
            HeapSegments heapSegments = idleDecay(stripe).heapSegments;
            int usedSlices = 0;
            for (int i = 0; i < heapSegments.count; i++) {
                usedSlices += heapSegments.segments[i].usedSlices();
            }
            assertEquals(chunks * slices, usedSlices, "each chunk claimed its slices, no more");
            if (round == 0) {
                for (int i = 0; i < chunks; i++) {
                    // Every buffer of every chunk is out: the lowest offset is where the chunk's buffers start.
                    assertEquals(i % colours * 64, firstOffsets[i], "chunk " + i + " of size " + size);
                    assertEquals(slices, seen.get(i).spanSlices());
                }
            }
            for (ByteBuf buf : bufs) {
                assertEquals(0x0123456789ABCDEFL, buf.getLong(size - 8));
                buf.release();
            }
            assertAccounted(source, allocator);
        }
        long now = System.nanoTime();
        for (int decay = 1; decay <= 8; decay++) {
            decayStripe(stripe, now += INTERVAL);
        }
        assertEquals(0, idleDecay(stripe).heapSegments.count, "every span went back whole");
        assertEquals(0, source.segmentsLive());
        assertAccounted(source, allocator);
    }

    /**
     * As {@link #spanChunksRotateTheirStartThroughTheirTail} on a thread-local heap, whose owner allocates from the
     * chunk's local free list: an exact fit's dropped segment is never handed out, coloured or not.
     */
    @ParameterizedTest
    @CsvSource({"false, 1024", "false, 4096", "true, 1024", "true, 4096"})
    void threadLocalSpanChunksNeverHandOutTheDroppedSegment(boolean heapMemory, final int size) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource(heapMemory);
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        final int slices = expectedSlices(size);
        final int perChunk = expectedBuffers(size);
        assertEquals(slices * SLICE_SIZE_BYTES / size - 1, perChunk);
        final int chunks = 20;
        onFastThreadLocalThread(() -> {
            for (int round = 0; round < 2; round++) {
                // The second round re-creates the chunks with the free lists the first one gave up.
                List<ByteBuf> bufs = new ArrayList<ByteBuf>();
                List<SizeClassedChunk> seen = new ArrayList<SizeClassedChunk>();
                long[] firstOffsets = new long[chunks];
                for (int i = 0; i < chunks * perChunk; i++) {
                    ByteBuf buf = allocator.allocate(size, size);
                    bufs.add(buf);
                    SizeClassedChunk chunk = chunkOf(buf);
                    assertTrue(chunk.inThreadLocalMagazine());
                    long offset = offsetInSpan(buf, chunk);
                    int c = firstOffset(seen, chunk, firstOffsets);
                    firstOffsets[c] = Math.min(firstOffsets[c], offset);
                    assertTrue(offset >= 0 && offset + size <= (long) slices * SLICE_SIZE_BYTES,
                            "size " + size + " outside its span");
                    buf.setLong(size - 8, 0x0123456789ABCDEFL);
                }
                assertEquals(chunks, seen.size(), perChunk + " buffers per chunk");
                for (int i = 0; i < chunks; i++) {
                    long colour = firstOffsets[i];
                    assertTrue(colour >= 0 && colour < 16 * 64 && colour % 64 == 0, "colour " + colour);
                    if (round == 0) {
                        assertEquals(i % 16 * 64, colour, "chunk " + i + " of size " + size);
                    }
                }
                for (ByteBuf buf : bufs) {
                    assertEquals(0x0123456789ABCDEFL, buf.getLong(size - 8));
                    buf.release();
                }
            }
            Object heap = threadLocalHeap(allocator);
            long now = System.nanoTime();
            for (int decay = 1; decay <= 8; decay++) {
                idleDecay(heap).decay(now += INTERVAL);
            }
            assertEquals(0, idleDecay(heap).heapSegments.count, "every span went back whole");
            return null;
        });
        assertAccounted(source, allocator);
    }

    /** Where {@code buf} starts in the span of its chunk. */
    private static long offsetInSpan(ByteBuf buf, SizeClassedChunk chunk) {
        return offsetIn(buf, chunk.segment) - (long) chunk.spanStart * SLICE_SIZE_BYTES;
    }

    /** The index of {@code chunk} in {@code seen}, added on first sight with no offset seen yet. */
    private static int firstOffset(List<SizeClassedChunk> seen, SizeClassedChunk chunk, long[] firstOffsets) {
        int i = seen.indexOf(chunk);
        if (i < 0) {
            i = seen.size();
            seen.add(chunk);
            firstOffsets[i] = Long.MAX_VALUE;
        }
        return i;
    }

    /**
     * Buffers of different chunks of one heap segment, each written whole with its own pattern, then read back: no two
     * overlap. The second round's chunks are re-created with the free lists of the first round's (see
     * {@link AdaptivePoolingAllocator.SizeClassChunkRecycler}), at other spans.
     */
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void buffersOfRecreatedChunksStayInTheirSpans(boolean heap, boolean threadLocal) throws Exception {
        assumeFalse(isLowMemory() && threadLocal, "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource(heap);
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        final int small = 1024; // 2-slice chunks
        final int large = isLowMemory() ? 16384 : 65536; // 8-slice chunks
        Callable<Void> work = () -> {
            List<Object> firstLists = new ArrayList<Object>();
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < 3 * (8 * SLICE_SIZE_BYTES / large); i++) {
                bufs.add(allocator.allocate(large, large));
            }
            for (ByteBuf buf : bufs) {
                firstLists.add(field(chunkOf(buf), "externalFreeList"));
                buf.release();
            }
            bufs.clear();
            // The large class keeps one chunk: another class's chunks take the freed spans, at other offsets.
            for (int i = 0; i < 3 * (2 * SLICE_SIZE_BYTES / small); i++) {
                bufs.add(allocator.allocate(small, small));
            }
            for (int i = 0; i < 3 * (8 * SLICE_SIZE_BYTES / large); i++) {
                bufs.add(allocator.allocate(large, large));
            }
            int recreated = 0;
            List<SizeClassedChunk> seen = new ArrayList<SizeClassedChunk>();
            for (int i = 0; i < bufs.size(); i++) {
                ByteBuf buf = bufs.get(i);
                SizeClassedChunk chunk = chunkOf(buf);
                if (!seen.contains(chunk)) {
                    seen.add(chunk);
                    recreated += firstLists.contains(field(chunk, "externalFreeList")) && chunk.spanStart != 0 ? 1 : 0;
                }
                long offset = offsetIn(buf, chunk.segment);
                assertTrue(offset >= (long) chunk.spanStart * SLICE_SIZE_BYTES && offset + buf.capacity()
                        <= ((long) chunk.spanStart + chunk.spanSlices()) * SLICE_SIZE_BYTES, "outside its span");
                for (int j = 0; j < buf.capacity(); j += 8) {
                    buf.setLong(j, (long) i << 32 | j);
                }
            }
            assertTrue(recreated > 0, "a chunk re-created from recycled lists, not at the segment's start");
            for (int i = 0; i < bufs.size(); i++) {
                ByteBuf buf = bufs.get(i);
                for (int j = 0; j < buf.capacity(); j += 8) {
                    assertEquals((long) i << 32 | j, buf.getLong(j));
                }
                buf.release();
            }
            assertAccounted(source, allocator);
            return null;
        };
        if (threadLocal) {
            onFastThreadLocalThread(work);
        } else {
            work.call();
        }
        assertEquals(1, source.segmentsAllocated());
        assertTrue(source.chunks.isEmpty(), "no chunk buffer of its own");
    }

    /**
     * Chunks given up by one size class free their spans, which other classes of the same heap reuse, of any chunk
     * size: no new segment. Then decays give everything back: an idle class gives up its chunks, an emptied segment
     * goes to the heap's reserve, and the decays give the reserve back by halves.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void spansAreReusedAcrossClassesAndDecaysGiveSegmentsBack(boolean heap) throws Exception {
        CountingSegmentSource source = new CountingSegmentSource(heap);
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        int size = isLowMemory() ? 16384 : 65536; // 8-slice chunks either way
        int perChunk = expectedBuffers(size);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 12 * perChunk; i++) {
            bufs.add(allocator.allocate(size, size));
        }
        // 12 chunks of 8 slices: the first segment is filled (8 of them), then a second one.
        assertEquals(2, source.segmentsAllocated());
        Object stripe = usedStripe(allocator);
        HeapSegments heapSegments = idleDecay(stripe).heapSegments;
        assertEquals(2, heapSegments.count);
        assertEquals(64, heapSegments.segments[0].usedSlices());
        assertEquals(32, heapSegments.segments[1].usedSlices());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        assertAccounted(source, allocator);
        // Every chunk ran out of segments, so none is active; the class keeps the last four to empty (its floor), in
        // the second segment. The first segment emptied and went to the heap's reserve.
        assertEquals(1, heapSegments.count);
        assertEquals(32, heapSegments.segments[0].usedSlices());
        assertEquals(1, heapSegments.reserved);
        // Another class, another chunk size: from the free slices.
        int other = 1024; // 2-slice chunks
        for (int i = 0; i < 16 * expectedBuffers(other); i++) {
            bufs.add(allocator.allocate(other, other));
        }
        assertEquals(2, source.segmentsAllocated(), "32 slices fit in the free ones");
        assertEquals(1, heapSegments.count, "the fullest segment with room, not the reserved one");
        assertEquals(1, heapSegments.reserved);
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(source, allocator);

        // Decays, forced, one interval apart. Nothing allocates in between, so both classes go idle.
        long now = System.nanoTime();
        for (int decay = 1; decay <= 8; decay++) {
            now += INTERVAL;
            decayStripe(stripe, now);
            assertAccounted(source, allocator);
            assertEquals(heapSegments.count + heapSegments.reserved, source.segmentsLive());
        }
        assertEquals(0, heapSegments.count, "every segment left the heap");
        assertEquals(0, heapSegments.reserved, "and the reserve went back");
        assertEquals(0, source.segmentsLive());
        assertEquals(0, allocator.usedMemory());
        assertEquals(2, source.segmentsAllocated());
    }

    /**
     * On a thread-local heap every other thread's release is a note. The chunks such releases empty are applied by
     * the owner's decays: an idle class gives them up, and the segment they emptied goes to the heap's reserve.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void foreignReleasesEmptyASegmentThroughTheNotes(boolean heapMemory) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource(heapMemory);
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        final int size = 65536;
        final int perChunk = expectedBuffers(size);
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
            assertEquals(0, heapSegments.reserved);
            long now = System.nanoTime();
            idleDecay(heap).decay(now + INTERVAL); // the class allocated since the last decay: not idle yet
            assertEquals(1, heapSegments.count);
            idleDecay(heap).decay(now + 2 * INTERVAL); // idle: its chunks, applied from the notes, are given up
            assertEquals(0, heapSegments.count);
            assertEquals(1, heapSegments.reserved);
            assertAccounted(source, allocator);
            return null;
        });
        assertEquals(1, source.segmentsAllocated());
        assertAccounted(source, allocator);
    }

    /**
     * A decay frees the chunks that hold no buffer and alone keep a sparse segment in use, when the fullest segment
     * has room for them: their classes make their next chunk there, and the emptied segment goes to the reserve. A
     * chunk with a buffer out stays, and so does everything when the other segments have no room.
     */
    @Test
    void decaysMoveEmptyChunksOutOfSparseSegments() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        int size = isLowMemory() ? 16384 : 65536; // 8-slice chunks either way
        int perChunk = expectedBuffers(size);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 8 * perChunk; i++) {
            bufs.add(allocator.allocate(size, size));
        }
        ByteBuf small = allocator.allocate(1024, 1024); // a 2-slice chunk: the first segment is full
        Object stripe = usedStripe(allocator);
        HeapSegments heapSegments = idleDecay(stripe).heapSegments;
        assertEquals(2, heapSegments.count);
        Segment sparse = chunkOf(small).segment;
        assertSame(heapSegments.segments[1], sparse);
        long now = System.nanoTime();

        // The full segment has no room for the small chunk: it stays, emptied or not.
        small.release();
        decayStripe(stripe, now += INTERVAL);
        assertEquals(2, heapSegments.count);
        assertEquals(2, sparse.usedSlices());

        // Two chunks of the full segment empty and go (above the class's floor): now there is room.
        for (int i = 0; i < 2 * perChunk; i++) {
            bufs.remove(0).release();
        }
        assertEquals(48, heapSegments.segments[0].usedSlices());
        small = allocator.allocate(1024, 1024);
        assertSame(sparse, chunkOf(small).segment, "the class keeps its chunk");
        // A buffer out pins the chunk.
        decayStripe(stripe, now += INTERVAL);
        assertEquals(2, heapSegments.count);
        small.release();
        allocator.allocate(1024, 1024).release(); // the class stays in use: its decay does not give the chunk up
        decayStripe(stripe, now += INTERVAL);
        assertEquals(1, heapSegments.count, "the sparse segment emptied");
        assertEquals(1, heapSegments.reserved);
        assertTrue(sparse.isWhollyFree());
        small = allocator.allocate(1024, 1024);
        assertSame(heapSegments.segments[0], chunkOf(small).segment, "the next chunk is made in the fullest");
        assertEquals(50, heapSegments.segments[0].usedSlices());
        assertEquals(2, source.segmentsAllocated(), "no segment allocated to move it");
        small.release();
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(source, allocator);
    }

    /**
     * A thread-local heap freed (its thread ended) while buffers are still out: its segments stay accounted, and the
     * release of the last buffer of a segment, on another thread, gives the segment back.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void segmentsOfAnEndedThreadGoBackWithTheirLastBuffer(boolean heap) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource(heap);
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
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
        assertEquals(1, source.segmentsLive(), "spans are still out");
        Segment segment = chunkOf(bufs.get(0)).segment;
        assertNotNull(segment.owner, "still the ended heap's");
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertTrue(segment.isWhollyFree());
        assertNull(segment.owner);
        assertEquals(0, source.segmentsLive());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(source, allocator);
    }

    /** 2 MiB segments: 32 slices, the same chunks, three 9-slice chunks per segment. */
    @Test
    void twoMebibyteSegments() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, 2 * 1024 * 1024);
        int size = 16896;
        int perChunk = 9 * SLICE_SIZE_BYTES / size; // 34
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
     * The defaults: 4 MiB segments in 64-segment regions; 2 MiB segments in low-memory mode, where the single stripe
     * carves its chunks out of segments too. {@code mmap} regions wherever they can be mapped, for direct memory only;
     * else regions of one block, as heap memory always has. The memory accounted is a segment, direct or heap. Heap
     * segments are cut under G1 (see {@link PageStoreConfig#heapSegmentSizeOf}).
     */
    @Test
    void segmentDefaultsFollowTheMemoryMode() throws Exception {
        boolean lowMemory = isLowMemory();
        assumeFalse(System.getProperty("io.netty.allocator.segmentSize") != null
                || System.getProperty("io.netty.allocator.segmentRegionSize") != null, "set explicitly");
        assertEquals(lowMemory ? 2 * 1024 * 1024 : 4 * 1024 * 1024, PageStoreConfig.SEGMENT_SIZE_BYTES);
        assertEquals(Long.SIZE * PageStoreConfig.SEGMENT_SIZE_BYTES, PageStoreConfig.SEGMENT_REGION_SIZE_BYTES);
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, AdaptiveByteBufAllocatorTest.directSegmentSize(allocator));
        // Direct memory always has regions: mmap'd where they can be, else malloc'd.
        assertTrue(AdaptiveByteBufAllocatorTest.directRegions(allocator));
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        assertNotNull(chunkOf(buf).segment);
        assertNotNull(chunkOf(buf).segment.region);
        // mmap'd: the chunk's slices, 128 KiB for 1 KiB buffers; malloc'd: the whole region.
        long directUsed = AdaptiveByteBufAllocatorTest.directSharesSlices(allocator) ? 128 * 1024 :
                PageStoreConfig.SEGMENT_SIZE_BYTES;
        assertEquals(directUsed, allocator.metric().usedDirectMemory());
        PageStore heapStore = ((AdaptivePoolingAllocator) field(allocator, "heap")).pageStore;
        int heapSegmentSize = PageStoreConfig.heapDefaults().segmentSize;
        assertEquals(heapSegmentSize, heapStore.config.segmentSize);
        assertTrue(heapStore.regionSource instanceof MallocRegionSource);
        assertEquals(heapSegmentSize, heapStore.config.regionSize);
        ByteBuf heap = allocator.heapBuffer(1024, 1024);
        assertNotNull(chunkOf(heap).segment);
        assertSame(chunkOf(heap).segment.region.buffer, chunkOf(heap).segment.buffer);
        assertSame(chunkOf(heap).segment.buffer.array(), heap.array());
        assertEquals(heapSegmentSize, allocator.metric().usedHeapMemory());
        buf.release();
        heap.release();
        assertEquals(directUsed, allocator.metric().usedDirectMemory(),
                "the segment stays with the chunk its size class keeps");
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
                new PageStoreConfig(2 * 1024 * 1024, 32 * 1024, INTERVAL));
        AdaptivePoolingAllocator large = newAllocator(source, SEGMENT_SIZE);
        ByteBuf a = small.allocate(4352, 4352);
        ByteBuf b = large.allocate(4352, 4352);
        try {
            assertEquals(160 * 1024, chunkOf(a).capacity());
            assertEquals(32 * 1024, chunkOf(a).segment.sliceSize);
            assertEquals(64, chunkOf(a).segment.slices);
            assertEquals(192 * 1024, chunkOf(b).capacity());
            assertEquals(SLICE_SIZE_BYTES, chunkOf(b).segment.sliceSize);
            assertEquals(2L * 1024 * 1024, small.usedMemory());
            assertEquals(SEGMENT_SIZE, large.usedMemory());
        } finally {
            a.release();
            b.release();
        }
    }

    /**
     * A configured reserve bounds the wholly free segments a heap keeps after a burst: one, over the segment the
     * class's floor chunk keeps, where mimalloc's rule keeps eight.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 8})
    void aBurstLeavesAtMostTheConfiguredReserve(int reserve) throws Exception {
        CountingSegmentSource source = new CountingSegmentSource(true);
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, reserve));
        assertEquals(reserve, allocator.pageStore.reserveLimit());
        int size = isLowMemory() ? 16384 : 65536; // 8-slice chunks
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        // Exactly 96 chunks of 8 slices: 12 segments. Buffers per chunk as colouring leaves them (31 for 16 KiB).
        for (int i = 0; i < 12 * 8 * expectedBuffers(size); i++) {
            bufs.add(allocator.allocate(size, size));
        }
        assertEquals(12, source.segmentsAllocated());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertEquals(1 + reserve, source.segmentsLive(), "the floor chunk's segment and the reserve");
        assertAccounted(source, allocator);
    }

    /** The allocator rejects a page store whose segments cannot hold a buffer of its largest size class. */
    @Test
    void segmentsMustHoldTheLargestSizeClass() {
        final CountingSegmentSource source = new CountingSegmentSource();
        assertThrows(IllegalArgumentException.class, () -> new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(2 * SLICE_SIZE_BYTES, SLICE_SIZE_BYTES, INTERVAL)), "132 KiB takes 3 slices");
    }

    /**
     * Segments smaller than the largest size-class chunk (9 slices), as heap segments under G1 with 1 MiB regions
     * (7 slices): the chunks are cut to a segment, every size class still allocates, and every buffer stays inside
     * its segment.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void chunksAreCutToSegmentsSmallerThanThem(boolean heap) throws Exception {
        CountingSegmentSource source = new CountingSegmentSource(heap);
        int segmentSize = 7 * SLICE_SIZE_BYTES;
        AdaptivePoolingAllocator allocator = newAllocator(source, segmentSize);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < pooledSizeClassesCount(); i++) {
            int size = SIZE_CLASSES[i];
            for (int j = 0; j < 2; j++) {
                ByteBuf buf = allocator.allocate(size, size);
                SizeClassedChunk chunk = chunkOf(buf);
                assertEquals(Math.min(expectedSlices(size), 7) * SLICE_SIZE_BYTES, chunk.capacity(), "size " + size);
                long offset = offsetIn(buf, chunk.segment);
                assertTrue(offset >= 0 && offset + size <= segmentSize, "size " + size + " outside its segment");
                buf.setLong(size - 8, size);
                bufs.add(buf);
            }
        }
        for (ByteBuf buf : bufs) {
            assertEquals(buf.capacity(), buf.getLong(buf.capacity() - 8));
            buf.release();
        }
        assertTrue(source.chunks.isEmpty(), "no chunk buffer of its own");
        assertAccounted(source, allocator);
    }
}
