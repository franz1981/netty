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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The page store inside a direct {@link AdaptivePoolingAllocator}: size-class chunks as spans of the heaps' segments,
 * reused across size classes, given back by the heaps' decays, and the defaults per memory mode.
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
                assertEquals(expectedSlices(size) * SLICE_SIZE_BYTES, chunk.capacity(), "chunk of size " + size);
                assertEquals(chunk.capacity() / size, field(chunk, "segments"), "segments of size " + size);
                long base = chunk.segment.memoryAddress();
                long address = buf.memoryAddress();
                assertTrue(address >= base + (long) chunk.spanStart * SLICE_SIZE_BYTES
                        && address + size <= base + ((long) chunk.spanStart + expectedSlices(size)) * SLICE_SIZE_BYTES,
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
        assertNull(heap.pageStore);
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
     * size: no new segment. Then decays give everything back: an idle class gives up its chunks, an emptied segment
     * goes to the heap's reserve, and the decays give the reserve back by halves.
     */
    @Test
    void spansAreReusedAcrossClassesAndDecaysGiveSegmentsBack() throws Exception {
        CountingSegmentSource source = new CountingSegmentSource();
        AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        int size = isLowMemory() ? 16384 : 65536; // 8-slice chunks either way
        int perChunk = 8 * SLICE_SIZE_BYTES / size;
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
        // the second segment. The first segment emptied and went to the heap's reserve.
        assertEquals(1, heapSegments.count);
        assertEquals(8, heapSegments.segments[0].usedSlices());
        assertEquals(1, heapSegments.reserved);
        // Another class, another chunk size: from the free slices.
        int other = 1024; // 2-slice chunks
        for (int i = 0; i < 20 * (2 * SLICE_SIZE_BYTES / other); i++) {
            bufs.add(allocator.allocate(other, other));
        }
        assertEquals(2, source.segmentsAllocated(), "40 slices fit in the free ones");
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
    @Test
    void foreignReleasesEmptyASegmentThroughTheNotes() throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource();
        final AdaptivePoolingAllocator allocator = newAllocator(source, SEGMENT_SIZE);
        final int size = 65536;
        final int perChunk = 8 * SLICE_SIZE_BYTES / size;
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
        int perChunk = 8 * SLICE_SIZE_BYTES / size;
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
    @Test
    void segmentsOfAnEndedThreadGoBackWithTheirLastBuffer() throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final CountingSegmentSource source = new CountingSegmentSource();
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
     * carves its direct chunks out of segments too; heap buffers never. Regions wherever they can be mapped. The
     * memory accounted is a segment, with regions or not.
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
        boolean regions = MmapRegionSource.isAvailable();
        assertEquals(regions, AdaptiveByteBufAllocatorTest.directRegions(allocator));
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        assertNotNull(chunkOf(buf).segment);
        assertEquals(regions, chunkOf(buf).segment.region != null);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.metric().usedDirectMemory());
        ByteBuf heap = allocator.heapBuffer(1024, 1024);
        assertNull(chunkOf(heap).segment);
        assertEquals(AdaptivePoolingAllocator.MIN_CHUNK_SIZE, allocator.metric().usedHeapMemory());
        buf.release();
        heap.release();
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.metric().usedDirectMemory(),
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

    /** The allocator rejects a page store whose segments cannot hold its largest size-class chunk. */
    @Test
    void segmentsMustHoldTheLargestSizeClassChunk() {
        final CountingSegmentSource source = new CountingSegmentSource();
        assertThrows(IllegalArgumentException.class, () -> new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(512 * 1024, 8 * 1024, INTERVAL)), "576 KiB chunks do not fit");
    }
}
