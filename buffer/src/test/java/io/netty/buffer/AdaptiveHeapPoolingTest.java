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

import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.AdaptiveByteBufAllocatorTest.stripesWithABuddyMagazine;
import static io.netty.buffer.AdaptiveByteBufAllocatorTest.threadLocalIdleDecay;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Heap buffers: nothing above the size classes is pooled, and the size-class chunks stay under a cap, by default
 * the largest byte array G1 does not allocate as a humongous object. Direct buffers are pooled as before.
 */
public class AdaptiveHeapPoolingTest {
    private static final int[] SIZE_CLASSES = AdaptivePoolingAllocator.getSizeClasses();
    private static final int LARGEST_SIZE_CLASS = SIZE_CLASSES[SIZE_CLASSES.length - 1];
    private static final int KIB = 1024;
    private static final int MIB = 1024 * KIB;

    /** The cap the allocator computes for G1 regions of {@code regionSize} bytes. */
    private static int capFor(int regionSize) {
        return regionSize / 2 - AdaptivePoolingAllocator.BYTE_ARRAY_HEADER_ALLOWANCE - 1;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void heapBuffersAboveTheSizeClassesAreNotPooled(final boolean threadLocal) throws Throwable {
        final AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, true);
        onThread(threadLocal, () -> {
            for (int size : new int[] {LARGEST_SIZE_CLASS + 1, 512 * KIB, MIB}) {
                long before = allocator.usedHeapMemory();
                ByteBuf buf = allocator.heapBuffer(size, size);
                assertFalse(isPooled(buf), size + " bytes");
                assertEquals(size, chunkOf(buf).capacity, "a one-shot chunk of its own");
                assertEquals(before + size, allocator.usedHeapMemory(), "counted while its buffer lives");
                assertTrue(buf.release());
                assertEquals(before, allocator.usedHeapMemory(), "and freed with it: nothing pooled is left");
            }
            assertEquals(0, stripesWithABuddyMagazine(allocator, false), "no stripe of the heap allocator");
            if (threadLocal && !isLowMemory()) {
                // Low-memory mode has no thread-local heaps.
                assertNull(threadLocalIdleDecay(allocator, false).buddyMagazine, "nor the thread-local heap");
            }
            if (!isLowMemory()) {
                ByteBuf largest = allocator.heapBuffer(LARGEST_SIZE_CLASS, LARGEST_SIZE_CLASS);
                assertTrue(isPooled(largest), "the largest size class is pooled");
                assertTrue(chunkOf(largest) instanceof AdaptivePoolingAllocator.SizeClassedChunk);
                largest.release();
            }
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void directBuffersAboveTheSizeClassesArePooled(final boolean threadLocal) throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode does not pool buffers above its size classes");
        final AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, true);
        onThread(threadLocal, () -> {
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int size : new int[] {LARGEST_SIZE_CLASS + 1, 512 * KIB, MIB}) {
                ByteBuf buf = allocator.directBuffer(size, size);
                bufs.add(buf);
                assertTrue(isPooled(buf), size + " bytes");
                assertTrue(chunkOf(buf).capacity > size, "a block of a chunk that holds more");
            }
            if (threadLocal) {
                assertNotNull(threadLocalIdleDecay(allocator, true).buddyMagazine, "the thread-local heap's own");
                assertEquals(0, stripesWithABuddyMagazine(allocator, true));
            } else {
                assertEquals(1, stripesWithABuddyMagazine(allocator, true), "the stripe's");
            }
            for (ByteBuf buf : bufs) {
                buf.release();
            }
        });
    }

    /**
     * A heap buffer that grows past the largest size class moves to a one-shot chunk with its content; shrinking it
     * back below the size classes keeps it there, and growing it further replaces the one-shot chunk. The size-class
     * chunk it left stays pooled, and every one-shot chunk is freed when the buffer leaves it.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void heapBufferGrowsAndShrinksAcrossTheLargestSizeClass(final boolean threadLocal) throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode pools fewer size classes");
        final AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, true);
        onThread(threadLocal, () -> {
            ByteBuf buf = allocator.heapBuffer(KIB);
            long pooled;
            try {
                for (int i = 0; i < KIB; i++) {
                    buf.writeByte(i);
                }
                buf.capacity(LARGEST_SIZE_CLASS);
                assertTrue(isPooled(buf), "the largest size class");
                pooled = allocator.usedHeapMemory();

                buf.capacity(LARGEST_SIZE_CLASS + 1);
                assertFalse(isPooled(buf), "above the size classes");
                assertEquals(LARGEST_SIZE_CLASS + 1, buf.capacity());
                assertEquals(pooled + LARGEST_SIZE_CLASS + 1, allocator.usedHeapMemory());
                assertContent(buf, KIB);
                Object oneShot = chunkOf(buf);

                buf.capacity(KIB / 2);
                assertEquals(KIB / 2, buf.capacity());
                assertSame(oneShot, chunkOf(buf), "shrinking does not move the buffer");
                assertEquals(pooled + LARGEST_SIZE_CLASS + 1, allocator.usedHeapMemory());
                assertContent(buf, KIB / 2);
                buf.capacity(LARGEST_SIZE_CLASS + 1);
                assertSame(oneShot, chunkOf(buf), "nor does growing within the chunk");

                buf.capacity(300 * KIB);
                assertFalse(isPooled(buf));
                assertEquals(300 * KIB, chunkOf(buf).capacity);
                assertEquals(pooled + 300 * KIB, allocator.usedHeapMemory(), "the previous one-shot chunk is freed");
                assertContent(buf, KIB / 2);
            } finally {
                assertTrue(buf.release());
            }
            assertEquals(pooled, allocator.usedHeapMemory(), "only the size-class chunks are left");
        });
    }

    /** With 1 MiB regions the 512 and 528 KiB chunks become 256 and 272 KiB; larger regions change nothing. */
    @Test
    void heapChunkSizesForG1Regions() {
        int[] uncapped = AdaptivePoolingAllocator.sizeClassChunkSizes(0);
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            assertEquals(AdaptivePoolingAllocator.chunkSizeOf(SIZE_CLASSES[i]), uncapped[i]);
        }
        for (int region : new int[] {2 * MIB, 4 * MIB, 8 * MIB, 32 * MIB}) {
            assertArrayEquals(uncapped, AdaptivePoolingAllocator.sizeClassChunkSizes(capFor(region)),
                    region + " bytes regions");
        }
        int[] capped = AdaptivePoolingAllocator.sizeClassChunkSizes(capFor(MIB));
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            int expected = uncapped[i];
            if (expected == 512 * KIB) {
                expected = 256 * KIB;
            } else if (expected == 528 * KIB) {
                expected = 272 * KIB;
            }
            assertEquals(expected, capped[i], SIZE_CLASSES[i] + " bytes");
            assertTrue(capped[i] / SIZE_CLASSES[i] >= AdaptivePoolingAllocator.MIN_SEGMENTS_UNDER_CAP);
        }
    }

    /**
     * Every chunk buffer a capped heap allocator takes for its size classes is within the cap, whatever the size class,
     * and the used memory is what it holds from its chunk allocator.
     */
    @Test
    void heapChunksRespectTheCap() throws Exception {
        RecordingChunkAllocator chunks = new RecordingChunkAllocator();
        int cap = capFor(MIB);
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(chunks, false, false, cap);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            int size = SIZE_CLASSES[i];
            int chunkSize = allocator.chunkSizeOfClass(i);
            if (chunkSize == 0) {
                assertTrue(isLowMemory(), size + " bytes is pooled but in low-memory mode");
                continue;
            }
            assertTrue(chunkSize <= cap, size + " bytes: chunk " + chunkSize);
            // Two chunks' worth, so that the second chunk is the one a full first one leads to.
            for (int n = 0; n < 2 * (chunkSize / size); n++) {
                ByteBuf buf = allocator.allocate(size, size);
                assertTrue(isPooled(buf));
                assertEquals(chunkSize, chunkOf(buf).capacity, size + " bytes");
                bufs.add(buf);
            }
        }
        assertTrue(chunks.largest > 0);
        assertTrue(chunks.largest <= cap, "largest chunk buffer: " + chunks.largest);
        assertEquals(chunks.unreleasedBytes(), allocator.usedMemory());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertEquals(chunks.unreleasedBytes(), allocator.usedMemory());
    }

    /**
     * A size class with no chunk size under the cap that holds two of its buffers is not pooled; the others still
     * are, in smaller chunks. Each class is decided on its own: under a cap between 128 and 136 KiB the 4352-byte
     * class, whose chunks are 136 KiB, is not pooled, while the 8 KiB class above it is, in 128 KiB chunks.
     */
    @Test
    void sizeClassesThatDoNotFitUnderTheCapAreNotPooled() throws Exception {
        int[] between = AdaptivePoolingAllocator.sizeClassChunkSizes(135000);
        assertEquals(0, between[AdaptivePoolingAllocator.sizeClassIndexOf(4352)]);
        assertEquals(128 * KIB, between[AdaptivePoolingAllocator.sizeClassIndexOf(8 * KIB)]);

        RecordingChunkAllocator chunks = new RecordingChunkAllocator();
        int cap = 200000;
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(chunks, false, false, cap);
        assertEquals(128 * KIB, allocator.chunkSizeOfClass(AdaptivePoolingAllocator.sizeClassIndexOf(16 * KIB)));
        for (int size : new int[] {128 * KIB, LARGEST_SIZE_CLASS}) {
            assertEquals(0, allocator.chunkSizeOfClass(AdaptivePoolingAllocator.sizeClassIndexOf(size)));
            ByteBuf buf = allocator.allocate(size, size);
            assertFalse(isPooled(buf), size + " bytes");
            assertEquals(size, chunkOf(buf).capacity);
            assertEquals(chunks.unreleasedBytes(), allocator.usedMemory());
            buf.release();
        }
        ByteBuf small = allocator.allocate(16 * KIB, 16 * KIB);
        assertTrue(isPooled(small));
        assertEquals(128 * KIB, chunkOf(small).capacity);
        small.release();
        assertEquals(chunks.unreleasedBytes(), allocator.usedMemory());
        assertFalse(hasBuddyMagazine(allocator), "not pooled above the size classes either");
    }

    /**
     * A size class with no chunk size under the cap goes through the size-class routing like any other: its
     * magazine's slow path serves every buffer from a one-shot chunk, and the magazine never gets a chunk.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cappedOutHeapClassIsServedByTheSlowPathFromOneShotChunks(final boolean threadLocal) throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode does not pool 128 KiB buffers at all");
        final RecordingChunkAllocator chunks = new RecordingChunkAllocator();
        final AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(chunks, true, false, 200000);
        final int size = 128 * KIB;
        final int index = AdaptivePoolingAllocator.sizeClassIndexOf(size);
        assertEquals(0, allocator.chunkSizeOfClass(index));
        onThread(threadLocal, () -> {
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            // More buffers than a pooled chunk of this class would hold.
            for (int i = 0; i < 4 * AdaptivePoolingAllocator.MIN_SEGMENTS_UNDER_CAP; i++) {
                ByteBuf buf = allocator.allocate(size, size);
                assertFalse(isPooled(buf), "buffer " + i);
                assertEquals(size, chunkOf(buf).capacity, "a one-shot chunk of its own");
                assertEquals(i + 1, chunks.allocated.size(), "one chunk buffer per buffer, none for a chunk");
                bufs.add(buf);
            }
            Object magazine = sizeClassMagazine(allocator, index, threadLocal);
            assertNotNull(magazine, "routed through the size class's magazine");
            Field current = magazine.getClass().getDeclaredField("current");
            current.setAccessible(true);
            assertNull(current.get(magazine), "the magazine never has a chunk");
            assertEquals(chunks.unreleasedBytes(), allocator.usedMemory());
            for (ByteBuf buf : bufs) {
                assertTrue(buf.release());
            }
            assertEquals(0, allocator.usedMemory(), "freed with their buffers");
            assertEquals(0, chunks.unreleasedBytes());
        });
    }

    /** The magazine of a size class on the calling thread's thread-local heap, or on the stripe that has one. */
    private static Object sizeClassMagazine(AdaptivePoolingAllocator allocator, int index, boolean threadLocal)
            throws Exception {
        Object[] heaps;
        if (threadLocal) {
            Field tlField = AdaptivePoolingAllocator.class.getDeclaredField("threadLocalSizeClassHeap");
            tlField.setAccessible(true);
            heaps = new Object[] {((io.netty.util.concurrent.FastThreadLocal<?>) tlField.get(allocator)).get()};
        } else {
            Field stripesField = AdaptivePoolingAllocator.class.getDeclaredField("stripedHeaps");
            stripesField.setAccessible(true);
            heaps = (Object[]) stripesField.get(allocator);
        }
        for (Object heap : heaps) {
            Field magsField = heap.getClass().getDeclaredField("magazines");
            magsField.setAccessible(true);
            Object[] mags = (Object[]) magsField.get(heap);
            if (mags != null && mags[index] != null) {
                return mags[index];
            }
        }
        return null;
    }

    private static void assertContent(ByteBuf buf, int length) {
        for (int i = 0; i < length; i++) {
            assertEquals((byte) i, buf.getByte(i), "index " + i);
        }
    }

    private static AdaptivePoolingAllocator.Chunk chunkOf(ByteBuf buf) {
        while (!(buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf)) {
            buf = buf.unwrap();
        }
        return ((AdaptivePoolingAllocator.AdaptiveByteBuf) buf).chunk;
    }

    private static boolean isPooled(ByteBuf buf) throws Exception {
        Field pooled = AdaptivePoolingAllocator.Chunk.class.getDeclaredField("pooled");
        pooled.setAccessible(true);
        return pooled.getBoolean(chunkOf(buf));
    }

    private static boolean hasBuddyMagazine(AdaptivePoolingAllocator allocator) throws Exception {
        Field stripesField = AdaptivePoolingAllocator.class.getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        for (Object stripe : (Object[]) stripesField.get(allocator)) {
            Field magField = stripe.getClass().getDeclaredField("buddyMagazine");
            magField.setAccessible(true);
            if (magField.get(stripe) != null) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLowMemory() throws Exception {
        Field f = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        f.setAccessible(true);
        return f.getBoolean(null);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code body} on a thread with a thread-local heap, or on a plain thread, which allocates on a stripe. */
    private static void onThread(boolean threadLocal, final ThrowingRunnable body) throws Throwable {
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Runnable task = () -> {
            try {
                body.run();
            } catch (Throwable t) {
                failure.set(t);
            }
        };
        Thread thread = threadLocal ? new FastThreadLocalThread(task) : new Thread(task);
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    private static final class RecordingChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator {
        private final List<AbstractByteBuf> allocated = new ArrayList<AbstractByteBuf>();
        /** The largest chunk buffer allocated. */
        int largest;

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            AbstractByteBuf buf =
                    new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, initialCapacity, maxCapacity);
            allocated.add(buf);
            largest = Math.max(largest, initialCapacity);
            return buf;
        }

        long unreleasedBytes() {
            long bytes = 0;
            for (AbstractByteBuf buf : allocated) {
                if (buf.refCnt() > 0) {
                    bytes += buf.capacity();
                }
            }
            return bytes;
        }
    }
}
