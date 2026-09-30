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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.AdaptiveByteBufAllocatorTest.stripesWithABuddyMagazine;
import static io.netty.buffer.AdaptiveByteBufAllocatorTest.threadLocalIdleDecay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Heap buffers: nothing above the size classes is pooled. Direct buffers are pooled as before.
 */
public class AdaptiveHeapPoolingTest {
    private static final int[] SIZE_CLASSES = AdaptivePoolingAllocator.getSizeClasses();
    private static final int LARGEST_SIZE_CLASS = SIZE_CLASSES[SIZE_CLASSES.length - 1];
    private static final int KIB = 1024;
    private static final int MIB = 1024 * KIB;

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
            if (threadLocal) {
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
}
