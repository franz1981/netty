/*
 * Copyright 2024 The Netty Project
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

import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.test.DisabledForSlowLeakDetection;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.buffer.AdaptivePoolingAllocator.IdleDecay;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunkCache;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import io.netty.buffer.AbstractByteBufTest.TestGatheringByteChannel;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AdaptiveByteBufAllocatorTest extends AbstractByteBufAllocatorTest<AdaptiveByteBufAllocator> {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    @Override
    protected AdaptiveByteBufAllocator newAllocator(boolean preferDirect) {
        return closer.add(new AdaptiveByteBufAllocator(preferDirect));
    }

    @Override
    protected AdaptiveByteBufAllocator newUnpooledAllocator() {
        return newAllocator(false);
    }

    @Override
    protected long expectedUsedMemory(AdaptiveByteBufAllocator allocator, int capacity) {
        return PageStoreTestSupport.chunkSizeOf(capacity, direct(allocator).pageStore.config);
    }

    @Override
    protected long expectedUsedMemoryAfterRelease(AdaptiveByteBufAllocator allocator, int capacity) {
        return expectedUsedMemory(allocator, capacity);
    }

    @Override
    @Test
    public void testUnsafeHeapBufferAndUnsafeDirectBuffer() {
        AdaptiveByteBufAllocator allocator = newUnpooledAllocator();
        ByteBuf directBuffer = allocator.directBuffer();
        assertInstanceOf(directBuffer, AdaptivePoolingAllocator.AdaptiveByteBuf.class);
        assertTrue(directBuffer.isDirect());
        directBuffer.release();

        ByteBuf heapBuffer = allocator.heapBuffer();
        assertInstanceOf(heapBuffer, AdaptivePoolingAllocator.AdaptiveByteBuf.class);
        assertFalse(heapBuffer.isDirect());
        heapBuffer.release();
    }

    /**
     * Direct size-class chunks are spans of a segment, which is the memory accounted: the first buffer takes a whole
     * segment, and the chunk of the next size class is another span of it. With shared slices, the memory accounted
     * is the chunks' slices.
     */
    @Override
    @Test
    public void testUsedDirectMemory() {
        AdaptiveByteBufAllocator allocator =  newAllocator(true);
        ByteBufAllocatorMetric metric = allocator.metric();
        assertEquals(0, metric.usedDirectMemory());
        ByteBuf buffer = allocator.directBuffer(1024, 4096);
        // mmap'd regions count the chunks' slices; malloc'd ones, the region.
        boolean perSegment = !directSharesSlices(allocator);
        long unit = direct(allocator).pageStore.config.regionSize;
        try {
            int capacity = buffer.capacity();
            long first = perSegment ? unit : expectedUsedMemory(allocator, capacity);
            assertEquals(first, metric.usedDirectMemory());

            // Double the size of the buffer
            buffer.capacity(capacity << 1);
            capacity = buffer.capacity();
            // This is a new size class, and a new magazine with a new chunk: another span of the same segment.
            long both = perSegment ? unit : 2 * expectedUsedMemory(allocator, capacity);
            assertEquals(both, metric.usedDirectMemory(), buffer.toString());
        } finally {
            buffer.release();
        }
        // Memory is still held by the magazines
        assertEquals(perSegment ? unit : expectedUsedMemory(allocator, 1024) + expectedUsedMemory(allocator, 2048),
                metric.usedDirectMemory());
    }

    static AdaptivePoolingAllocator direct(AdaptiveByteBufAllocator allocator) {
        try {
            Field directField = AdaptiveByteBufAllocator.class.getDeclaredField("direct");
            directField.setAccessible(true);
            return (AdaptivePoolingAllocator) directField.get(allocator);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** The bytes of a heap chunk of {@code size}'s class under the heap defaults. */
    private static int heapChunkSizeOf(int size) {
        return PageStoreTestSupport.chunkSizeOf(size, PageStoreConfig.heapDefaults());
    }

    /** The size of the direct allocator's blocks. */
    static int directSegmentSize(AdaptiveByteBufAllocator allocator) {
        return direct(allocator).pageStore.config.segmentSize;
    }

    /** Whether the direct allocator's regions are mmap'd: their slices count from their first claim. */
    static boolean directSharesSlices(AdaptiveByteBufAllocator allocator) {
        return direct(allocator).pageStore.mmap != null;
    }

    static AdaptivePoolingAllocator heap(AdaptiveByteBufAllocator allocator) {
        try {
            Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
            heapField.setAccessible(true);
            return (AdaptivePoolingAllocator) heapField.get(allocator);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** The size of the heap allocator's blocks. */
    static int heapSegmentSize(AdaptiveByteBufAllocator allocator) {
        return heap(allocator).pageStore.config.segmentSize;
    }

    /**
     * Whether the heap allocator pools buffers of {@code size}: up to a block, which G1 regions below 16 MiB make
     * smaller than 4 MiB (see {@link PageStoreConfig#heapSegmentSizeOf}).
     */
    static boolean heapSegmentsHold(AdaptiveByteBufAllocator allocator, int size) {
        return size <= heapSegmentSize(allocator);
    }

    @Override
    @Test
    public void shouldReuseChunks() throws Exception {
        assumeTrue(heapSegmentsHold(newAllocator(false), 1024 * 1024), "1 MiB heap buffers are not pooled");
        super.shouldReuseChunks();
    }

    /** As {@link #testUsedDirectMemory}: heap segments are accounted whole, as direct ones. */
    @Override
    @Test
    public void testUsedHeapMemory() {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(true, false));
        ByteBufAllocatorMetric metric = allocator.metric();
        assertEquals(0, metric.usedHeapMemory());
        int segmentSize = heapSegmentSize(allocator);
        ByteBuf buffer = allocator.heapBuffer(1024, 4096);
        int capacity = buffer.capacity();
        assertEquals(segmentSize, metric.usedHeapMemory());

        // Double the size of the buffer
        buffer.capacity(capacity << 1);
        capacity = buffer.capacity();
        // This is a new size class, and a new magazine with a new chunk: another span of the same segment.
        long both = segmentSize;
        assertEquals(both, metric.usedHeapMemory(), buffer.toString());

        buffer.release();
        // Memory is still held by the magazines
        assertEquals(both, metric.usedHeapMemory());
        assertEquals(0, metric.usedDirectMemory());
    }

    /**
     * Buffers above the largest pooled size, in low-memory mode, get a one-shot chunk of
     * their own, of their exact size: accounted while the buffer lives, replaced on growth with the content kept, and
     * freed as soon as the buffer is released.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void oneShotChunkIsFreedWithItsBuffer(boolean direct) throws Exception {
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        assumeTrue(isLowMemory(), "see buffersUpToABlockAreSpans");
        ByteBufAllocatorMetric metric = allocator.metric();
        int size = 2200000; // above the largest pooled size
        ByteBuf buffer = direct ? allocator.directBuffer(size, Integer.MAX_VALUE) :
                allocator.heapBuffer(size, Integer.MAX_VALUE);
        assertEquals(size, buffer.capacity());
        assertEquals(size, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
        buffer.writeLong(0x0123456789ABCDEFL);
        buffer.setLong(size - 8, 0xFEDCBA9876543210L);

        buffer.capacity(2 * size);
        assertEquals(2 * size, buffer.capacity());
        // The first chunk was freed when the buffer moved to the second.
        assertEquals(2 * size, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
        assertEquals(0x0123456789ABCDEFL, buffer.getLong(0));
        assertEquals(0xFEDCBA9876543210L, buffer.getLong(size - 8));

        assertTrue(buffer.release());
        assertEquals(0, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
    }

    /**
     * A buffer above half a block and up to a block is a span of whole slices: counted by its
     * slices in {@code mmap}'d regions, by its whole block in regions of one block. Grown to a whole block, it moves
     * to another block; the first span goes back free and stays counted, as the second does once released, until a
     * purge.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void buffersUpToABlockAreSpans(boolean direct) throws Exception {
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        assumeFalse(isLowMemory(), "low-memory mode: no spans above the size classes");
        PageStore store = (direct ? direct(allocator) : heap(allocator)).pageStore;
        int block = store.config.segmentSize;
        int slice = store.config.sliceSize;
        int size = block / 4 * 3; // above half a block
        long span = (size + slice - 1) / slice * (long) slice;
        boolean perSlice = store.mmap != null;
        ByteBufAllocatorMetric metric = allocator.metric();
        ByteBuf buffer = direct ? allocator.directBuffer(size, Integer.MAX_VALUE) :
                allocator.heapBuffer(size, Integer.MAX_VALUE);
        assertEquals(size, buffer.capacity());
        assertEquals(perSlice ? span : block, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
        buffer.writeLong(0x0123456789ABCDEFL);
        buffer.setLong(size - 8, 0xFEDCBA9876543210L);

        buffer.capacity(block);
        assertEquals(block, buffer.capacity());
        long both = perSlice ? span + block : 2L * block;
        assertEquals(both, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
        assertEquals(0x0123456789ABCDEFL, buffer.getLong(0));
        assertEquals(0xFEDCBA9876543210L, buffer.getLong(size - 8));

        assertTrue(buffer.release());
        assertEquals(both, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
    }

    @Test
    void adaptiveChunkMustDeallocateOrReuseWthBufferRelease() throws Exception {
        AdaptiveByteBufAllocator allocator = newAllocator(false);
        Deque<ByteBuf> bufs = new ArrayDeque<>();
        assertEquals(0, allocator.usedHeapMemory());
        assertEquals(0, allocator.usedHeapMemory());
        bufs.add(allocator.heapBuffer(256));
        // Counted in the slices chunks hold: the used memory is whole blocks.
        long usedHeapMemory = claimedHeapBytes(allocator);
        int buffersPerChunk = segmentsOf(chunkOf(bufs.peek()));
        for (int i = 0; i < buffersPerChunk; i++) {
            bufs.add(allocator.heapBuffer(256));
        }
        assertEquals(2 * usedHeapMemory, claimedHeapBytes(allocator));
        bufs.pop().release();
        assertEquals(2 * usedHeapMemory, claimedHeapBytes(allocator));
        while (!bufs.isEmpty()) {
            bufs.pop().release();
        }
        assertEquals(2 * usedHeapMemory, claimedHeapBytes(allocator));
        for (int i = 0; i < 2 * buffersPerChunk; i++) {
            bufs.add(allocator.heapBuffer(256));
        }
        assertEquals(2 * usedHeapMemory, claimedHeapBytes(allocator));
        while (!bufs.isEmpty()) {
            bufs.pop().release();
        }
    }

    @Test
    public void getBytesBoundaryCheckWithFileChannel() {
        AdaptiveByteBufAllocator allocator = newAllocator(false);
        final ByteBuf buf = allocator.directBuffer(7);
        try {
            assertThrows(IndexOutOfBoundsException.class, new Executable() {
                @Override
                public void execute() throws IOException {
                    // capacity 7，4+8=12 <= maxFastCapacity 32
                    buf.getBytes(4, (FileChannel) null, 0L, 8);
                }
            });
        } finally {
            buf.release();
        }
    }

    @Test
    public void testGetBytesBoundaryCheckWithGatheringByteChannel() throws Exception {
        AdaptiveByteBufAllocator allocator = newAllocator(false);
        TestGatheringByteChannel channel = new TestGatheringByteChannel();
        final ByteBuf buf = allocator.directBuffer(7);
        try {
            assertThrows(IndexOutOfBoundsException.class, new Executable() {
                @Override
                public void execute() throws IOException {
                    // capacity 7，4+8=12 <= maxFastCapacity 32
                    buf.getBytes(4, channel, 8);
                }
            });
        } finally {
            channel.close();
            buf.release();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void sliceOrDuplicateUnwrapLetNotEscapeRootParent(boolean slice) {
        AdaptiveByteBufAllocator allocator = newAllocator(false);
        ByteBuf buffer = allocator.buffer(8);
        assertInstanceOf(buffer, AdaptivePoolingAllocator.AdaptiveByteBuf.class);
        // Unwrap if this is wrapped by a leak aware buffer.
        if (buffer instanceof SimpleLeakAwareByteBuf) {
            assertNull(buffer.unwrap().unwrap());
        } else {
            assertNull(buffer.unwrap());
        }

        ByteBuf derived = slice ? buffer.slice(0, 4) : buffer.duplicate();
        // When we unwrap the derived buffer we should get our original buffer of type AdaptiveByteBuf back.
        ByteBuf unwrapped = derived instanceof SimpleLeakAwareByteBuf ?
                derived.unwrap().unwrap() : derived.unwrap();
        assertInstanceOf(unwrapped, AdaptivePoolingAllocator.AdaptiveByteBuf.class);
        assertSameBuffer(buffer instanceof SimpleLeakAwareByteBuf ? buffer.unwrap() : buffer, unwrapped);

        ByteBuf retainedDerived = slice ? buffer.retainedSlice(0, 4) : buffer.retainedDuplicate();
        // When we unwrap the derived buffer we should get our original buffer of type AdaptiveByteBuf back.
        ByteBuf unwrappedRetained = retainedDerived instanceof SimpleLeakAwareByteBuf ?
                retainedDerived.unwrap().unwrap() :  retainedDerived.unwrap();
        assertInstanceOf(unwrappedRetained, AdaptivePoolingAllocator.AdaptiveByteBuf.class);
        assertSameBuffer(buffer instanceof SimpleLeakAwareByteBuf ? buffer.unwrap() : buffer, unwrappedRetained);
        retainedDerived.release();

        assertTrue(buffer.release());
    }

    @Test
    public void testAllocateWithoutLock() throws InterruptedException {
        final AdaptiveByteBufAllocator alloc = closer.add(new AdaptiveByteBufAllocator());
        // Make `threadCount` bigger than `AdaptivePoolingAllocator.MAX_STRIPES`, to let thread collision easily happen.
        int threadCount = NettyRuntime.availableProcessors() * 4;
        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<Throwable>();
        for (int i = 0; i < threadCount; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 1024; j++) {
                        try {
                            ByteBuf buffer = null;
                            try {
                                buffer = alloc.heapBuffer(128);
                                buffer.ensureWritable(ThreadLocalRandom.current().nextInt(512, 32769));
                            } finally {
                                if (buffer != null) {
                                    buffer.release();
                                }
                            }
                        } catch (Throwable t) {
                            throwableAtomicReference.set(t);
                        }
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Throwable throwable = throwableAtomicReference.get();
        if (throwable != null) {
            fail("Expected no exception, but got", throwable);
        }
    }

    /**
     * Spans released on another thread are usable again for the thread that allocates: a second round of the same
     * allocations after a foreign release needs no new memory.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void largeBuffersReleasedByAnotherThreadAreReused(boolean direct) throws Exception {
        final AdaptiveByteBufAllocator allocator = newAllocator(true);
        ByteBufAllocatorMetric metric = allocator.metric();
        final int size = 512 * 1024; // above the largest size class, below the unpooled fallback
        final ByteBuf[] bufs = new ByteBuf[24];
        for (int i = 0; i < bufs.length; i++) {
            bufs[i] = direct ? allocator.directBuffer(size, size) : allocator.heapBuffer(size, size);
        }
        long used = direct ? metric.usedDirectMemory() : metric.usedHeapMemory();
        Thread releaser = new Thread(() -> {
            for (ByteBuf buf : bufs) {
                buf.release();
            }
        });
        releaser.start();
        releaser.join();
        for (int i = 0; i < bufs.length; i++) {
            bufs[i] = direct ? allocator.directBuffer(size, size) : allocator.heapBuffer(size, size);
        }
        assertEquals(used, direct ? metric.usedDirectMemory() : metric.usedHeapMemory());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }

    /**
     * Idle memory above the size classes goes back: after a burst of large buffers is released, no more than a block
     * is held, without any further allocation, once the page store purged what stayed free for its delay.
     */
    @Test
    void idleLargeBufferMemoryGoesBack() {
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        PageStore store = heap(allocator).pageStore;
        int size = 1024 * 1024; // the largest pooled size
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        while (allocator.usedHeapMemory() < 32 * 1024 * 1024) {
            bufs.add(allocator.heapBuffer(size, size));
        }
        long peak = allocator.usedHeapMemory();
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        // No allocation follows: a heap that goes quiet must not keep the burst.
        store.purgeIfDue(System.nanoTime() + 2 * store.config.purgeDelayNanos);
        long settled = allocator.usedHeapMemory();
        assertTrue(settled <= store.config.segmentSize, "peak " + peak + ", settled " + settled);
    }

    /**
     * Several threads allocate buffers above the size classes and hand them to each other to check and release, so
     * spans go back from threads that did not allocate them while the stripe's magazine keeps allocating: every buffer
     * keeps its content until it is released, and nothing fails (run with assertions on).
     */
    @DisabledForSlowLeakDetection
    @Test
    void largeBuffersReleasedAcrossThreadsKeepTheirContent() throws Throwable {
        final AdaptiveByteBufAllocator allocator = newAllocator(true);
        final int[] sizes = {140 * 1024, 256 * 1024, 300 * 1024, 512 * 1024, 700 * 1024, 1024 * 1024};
        final int threads = 8;
        final int rounds = 3000;
        final BlockingQueue<ByteBuf> handoff = new ArrayBlockingQueue<ByteBuf>(64);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        List<Thread> workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            final int seed = t;
            Thread worker = new Thread(() -> {
                SplittableRandom rng = new SplittableRandom(seed);
                try {
                    for (int i = 0; i < rounds && failure.get() == null; i++) {
                        int size = sizes[rng.nextInt(sizes.length)];
                        ByteBuf buf = rng.nextBoolean() ? allocator.heapBuffer(size, size) :
                                allocator.directBuffer(size, size);
                        byte mark = (byte) rng.nextInt();
                        buf.writerIndex(size);
                        buf.setByte(0, mark);
                        buf.setByte(size - 1, mark);
                        buf.setByte(size / 2, mark);
                        if (!handoff.offer(buf)) {
                            buf.release();
                        }
                        ByteBuf other = handoff.poll();
                        if (other != null) {
                            int n = other.capacity();
                            byte m = other.getByte(0);
                            assertEquals(m, other.getByte(n - 1));
                            assertEquals(m, other.getByte(n / 2));
                            other.release();
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        ByteBuf left;
        while ((left = handoff.poll()) != null) {
            left.release();
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    /**
     * Spans given back are reused: allocating and releasing the same set of large buffers over and over from one
     * thread does not grow the memory held after the first round.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void largeBuffersAreReusedAcrossRounds(boolean direct) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode does not pool buffers above its size classes");
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        ByteBufAllocatorMetric metric = allocator.metric();
        int size = 512 * 1024; // above the largest size class, below the unpooled fallback
        assumeTrue(direct || heapSegmentsHold(allocator, size), "heap buffers above a heap segment are not pooled");
        ByteBuf[] bufs = new ByteBuf[24];
        long afterFirstRound = -1;
        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < bufs.length; i++) {
                bufs[i] = direct ? allocator.directBuffer(size, size) : allocator.heapBuffer(size, size);
            }
            for (ByteBuf buf : bufs) {
                buf.release();
            }
            long used = direct ? metric.usedDirectMemory() : metric.usedHeapMemory();
            if (afterFirstRound < 0) {
                afterFirstRound = used;
                // The released spans' blocks stay until they are idle for the purge delay.
                assertTrue(used >= (long) size * bufs.length, "used " + used);
            } else {
                assertEquals(afterFirstRound, used, "round " + round);
            }
        }
    }

    @DisabledForSlowLeakDetection
    @RepeatedTest(100)
    void largeAllocationConsistency(RepetitionInfo info) {
        SplittableRandom rng = new SplittableRandom(info.getCurrentRepetition());
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        int small = 256 * 1024; // above the largest size class: every size here is a span or a one-shot
        int large = 2 * small;
        int xlarge = 2 * large;

        int[] allocationSizes = {
                small, small, small, small, small, small, small, small,
                large, large, large, large,
                xlarge, xlarge,
        };

        shuffle(rng, allocationSizes);

        ByteBuf[] bufs = new ByteBuf[allocationSizes.length];
        Arrays.setAll(bufs, i -> allocator.buffer(allocationSizes[i], allocationSizes[i]));

        shuffle(rng, bufs);

        int[] reallocations = new int[bufs.length / 2];
        for (int i = 0; i < reallocations.length; i++) {
            reallocations[i] = bufs[i].capacity();
            bufs[i].release();
            bufs[i] = null;
        }
        for (int i = 0; i < reallocations.length; i++) {
            assertNull(bufs[i]);
            bufs[i] = allocator.buffer(reallocations[i], reallocations[i]);
        }

        for (int i = 0; i < bufs.length; i++) {
            while (bufs[i].isWritable()) {
                bufs[i].writeByte(i + 1);
            }
        }
        try {
            for (int i = 0; i < bufs.length; i++) {
                while (bufs[i].isReadable()) {
                    int b = Byte.toUnsignedInt(bufs[i].readByte());
                    if (b != i + 1) {
                        fail("Expected byte " + (i + 1) +
                                " at index " + (bufs[i].readerIndex() - 1) +
                                " but got " + b);
                    }
                }
            }
        } finally {
            for (ByteBuf buf : bufs) {
                buf.release();
            }
        }
    }

    /**
     * The chunk a size-class magazine allocates from must keep serving it after becoming fully free, whichever path
     * the return takes and whatever the drain and the purge do, even with the cache above its retention floor.
     *
     * <ul>
     *   <li>{@code owner}: thread-local heap, released by its owner thread (inline, no lock).</li>
     *   <li>{@code locked}: shared stripe, released by another thread that wins the stripe lock.</li>
     *   <li>{@code notified}: thread-local heap, released by another thread, which leaves a note that the
     *       owner drains.</li>
     * </ul>
     */
    @ParameterizedTest
    @ValueSource(strings = {"owner", "locked", "notified"})
    void activeChunkKeepsServingAllocationsWhenFullyFreeAboveTheFloor(String releasePath) throws Exception {
        final boolean threadLocal = !"locked".equals(releasePath);
        final boolean foreignRelease = !"owner".equals(releasePath);
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, threadLocal));
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Runnable test = () -> {
            try {
                assertActiveChunkKeepsServingAllocations(allocator, !threadLocal, foreignRelease);
            } catch (Throwable t) {
                failure.set(t);
            }
        };
        if (threadLocal) {
            FastThreadLocalThread.runWithFastThreadLocal(test);
        } else {
            test.run();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }

    private static void assertActiveChunkKeepsServingAllocations(
            AdaptiveByteBufAllocator allocator, boolean sharedStripe, boolean foreignRelease) throws Exception {
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        try {
            // Hand out every segment of the first chunk: the allocation after that lands in another chunk.
            for (int i = 0; i < BURST_SEGMENTS_PER_CHUNK; i++) {
                held.add(allocator.heapBuffer(BURST_BUF_SIZE));
            }
            SizeClassedChunk first = chunkOf(held.get(0));
            for (ByteBuf buf : held) {
                assertSame(first, chunkOf(buf));
            }
            final SizeClassedChunkCache cache = first.owningCache;

            // Fill the cache above its retention floor with chunks that have no free segment.
            int floor = 4; // the empty chunks a size class keeps
            for (int i = BURST_SEGMENTS_PER_CHUNK; i < (floor + 1) * BURST_SEGMENTS_PER_CHUNK; i++) {
                held.add(allocator.heapBuffer(BURST_BUF_SIZE));
            }
            assertEquals((long) (floor + 1) * BURST_CHUNK_SIZE, claimedHeapBytes(allocator));

            // One segment of a fresh chunk, returned: that chunk is fully free, above the floor.
            ByteBuf probe = allocator.heapBuffer(BURST_BUF_SIZE);
            SizeClassedChunk active = chunkOf(probe);
            long used = claimedHeapBytes(allocator);
            assertEquals((long) (floor + 2) * BURST_CHUNK_SIZE, used);
            release(probe, foreignRelease);
            underStripeLocks(allocator, sharedStripe, cache::drainPending);
            assertEquals(used, claimedHeapBytes(allocator),
                    "a fully free active chunk must not be evicted on release");
            underStripeLocks(allocator, sharedStripe, cache::tickPurge);
            assertEquals(used, claimedHeapBytes(allocator),
                    "a fully free active chunk must not be evicted by the purge");
            // The one representation check: it is still the cache's active chunk.
            assertSame(active, cache.active);

            ByteBuf next = allocator.heapBuffer(BURST_BUF_SIZE);
            held.add(next);
            assertSame(active, chunkOf(next), "the next allocation must land in the same chunk");
            assertEquals(used, claimedHeapBytes(allocator));

            // Control: in the same state, a fully free chunk that is not active is evicted, so the assertions
            // above are about the active chunk and not about a cache that would evict nothing. Its slices go back to
            // the page store.
            for (int i = 0; i < BURST_SEGMENTS_PER_CHUNK; i++) {
                release(held.get(i), foreignRelease);
            }
            held.subList(0, BURST_SEGMENTS_PER_CHUNK).clear();
            underStripeLocks(allocator, sharedStripe, cache::drainPending);
            assertEquals(used - BURST_CHUNK_SIZE, claimedHeapBytes(allocator), "evicted");
        } finally {
            for (ByteBuf buf : held) {
                buf.release();
            }
        }
    }

    // --- Where a note is applied -----------------------------------------------------------------------------
    //
    // A release that cannot apply itself - another thread's, on a thread-local heap or while the stripe lock is
    // taken - puts the segment or block on its chunk's free list and leaves a note for the chunk's cache. The tests
    // below pin each place that applies notes, and set things up so that nothing else could have: a size class that
    // went idle never takes its own slow path again, so its notes wait for another size class's slow path or for a
    // purge tick of its heap.

    private static final int NOTE_ALLOCATING_SIZE = 4096;
    private static final int NOTE_IDLE_SIZE = 1024;
    /** The chunks {@link #leaveNotes} keeps in use: one more than the empty chunks a size class keeps. */
    private static final int NOTE_KEPT_CHUNKS = 5;

    /** A size class left idle with notes outstanding on two of its chunks; see {@link #leaveNotes}. */
    private static final class IdleSizeClass {
        final SizeClassedChunkCache cache;
        final List<ByteBuf> stillHeld;
        final int chunkSize;

        IdleSizeClass(SizeClassedChunkCache cache, List<ByteBuf> stillHeld, int chunkSize) {
            this.cache = cache;
            this.stillHeld = stillHeld;
            this.chunkSize = chunkSize;
        }

        /** The two emptied chunks left the cache, which keeps the ones still in use. */
        void assertNotesApplied(String when) {
            assertEquals(0, cache.pendingCount(), when + ": the notes must be applied");
            assertEquals(0, cache.reusable.size, when);
            assertEquals(NOTE_KEPT_CHUNKS, cache.exhausted.size, when + ": only the chunks still in use stay");
        }

        void releaseRest() {
            for (ByteBuf buf : stillHeld) {
                buf.release();
            }
        }
    }

    /**
     * Fill {@link #NOTE_KEPT_CHUNKS} + 2 chunks of {@link #NOTE_IDLE_SIZE} exactly, so all are filed as exhausted,
     * then release the buffers of the first two from another thread that cannot apply the release: on a thread-local
     * heap it is not the owner, and on a stripe it runs while the stripe lock is held. Each of the two chunks gets one
     * note.
     */
    private static IdleSizeClass leaveNotes(AdaptiveByteBufAllocator allocator, boolean sharedStripe)
            throws Exception {
        int chunkSize = heapChunkSizeOf(NOTE_IDLE_SIZE);
        final List<ByteBuf> released = new ArrayList<ByteBuf>();
        List<ByteBuf> stillHeld = new ArrayList<ByteBuf>();
        released.add(allocator.heapBuffer(NOTE_IDLE_SIZE, NOTE_IDLE_SIZE));
        int perChunk = segmentsOf(chunkOf(released.get(0)));
        int chunks = NOTE_KEPT_CHUNKS + 2;
        for (int i = 1; i < chunks * perChunk; i++) {
            ByteBuf buf = allocator.heapBuffer(NOTE_IDLE_SIZE, NOTE_IDLE_SIZE);
            (i < 2 * perChunk ? released : stillHeld).add(buf);
        }
        SizeClassedChunkCache cache = chunkOf(stillHeld.get(0)).owningCache;
        assertEquals(chunks, cache.exhausted.size, "full chunks only");
        underStripeLocks(allocator, sharedStripe, () -> {
            try {
                Thread t = new Thread(() -> {
                    for (ByteBuf buf : released) {
                        buf.release();
                    }
                });
                t.start();
                t.join();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        assertEquals(2, cache.pendingCount(), "one note per emptied chunk");
        assertEquals(chunks, cache.exhausted.size, "nothing applied the notes yet");
        return new IdleSizeClass(cache, stillHeld, chunkSize);
    }

    /** Run on the thread that owns a thread-local heap, or on a plain thread that allocates from a stripe. */
    private static void onHeapThread(boolean threadLocal, final ThrowingRunnable body) throws Exception {
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
            throw new AssertionError(failure.get());
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * The slow path of one size class applies the notes of every other size class of its heap, so the notes of a
     * size class that went idle are applied by the first allocation of another one. The chunk that allocation needs
     * then takes slices the idle size class just gave up: no memory is allocated.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void anotherSizeClassSlowPathAppliesTheNotesOfAnIdleOne(final boolean threadLocal) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, threadLocal));
        onHeapThread(threadLocal, () -> {
            IdleSizeClass idle = leaveNotes(allocator, !threadLocal);
            ByteBuf first = null;
            try {
                long used = allocator.usedHeapMemory();
                assertEquals(heapChunkSizeOf(NOTE_ALLOCATING_SIZE), idle.chunkSize,
                        "both size classes must share a chunk size");

                first = allocator.heapBuffer(NOTE_ALLOCATING_SIZE, NOTE_ALLOCATING_SIZE);
                idle.assertNotesApplied("after another size class's slow path");
                assertEquals(used, allocator.usedHeapMemory(), "the new chunk must take slices given up");
            } finally {
                if (first != null) {
                    first.release();
                }
                idle.releaseRest();
            }
        });
    }

    /**
     * A size class that keeps allocating from its active chunk never takes its slow path, so the notes of an idle
     * size class of the same heap wait for its purge tick: exactly {@code chunkPurgeInterval} chunks' worth of its
     * allocations, not one fewer.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void purgeTickAppliesTheNotesOfAnIdleSizeClassAtItsInterval(final boolean threadLocal) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, threadLocal));
        onHeapThread(threadLocal, () -> {
            // The allocating size class gets its active chunk first: from here on, allocating and releasing one
            // buffer at a time never runs it out of segments, so it never takes its slow path again.
            allocator.heapBuffer(NOTE_ALLOCATING_SIZE, NOTE_ALLOCATING_SIZE).release();
            IdleSizeClass idle = leaveNotes(allocator, !threadLocal);
            try {
                long used = allocator.usedHeapMemory();
                int interval = (int) AdaptivePoolingAllocator.CHUNK_PURGE_INTERVAL
                        * (heapChunkSizeOf(NOTE_ALLOCATING_SIZE) / NOTE_ALLOCATING_SIZE);

                // One allocation is counted already; the tick comes with the interval-th.
                for (int i = 1; i < interval - 1; i++) {
                    allocator.heapBuffer(NOTE_ALLOCATING_SIZE, NOTE_ALLOCATING_SIZE).release();
                }
                assertEquals(2, idle.cache.pendingCount(),
                        "one allocation before the tick, the notes must still wait");

                allocator.heapBuffer(NOTE_ALLOCATING_SIZE, NOTE_ALLOCATING_SIZE).release();
                idle.assertNotesApplied("after the purge tick of another size class");
                assertEquals(used, allocator.usedHeapMemory(), "slices given back, memory not freed");
            } finally {
                idle.releaseRest();
            }
        });
    }

    /** The {@link IdleDecay} of the calling thread's thread-local heap. */
    private static IdleDecay threadLocalIdleDecay(AdaptiveByteBufAllocator allocator) throws Exception {
        Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        heapField.setAccessible(true);
        Object pooling = heapField.get(allocator);
        Field tlField = pooling.getClass().getDeclaredField("threadLocalSizeClassHeap");
        tlField.setAccessible(true);
        Object heap = ((io.netty.util.concurrent.FastThreadLocal<?>) tlField.get(pooling)).get();
        Field decayField = heap.getClass().getDeclaredField("idleDecay");
        decayField.setAccessible(true);
        return (IdleDecay) decayField.get(heap);
    }

    private static final int BUDDY_NOTE_SIZE = 512 * 1024;

    /** Runs {@code body} on a new thread whose heap is thread-local, freed when the body returns. */
    private static void onThreadLocalHeap(final ThrowingRunnable body) throws Throwable {
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread thread = new Thread(() -> FastThreadLocalThread.runWithFastThreadLocal(() -> {
            try {
                body.run();
            } catch (Throwable t) {
                failure.set(t);
            }
        }));
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    /** How many stripes created a magazine for buffers above the size classes. */
    private static int stripesWithASpanMagazine(AdaptiveByteBufAllocator allocator) throws Exception {
        Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        heapField.setAccessible(true);
        Object pooling = heapField.get(allocator);
        Field stripesField = pooling.getClass().getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        int stripes = 0;
        for (Object stripe : (Object[]) stripesField.get(pooling)) {
            Field magField = stripe.getClass().getDeclaredField("spanMagazine");
            magField.setAccessible(true);
            if (magField.get(stripe) != null) {
                stripes++;
            }
        }
        return stripes;
    }

    /** The calling thread's thread-local heap. */
    private static Object threadLocalHeap(AdaptiveByteBufAllocator allocator) throws Exception {
        Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        heapField.setAccessible(true);
        Object pooling = heapField.get(allocator);
        Field tlField = pooling.getClass().getDeclaredField("threadLocalSizeClassHeap");
        tlField.setAccessible(true);
        return ((io.netty.util.concurrent.FastThreadLocal<?>) tlField.get(pooling)).get();
    }

    /** The calling thread's thread-local heap's magazine for buffers above the size classes, or null. */
    private static Object spanMagazine(AdaptiveByteBufAllocator allocator) throws Exception {
        Object heap = threadLocalHeap(allocator);
        Field magField = heap.getClass().getDeclaredField("spanMagazine");
        magField.setAccessible(true);
        return magField.get(heap);
    }

    /** The current chunk of the calling thread's size-class magazine for {@code size}, or null. */
    private static Object currentChunk(AdaptiveByteBufAllocator allocator, int size) throws Exception {
        Object heap = threadLocalHeap(allocator);
        Field magsField = heap.getClass().getDeclaredField("magazines");
        magsField.setAccessible(true);
        Object mag = ((Object[]) magsField.get(heap))[AdaptivePoolingAllocator.sizeClassIndexOf(size)];
        Field currentField = mag.getClass().getDeclaredField("current");
        currentField.setAccessible(true);
        return currentField.get(mag);
    }

    /**
     * A size class that made no allocation through a whole decay interval gives up its chunks, the ones it keeps as
     * its floor included: their slices go back to the page store; a class still allocating keeps its own.
     */
    @Test
    void sizeClassIdleForAWholeIntervalGivesUpItsChunks() throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        onThreadLocalHeap(() -> {
            final int idleSize = 64 * 1024;
            final int busySize = 256;
            allocator.heapBuffer(idleSize, idleSize).release();
            long idleChunk = claimedHeapBytes(allocator);
            allocator.heapBuffer(busySize).release();
            IdleDecay idleDecay = threadLocalIdleDecay(allocator);
            long used = claimedHeapBytes(allocator);
            assertNotNull(currentChunk(allocator, idleSize));

            // The first decay only records where each class stands.
            idleDecay.decay(System.nanoTime());
            assertNotNull(currentChunk(allocator, idleSize), "allocated since the heap was created: not idle");
            allocator.heapBuffer(busySize).release();

            // Idle through a whole interval: its chunk's slices go back.
            idleDecay.decay(System.nanoTime());
            assertNull(currentChunk(allocator, idleSize), "the idle class gave its chunk up");
            assertNotNull(currentChunk(allocator, busySize), "the class in use keeps its chunk");
            assertEquals(used - idleChunk, claimedHeapBytes(allocator));
        });
    }

    /** A size class that allocated exactly one purge tick's worth since the previous decay is not idle. */
    @Test
    void sizeClassWithOneTickBetweenDecaysIsNotIdle() throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        onThreadLocalHeap(() -> {
            final int size = 64 * 1024;
            allocator.heapBuffer(size, size).release();
            IdleDecay idleDecay = threadLocalIdleDecay(allocator);
            idleDecay.decay(System.nanoTime());
            int threshold = (int) AdaptivePoolingAllocator.CHUNK_PURGE_INTERVAL
                    * (heapChunkSizeOf(size) / size);
            for (int i = 0; i < threshold; i++) {
                allocator.heapBuffer(size, size).release();
            }
            Object current = currentChunk(allocator, size);
            assertNotNull(current);
            idleDecay.decay(System.nanoTime());
            assertSame(current, currentChunk(allocator, size), "its allocation count came back to where it was");
        });
    }

    /** On a stripe too, a size class idle through a whole interval gives up its chunk while another one allocates. */
    @Test
    void sizeClassIdleOnAStripeGivesUpItsChunks() throws Throwable {
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        onHeapThread(false, () -> {
            final int idleSize = 16 * 1024;
            allocator.heapBuffer(idleSize, idleSize).release();
            long idleChunk = claimedHeapBytes(allocator);
            allocator.heapBuffer(256).release();
            long used = claimedHeapBytes(allocator);
            Object stripe = stripeHeapWithMagazines(allocator);
            Field decayField = stripe.getClass().getDeclaredField("idleDecay");
            decayField.setAccessible(true);
            IdleDecay idleDecay = (IdleDecay) decayField.get(stripe);
            Field lockField = stripe.getClass().getDeclaredField("lock");
            lockField.setAccessible(true);
            StampedLock lock = (StampedLock) lockField.get(stripe);
            for (int round = 0; round < 2; round++) {
                long stamp = lock.writeLock();
                try {
                    idleDecay.decay(System.nanoTime());
                } finally {
                    lock.unlockWrite(stamp);
                }
                allocator.heapBuffer(256).release();
            }
            assertNull(magazineCurrent(stripe, idleSize), "the idle class gave its chunk up");
            assertNotNull(magazineCurrent(stripe, 256), "the class in use keeps its chunk");
            assertEquals(used - idleChunk, claimedHeapBytes(allocator));
        });
    }

    /** A current chunk emptied by another thread's release, still only noted, is given up like any other. */
    @Test
    void idleSizeClassGivesUpAChunkAnotherThreadEmptied() throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        onThreadLocalHeap(() -> {
            final int idleSize = 64 * 1024;
            ByteBuf buf = allocator.heapBuffer(idleSize, idleSize);
            long idleChunk = claimedHeapBytes(allocator);
            SizeClassedChunk chunk = chunkOf(buf);
            release(buf, true);
            assertEquals(1, chunk.owningCache.pendingCount(), "the other thread's release left a note");
            allocator.heapBuffer(256).release();
            long used = claimedHeapBytes(allocator);
            IdleDecay idleDecay = threadLocalIdleDecay(allocator);
            idleDecay.decay(System.nanoTime());
            allocator.heapBuffer(256).release();
            idleDecay.decay(System.nanoTime());
            assertNull(currentChunk(allocator, idleSize));
            assertEquals(used - idleChunk, claimedHeapBytes(allocator));
            assertEquals(0, chunk.owningCache.pendingCount());
        });
    }

    /** The one stripe that created size-class magazines. */
    private static Object stripeHeapWithMagazines(AdaptiveByteBufAllocator allocator) throws Exception {
        Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        heapField.setAccessible(true);
        Object pooling = heapField.get(allocator);
        Field stripesField = pooling.getClass().getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        Object found = null;
        for (Object stripe : (Object[]) stripesField.get(pooling)) {
            Field magsField = stripe.getClass().getDeclaredField("magazines");
            magsField.setAccessible(true);
            if (magsField.get(stripe) != null) {
                assertNull(found, "one stripe only");
                found = stripe;
            }
        }
        assertNotNull(found);
        return found;
    }

    /** The current chunk of {@code heap}'s size-class magazine for {@code size}. */
    private static Object magazineCurrent(Object heap, int size) throws Exception {
        Field magsField = heap.getClass().getDeclaredField("magazines");
        magsField.setAccessible(true);
        Object mag = ((Object[]) magsField.get(heap))[AdaptivePoolingAllocator.sizeClassIndexOf(size)];
        Field currentField = mag.getClass().getDeclaredField("current");
        currentField.setAccessible(true);
        return currentField.get(mag);
    }

    /** A chunk with a buffer out is never given up, however long its class stays idle. */
    @Test
    void idleSizeClassKeepsAChunkWithABufferOut() throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        onThreadLocalHeap(() -> {
            final int size = 64 * 1024;
            ByteBuf out = allocator.heapBuffer(size, size);
            try {
                IdleDecay idleDecay = threadLocalIdleDecay(allocator);
                long used = allocator.usedHeapMemory();
                Object chunk = currentChunk(allocator, size);
                for (int i = 0; i < 4; i++) {
                    idleDecay.decay(System.nanoTime());
                }
                assertSame(chunk, currentChunk(allocator, size));
                assertEquals(used, allocator.usedHeapMemory());
            } finally {
                out.release();
            }
        });
    }

    /**
     * A thread with its own heap takes buffers above the size classes from that heap too: no stripe, no lock.
     */
    @Test
    void threadLocalHeapServesBuffersAboveTheSizeClasses() throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode does not pool 512 KiB buffers");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        onThreadLocalHeap(() -> {
            ByteBuf buf = allocator.heapBuffer(BUDDY_NOTE_SIZE, BUDDY_NOTE_SIZE);
            try {
                assertNotNull(spanMagazine(allocator), "the heap's own magazine");
                assertEquals(0, stripesWithASpanMagazine(allocator), "no stripe involved");
            } finally {
                buf.release();
            }
        });
    }

    /**
     * A buffer that grows from a size class into the sizes above them on a thread-local heap moves to the heap's own
     * magazine, keeping its content, without a stripe.
     */
    @Test
    void reallocationIntoTheLargeSizesStaysOnTheThreadLocalHeap() throws Throwable {
        assumeFalse(isLowMemory(), "low-memory mode does not pool 512 KiB buffers");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        onThreadLocalHeap(() -> {
            ByteBuf buf = allocator.heapBuffer(64 * 1024);
            try {
                for (int i = 0; i < 64 * 1024; i++) {
                    buf.writeByte(i);
                }
                buf.capacity(BUDDY_NOTE_SIZE);
                assertEquals(BUDDY_NOTE_SIZE, buf.capacity());
                for (int i = 0; i < 64 * 1024; i++) {
                    assertEquals((byte) i, buf.getByte(i));
                }
                assertNotNull(spanMagazine(allocator), "the heap's own magazine");
                assertEquals(0, stripesWithASpanMagazine(allocator), "no stripe involved");
            } finally {
                buf.release();
            }
        });
    }

    private static boolean isLowMemory() throws Exception {
        Field f = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        f.setAccessible(true);
        return f.getBoolean(null);
    }

    /** Runs a cache operation the way the allocator does: under the stripe lock when the cache is a stripe's. */
    private static void underStripeLocks(AdaptiveByteBufAllocator allocator, boolean sharedStripe, Runnable action)
            throws Exception {
        List<StampedLock> locks = sharedStripe ? stripeLocks(allocator) : Collections.<StampedLock>emptyList();
        List<Long> stamps = new ArrayList<Long>();
        for (StampedLock l : locks) {
            stamps.add(l.writeLock());
        }
        try {
            action.run();
        } finally {
            for (int i = 0; i < locks.size(); i++) {
                locks.get(i).unlockWrite(stamps.get(i));
            }
        }
    }

    /** The bytes of the heap allocator's slices that chunks and spans hold. */
    private static long claimedHeapBytes(AdaptiveByteBufAllocator allocator) {
        PageStore store = heap(allocator).pageStore;
        return (long) store.sliceCounts()[0] * store.config.sliceSize;
    }

    /** How many buffers {@code chunk} hands out. */
    private static int segmentsOf(SizeClassedChunk chunk) throws Exception {
        Field segments = SizeClassedChunk.class.getDeclaredField("segments");
        segments.setAccessible(true);
        return segments.getInt(chunk);
    }

    private static SizeClassedChunk chunkOf(ByteBuf buf) {
        // Unwrap the leak-aware wrapper, if any.
        while (!(buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf)) {
            buf = buf.unwrap();
        }
        return (SizeClassedChunk) ((AdaptivePoolingAllocator.AdaptiveByteBuf) buf).chunk;
    }

    private static void release(ByteBuf buf, boolean foreignThread) throws InterruptedException {
        if (!foreignThread) {
            buf.release();
            return;
        }
        Thread t = new Thread(buf::release);
        t.start();
        t.join();
    }

    // Regression: on the shared (striped) path a segment returned after the allocator was
    // freed was absorbed into the chunk's local free list by the lock-holding release path,
    // which skipped the deallocation accounting entirely -- so the chunk never deallocated.
    // The chunk is abandoned to the page store, whose purger takes the dead stripe's lock to look.
    @Test
    void segmentReturnedAfterFreeMustStillDeallocateChunk() throws Exception {
        // useCacheForNonEventLoopThreads=false -> a plain thread takes the shared path
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        ByteBuf buf = allocator.heapBuffer(256);
        SizeClassedChunk chunk = chunkOf(buf);
        assertFalse(spanFree(chunk));

        freeHeap(allocator);
        PageStore store = heap(allocator).pageStore;
        assertEquals(1, store.abandonedCount());

        // last outstanding segment comes back from another thread
        Thread t = new Thread(buf::release);
        t.start();
        t.join();

        runPurgePass(store);
        assertEquals(0, store.abandonedCount());
        assertTrue(spanFree(chunk), "chunk must deallocate once its last segment is returned");
    }

    // The thread-local counterpart: the owner thread exits (its FastThreadLocal heap is removed and freed) while
    // buffers of its magazine's active chunk are still live, and they come back from another thread.
    @Test
    void segmentReturnedAfterThreadLocalHeapFreeMustStillDeallocateChunk() throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        final List<ByteBuf> live = new ArrayList<ByteBuf>();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread owner = new Thread(() -> FastThreadLocalThread.runWithFastThreadLocal(() -> {
            try {
                for (int i = 0; i < 4; i++) {
                    live.add(allocator.heapBuffer(256));
                }
                // Some segments come back on the owner thread, some stay live past the heap's removal.
                live.remove(0).release();
                live.remove(0).release();
                assertSame(chunkOf(live.get(0)), chunkOf(live.get(1)), "both live buffers share the active chunk");
            } catch (Throwable t) {
                failure.set(t);
            }
        }));
        owner.start();
        owner.join();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        SizeClassedChunk chunk = chunkOf(live.get(0));
        PageStore store = heap(allocator).pageStore;
        assertFalse(spanFree(chunk), "the live buffers still hold their chunk");
        assertEquals(1, store.abandonedCount());

        // The test thread is not the owner, so these take the cross-thread release path.
        live.remove(0).release();
        runPurgePass(store);
        assertFalse(spanFree(chunk), "one segment is still outstanding");
        live.remove(0).release();
        runPurgePass(store);
        assertTrue(spanFree(chunk), "chunk must deallocate once its last segment is returned");
    }

    /**
     * A thread-local heap dies with buffers out: their chunk is abandoned to the page store, and every later release
     * takes the CAS path, the dying thread's own included (as from a later {@code FastThreadLocal}'s
     * {@code onRemoval}). The span goes back at the first purge pass after the last release, not before.
     */
    @Test
    void chunkOfADeadHeapGoesBackAtThePurgePassAfterItsLastRelease() throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        final List<ByteBuf> handedOver = new ArrayList<ByteBuf>();
        final AtomicReference<SizeClassedChunk> chunkRef = new AtomicReference<SizeClassedChunk>();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final int[] externalAfterOwnRelease = {-1};
        Thread owner = new FastThreadLocalThread(() -> {
            try {
                for (int i = 0; i < 3; i++) {
                    handedOver.add(allocator.heapBuffer(256));
                }
                ByteBuf own = allocator.heapBuffer(256);
                SizeClassedChunk chunk = chunkOf(own);
                chunkRef.set(chunk);
                // The heap dies first, as when its FastThreadLocal is removed before another one whose onRemoval
                // releases a buffer on this same thread.
                threadLocalHeapVariable(allocator).remove();
                assertNull(chunk.ownerThread(), "an abandoned chunk has no owner thread");
                own.release();
                externalAfterOwnRelease[0] = chunk.externalFreeCount();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        owner.start();
        owner.join();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        SizeClassedChunk chunk = chunkRef.get();
        PageStore store = heap(allocator).pageStore;
        assertEquals(1, externalAfterOwnRelease[0], "the dead heap's thread must push on the external list");
        assertEquals(1, store.abandonedCount(), "the chunk had buffers out: abandoned");
        assertFalse(spanFree(chunk));

        // U, this thread, releases what it was handed, one buffer short of all.
        handedOver.remove(0).release();
        handedOver.remove(0).release();
        runPurgePass(store);
        assertEquals(1, store.abandonedCount(), "a buffer is still out: the chunk waits");
        assertFalse(spanFree(chunk));

        handedOver.remove(0).release();
        assertFalse(spanFree(chunk), "the span goes back at a purge pass, not at the release");
        runPurgePass(store);
        assertEquals(0, store.abandonedCount());
        assertTrue(spanFree(chunk), "the span must go back once its last buffer is back");
    }

    /** Runs a pass of {@code store}'s purge: one due at a time a purge delay and a check interval from its last. */
    private static void runPurgePass(PageStore store) {
        long passes = store.purges;
        long now = Math.max(System.nanoTime(), store.lastPurgeNanos)
                + store.config.purgeDelayNanos + store.config.purgeCheckNanos;
        store.purgeIfDue(now);
        assertEquals(passes + 1, store.purges, "the pass did not run");
    }

    /** Whether {@code chunk}'s span is back in the store: free in its block, or its region given back whole. */
    private static boolean spanFree(SizeClassedChunk chunk) {
        Segment block = chunk.segment;
        long bits = block.bits(chunk.spanStart, chunk.spanSlices());
        return block.region.released || (block.free & bits) == bits;
    }

    /** The allocator's {@code FastThreadLocal} of thread-local heaps. */
    private static FastThreadLocal<?> threadLocalHeapVariable(AdaptiveByteBufAllocator allocator) throws Exception {
        Field tlField = AdaptivePoolingAllocator.class.getDeclaredField("threadLocalSizeClassHeap");
        tlField.setAccessible(true);
        return (FastThreadLocal<?>) tlField.get(heap(allocator));
    }

    /**
     * The fallback in the allocation slow path: a polled chunk without a free segment is given up and a fresh
     * chunk serves the allocation. The cache never hands out such a chunk, so the test makes one by taking every
     * free segment out of a cached chunk behind the cache's back. With assertions enabled the allocation fails
     * the assertion, but only after the fresh chunk is in place, so the allocator keeps working.
     */
    @Test
    void polledChunkWithoutAFreeSegmentFallsBackToAFreshChunk() throws Exception {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        try {
            // Chunk A: every segment handed out. Chunk B: the active chunk, one segment handed out.
            for (int i = 0; i <= BURST_SEGMENTS_PER_CHUNK; i++) {
                held.add(allocator.heapBuffer(BURST_BUF_SIZE));
            }
            SizeClassedChunk chunkA = chunkOf(held.get(0));
            SizeClassedChunk chunkB = chunkOf(held.get(BURST_SEGMENTS_PER_CHUNK));
            SizeClassedChunkCache cache = chunkA.owningCache;
            // Chunk A is the only queued chunk of its class, so it is retained once fully free.

            // Return A's segments from another thread while the stripe lock is held, so they land in A's MPSC
            // free list and leave a note; the drain then files A as reusable.
            List<StampedLock> locks = stripeLocks(allocator);
            List<Long> stamps = new ArrayList<Long>();
            for (StampedLock l : locks) {
                stamps.add(l.writeLock());
            }
            try {
                for (int i = 0; i < BURST_SEGMENTS_PER_CHUNK; i++) {
                    release(held.get(i), true);
                }
                cache.drainPending();
                // Behind the cache's back: take every free segment out of A.
                assertEquals(BURST_SEGMENTS_PER_CHUNK, chunkA.takeAllFreeSegments());
            } finally {
                for (int i = 0; i < locks.size(); i++) {
                    locks.get(i).unlockWrite(stamps.get(i));
                }
            }
            held.subList(0, BURST_SEGMENTS_PER_CHUNK).clear();
            assertEquals(2L * BURST_CHUNK_SIZE, claimedHeapBytes(allocator));

            // Run B out of segments; the next allocation polls A.
            for (int i = 1; i < BURST_SEGMENTS_PER_CHUNK; i++) {
                held.add(allocator.heapBuffer(BURST_BUF_SIZE));
            }
            assertEquals(2L * BURST_CHUNK_SIZE, claimedHeapBytes(allocator));
            if (AdaptivePoolingAllocator.class.desiredAssertionStatus()) {
                AssertionError failed = null;
                try {
                    allocator.heapBuffer(BURST_BUF_SIZE).release();
                } catch (AssertionError e) {
                    failed = e;
                }
                assertNotNull(failed, "the fallback must fail its assertion when assertions are enabled");
                assertEquals("the cache handed out a chunk without a free segment", failed.getMessage());
            } else {
                held.add(allocator.heapBuffer(BURST_BUF_SIZE));
            }
            // A fresh chunk C serves the allocations, and the allocator keeps working.
            assertEquals(3L * BURST_CHUNK_SIZE, claimedHeapBytes(allocator));
            ByteBuf next = allocator.heapBuffer(BURST_BUF_SIZE);
            held.add(next);
            assertFalse(chunkOf(next) == chunkA || chunkOf(next) == chunkB, "the allocation must land in chunk C");
            assertEquals(3L * BURST_CHUNK_SIZE, claimedHeapBytes(allocator));
        } finally {
            for (ByteBuf buf : held) {
                buf.release();
            }
        }
    }

    // --- Cross-thread returns that miss the stripe lock ---
    //
    // A releaser that cannot take the stripe lock puts its segment in the chunk's MPSC free list and
    // leaves a note on the owning cache. Nothing scans for such chunks any more, so if a note is lost
    // the chunk stays on the exhausted list forever: it has capacity nobody can find, and it is never
    // fully free either, so the purge sweep will not evict it. Both tests below are about that.

    /** Buffer size whose size class has a 64 KiB chunk of 16 segments. */
    private static final int BURST_BUF_SIZE = 4096;
    /** A 64 KiB chunk fits 16, less the one an exact fit gives up for its colours. */
    private static final int BURST_SEGMENTS_PER_CHUNK = 15;
    private static final int BURST_CHUNK_SIZE = 64 * 1024;
    private static final int BURST_CHUNKS = 400;

    /**
     * Runs the burst with every stripe write lock held, so no releaser can apply the exhausted -&gt;
     * reusable transition inline and the notification is the only thing that can move a chunk. Without
     * that this assertion is at the mercy of the scheduler: with the locks free, most chunks are moved
     * by the lock-winning path and deleting the drain's {@code moveToReusable} still leaves only a
     * handful stranded.
     */
    @Test
    void noChunkIsStrandedAfterABurstWithCrossThreadReleases() throws Exception {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        runBurstWithCrossThreadReleases(allocator, true);

        // Every worker has been joined, so the lists are quiescent and safe to walk from here.
        for (SizeClassedChunkCache cache : sizeClassChunkCaches(allocator)) {
            int stranded = 0;
            for (AdaptivePoolingAllocator.Chunk c = cache.exhausted.head; c != null; c = c.nextInQueue) {
                if (((SizeClassedChunk) c).hasRemainingCapacity()) {
                    stranded++;
                }
            }
            assertEquals(0, stranded,
                    "chunks left on the exhausted list with capacity: neither reusable nor evictable");
        }
    }

    @Test
    void memoryFallsBackToTheKeptChunksAfterAnIdleBurst() throws Exception {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        long peak = runBurstWithCrossThreadReleases(allocator, false);

        int caches = sizeClassChunkCaches(allocator).size();
        // Per cache: the four empty chunks it keeps, the magazine's active chunk, and slack. The burst ran on one
        // stripe (see runBurstWithCrossThreadReleases).
        long bound = (long) caches * 6 * BURST_CHUNK_SIZE;
        long settled = claimedHeapBytes(allocator);

        assertTrue(peak > bound, "the burst must go beyond what may be retained, or this tests nothing: peak "
                + peak + ", bound " + bound);
        assertTrue(settled <= bound,
                "after the burst went idle the caches must fall back to the chunks they keep: settled " + settled
                        + " > " + bound + " (" + caches + " caches, chunks of " + BURST_CHUNK_SIZE + "), peak was "
                        + peak);
    }

    /**
     * One heap's owner allocates across many size classes, switching chunks all the time, while other threads
     * release what it allocates: their notes land on the heap's notes while its slow paths drain them. Once every
     * buffer is back and the owner drained once more, no note is left and no chunk sits on an exhausted list: every
     * chunk was refiled. The purge then takes every cache down to the chunks it keeps.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noNoteIsLostWhileRemoteReleasesRaceChunkSwitches(final boolean threadLocal) throws Exception {
        assumeFalse(isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, threadLocal));
        final int[] sizes = smallSizeClasses();
        final BlockingQueue<ByteBuf> toRelease = new ArrayBlockingQueue<ByteBuf>(4096);
        final AtomicBoolean done = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread[] releasers = new Thread[4];
        for (int i = 0; i < releasers.length; i++) {
            releasers[i] = new Thread(() -> {
                try {
                    for (;;) {
                        ByteBuf buf = toRelease.poll(1, TimeUnit.MILLISECONDS);
                        if (buf != null) {
                            buf.release();
                        } else if (done.get() && toRelease.isEmpty()) {
                            return;
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "releaser-" + i);
            releasers[i].start();
        }
        // On stripes, a release that wins the stripe lock applies itself and leaves no note: hold every stripe lock
        // on and off, so that many releases cannot, while the allocations go on between the holds.
        final List<StampedLock> locks = threadLocal ? Collections.<StampedLock>emptyList() : stripeLocks(allocator);
        Thread blocker = new Thread(() -> {
            long[] stamps = new long[locks.size()];
            while (!done.get()) {
                for (int i = 0; i < stamps.length; i++) {
                    stamps[i] = locks.get(i).writeLock();
                }
                LockSupport.parkNanos(50000);
                for (int i = 0; i < stamps.length; i++) {
                    locks.get(i).unlockWrite(stamps[i]);
                }
                LockSupport.parkNanos(50000);
            }
        }, "stripe-blocker");
        blocker.start();
        onHeapThread(threadLocal, () -> {
            try {
                SplittableRandom rng = new SplittableRandom(42);
                for (int i = 0; i < 200000; i++) {
                    toRelease.put(allocator.heapBuffer(sizes[rng.nextInt(sizes.length)]));
                }
            } finally {
                done.set(true);
            }
            for (Thread t : releasers) {
                t.join();
            }
            blocker.join();
            List<SizeClassedChunkCache> caches = threadLocal ? threadLocalChunkCaches(allocator)
                    : sizeClassChunkCaches(allocator);
            assertTrue(caches.size() > 16, "many size classes must take part: " + caches.size());
            underStripeLocks(allocator, !threadLocal, () -> {
                for (SizeClassedChunkCache cache : caches) {
                    cache.drainPending();
                }
                for (SizeClassedChunkCache cache : caches) {
                    assertEquals(0, cache.pendingCount(), "a note was left after the last drain");
                    assertEquals(0, cache.exhausted.size, "every buffer is back: no chunk may stay exhausted");
                    for (AdaptivePoolingAllocator.Chunk c = cache.reusable.head; c != null; c = c.nextInQueue) {
                        assertTrue(((SizeClassedChunk) c).hasFullCapacity(), "every buffer is back");
                    }
                    cache.tickPurge();
                    assertTrue(cache.reusable.size <= SizeClassedChunkCache.FLOOR,
                            "the purge must take the cache down to the chunks it keeps: " + cache.reusable.size);
                }
            });
        });
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }

    /** The size classes up to 16 KiB, whose chunks are a few slices at most: many chunk switches. */
    private static int[] smallSizeClasses() {
        int[] all = AdaptivePoolingAllocator.getSizeClasses();
        int n = 0;
        while (n < all.length && all[n] <= 16384) {
            n++;
        }
        return Arrays.copyOf(all, n);
    }

    /** The size-class caches of the calling thread's thread-local heap. */
    private static List<SizeClassedChunkCache> threadLocalChunkCaches(AdaptiveByteBufAllocator allocator)
            throws Exception {
        Object heap = threadLocalHeapVariable(allocator).get();
        Field magsField = heap.getClass().getDeclaredField("magazines");
        magsField.setAccessible(true);
        List<SizeClassedChunkCache> caches = new ArrayList<SizeClassedChunkCache>();
        for (Object magazine : (Object[]) magsField.get(heap)) {
            if (magazine != null) {
                Field cacheField = magazine.getClass().getDeclaredField("chunkCache");
                cacheField.setAccessible(true);
                caches.add((SizeClassedChunkCache) cacheField.get(magazine));
            }
        }
        return caches;
    }

    /**
     * Allocate a large live set on one thread, then hand every buffer to a pool of releaser threads
     * that contend with each other for the same stripe lock, so most returns take the lock-free MPSC
     * path and have to leave a note behind. Returns the peak used memory, and leaves the allocator
     * settled on a small working set.
     *
     * <p>All allocation happens on one thread, and never while the releasers are running: a stripe
     * whose lock is contended makes the allocation path fall through to another stripe, and a stripe
     * that is never allocated on again is also never purged (that is true of this allocator with or
     * without the notification queue). Keeping to a single stripe is what makes the assertions here
     * about the mechanism rather than about stripe scheduling.
     */
    private static long runBurstWithCrossThreadReleases(final AdaptiveByteBufAllocator allocator,
            final boolean forceNotifyPath) throws Exception {
        final BlockingQueue<ByteBuf> toRelease = new ArrayBlockingQueue<ByteBuf>(1024);
        final AtomicBoolean handedOver = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final AtomicReference<Long> peak = new AtomicReference<Long>(0L);
        final CountDownLatch releasersDone = new CountDownLatch(8);

        Thread[] releasers = new Thread[8];
        for (int i = 0; i < releasers.length; i++) {
            releasers[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (;;) {
                            ByteBuf buf = toRelease.poll(1, TimeUnit.MILLISECONDS);
                            if (buf != null) {
                                buf.release();
                            } else if (handedOver.get() && toRelease.isEmpty()) {
                                return;
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        releasersDone.countDown();
                    }
                }
            }, "releaser-" + i);
            releasers[i].start();
        }

        Thread allocatorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    int burstBuffers = BURST_CHUNKS * BURST_SEGMENTS_PER_CHUNK;
                    ByteBuf[] live = new ByteBuf[burstBuffers];
                    for (int i = 0; i < burstBuffers; i++) {
                        live[i] = allocator.heapBuffer(BURST_BUF_SIZE);
                    }
                    peak.set(claimedHeapBytes(allocator));

                    // Optionally hold every stripe write lock across the release phase. Contention
                    // alone only makes *most* returns take the notify path - how many is up to the
                    // scheduler, and if every releaser happens to win the lock the assertions below
                    // test nothing. With the locks held no releaser can win, so the notification is
                    // the only thing that can move a chunk, deterministically.
                    List<Long> stamps = new ArrayList<Long>();
                    List<StampedLock> locks = forceNotifyPath ?
                            stripeLocks(allocator) : Collections.<StampedLock>emptyList();
                    for (StampedLock l : locks) {
                        stamps.add(l.writeLock());
                    }

                    // Hand the live set to the releasers, which now contend with each other.
                    for (int i = 0; i < burstBuffers; i++) {
                        toRelease.put(live[i]);
                        live[i] = null;
                    }
                    handedOver.set(true);
                    releasersDone.await();
                    for (int i = 0; i < locks.size(); i++) {
                        locks.get(i).unlockWrite(stamps.get(i));
                    }

                    // Settle on a tiny working set, on the same thread and so the same stripe. These
                    // allocations are what drives the heap-wide drain and the purge tick; the releases
                    // are uncontended now, so they take the inline path and leave no new notes.
                    int allocations = 8 * BURST_SEGMENTS_PER_CHUNK
                            * (int) AdaptivePoolingAllocator.CHUNK_PURGE_INTERVAL * 4;
                    for (int i = 0; i < allocations; i++) {
                        allocator.heapBuffer(BURST_BUF_SIZE).release();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                    handedOver.set(true);
                }
            }
        }, "burst-allocator");
        allocatorThread.start();
        allocatorThread.join();
        for (Thread t : releasers) {
            t.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        return peak.get();
    }

    private static List<StampedLock> stripeLocks(AdaptiveByteBufAllocator allocator) throws Exception {
        Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        heapField.setAccessible(true);
        Object pooling = heapField.get(allocator);
        Field stripesField = pooling.getClass().getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        Object[] stripes = (Object[]) stripesField.get(pooling);
        List<StampedLock> out = new ArrayList<StampedLock>();
        for (Object stripe : stripes) {
            if (stripe == null) {
                continue;
            }
            Field lockField = stripe.getClass().getDeclaredField("lock");
            lockField.setAccessible(true);
            out.add((StampedLock) lockField.get(stripe));
        }
        return out;
    }

    private static List<SizeClassedChunkCache> sizeClassChunkCaches(
            AdaptiveByteBufAllocator allocator) throws Exception {
        Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        heapField.setAccessible(true);
        Object pooling = heapField.get(allocator);
        Field stripesField = pooling.getClass().getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        Object[] stripes = (Object[]) stripesField.get(pooling);
        List<SizeClassedChunkCache> caches = new ArrayList<SizeClassedChunkCache>();
        for (Object stripe : stripes) {
            if (stripe == null) {
                continue;
            }
            Field magsField = stripe.getClass().getDeclaredField("magazines");
            magsField.setAccessible(true);
            Object[] magazines = (Object[]) magsField.get(stripe);
            if (magazines == null) {
                continue;
            }
            for (Object magazine : magazines) {
                if (magazine == null) {
                    continue;
                }
                Field cacheField = magazine.getClass().getDeclaredField("chunkCache");
                cacheField.setAccessible(true);
                Object cache = cacheField.get(magazine);
                if (cache instanceof SizeClassedChunkCache) {
                    caches.add((SizeClassedChunkCache) cache);
                }
            }
        }
        return caches;
    }

    private static void shuffle(SplittableRandom rng, Object array) {
        int len = Array.getLength(array);
        for (int i = 0; i < len; i++) {
            int n = rng.nextInt(i, len);
            Object value = Array.get(array, i);
            Array.set(array, i, Array.get(array, n));
            Array.set(array, n, value);
        }
    }

    /**
     * What a chunk says about its free segments - whether it has one, whether it has all of them, and the bytes it
     * last counted - through every way a segment comes back: never handed out, released under the lock, released
     * by another thread that could not take the lock and not yet polled, and polled. Each segment that came back
     * is then handed out again, once.
     */
    @Test
    void capacityQueriesFollowEveryWayASegmentComesBack() throws Exception {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        // One buffer: chunk A is active and its other segments were never handed out.
        held.add(allocator.heapBuffer(BURST_BUF_SIZE));
        SizeClassedChunk chunkA = chunkOf(held.get(0));
        assertTrue(chunkA.hasRemainingCapacity());
        assertFalse(chunkA.hasFullCapacity());

        // Chunk A: every segment handed out. Chunk B: the active chunk, one segment handed out.
        for (int i = 1; i <= BURST_SEGMENTS_PER_CHUNK; i++) {
            held.add(allocator.heapBuffer(BURST_BUF_SIZE));
        }
        assertSame(chunkA, chunkOf(held.get(BURST_SEGMENTS_PER_CHUNK - 1)));
        assertNotSame(chunkA, chunkOf(held.get(BURST_SEGMENTS_PER_CHUNK)));
        assertFalse(chunkA.hasRemainingCapacity());
        assertEquals(0, chunkA.remainingCapacity());

        // Released by a thread that could take the stripe lock: straight into the chunk's local free list.
        release(held.get(0), false);
        assertTrue(chunkA.hasRemainingCapacity());
        assertEquals(BURST_BUF_SIZE, chunkA.remainingCapacity());

        // Released by a thread that cannot take the lock: counted as free before anyone takes them over. The last
        // buffer of A stays in use, so that A is never given up and the same chunk serves what follows.
        ByteBuf lastOfA = held.get(BURST_SEGMENTS_PER_CHUNK - 1);
        List<StampedLock> locks = stripeLocks(allocator);
        List<Long> stamps = new ArrayList<Long>();
        for (StampedLock l : locks) {
            stamps.add(l.writeLock());
        }
        try {
            for (int i = 1; i < BURST_SEGMENTS_PER_CHUNK - 1; i++) {
                release(held.get(i), true);
            }
            assertTrue(chunkA.hasRemainingCapacity());
            assertFalse(chunkA.hasFullCapacity());
            assertEquals((BURST_SEGMENTS_PER_CHUNK - 1) * BURST_BUF_SIZE, chunkA.remainingCapacity());
        } finally {
            for (int i = 0; i < locks.size(); i++) {
                locks.get(i).unlockWrite(stamps.get(i));
            }
        }
        ByteBuf firstOfB = held.get(BURST_SEGMENTS_PER_CHUNK);
        held.clear();

        // Run B out of segments; then A serves all but one of its segments: the one released under the lock, then
        // those taken over from the other thread, each once and never the segment still in use.
        List<ByteBuf> fromB = new ArrayList<ByteBuf>();
        fromB.add(firstOfB);
        for (int i = 1; i < BURST_SEGMENTS_PER_CHUNK; i++) {
            fromB.add(allocator.heapBuffer(BURST_BUF_SIZE));
            assertNotSame(chunkA, chunkOf(fromB.get(i)));
        }
        Set<Integer> offsets = new HashSet<Integer>();
        offsets.add(lastOfA.arrayOffset());
        for (int i = 1; i < BURST_SEGMENTS_PER_CHUNK; i++) {
            ByteBuf buf = allocator.heapBuffer(BURST_BUF_SIZE);
            held.add(buf);
            assertSame(chunkA, chunkOf(buf));
            assertTrue(offsets.add(buf.arrayOffset()), "segment handed out twice");
        }
        assertFalse(chunkA.hasRemainingCapacity());
        assertEquals(0, chunkA.remainingCapacity());

        // Every segment of A back, from another thread that cannot take the lock: all of them free, none polled yet.
        held.add(lastOfA);
        for (StampedLock l : locks) {
            stamps.set(locks.indexOf(l), l.writeLock());
        }
        try {
            for (ByteBuf buf : held) {
                release(buf, true);
            }
            assertTrue(chunkA.hasFullCapacity());
            assertTrue(chunkA.hasRemainingCapacity());
        } finally {
            for (int i = 0; i < locks.size(); i++) {
                locks.get(i).unlockWrite(stamps.get(i));
            }
        }
        for (ByteBuf buf : fromB) {
            buf.release();
        }
    }

    /**
     * The free lists live outside the memory they list: writing to a buffer after releasing it changes nothing for
     * the allocator, which hands every segment out once.
     */
    @Test
    void writeAfterReleaseDoesNotReachTheFreeLists() {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        ByteBuf first = allocator.heapBuffer(256, 256);
        ByteBuf second = allocator.heapBuffer(256, 256);
        byte[] chunk = second.array();
        int firstOffset = first.arrayOffset();
        int secondOffset = second.arrayOffset();
        first.release();
        second.release();
        Arrays.fill(chunk, firstOffset, firstOffset + 256, (byte) 0x7f);
        Arrays.fill(chunk, secondOffset, secondOffset + 256, (byte) 0x7f);
        ByteBuf again = allocator.heapBuffer(256, 256);
        ByteBuf andAgain = allocator.heapBuffer(256, 256);
        assertEquals(secondOffset, again.arrayOffset());
        assertEquals(firstOffset, andAgain.arrayOffset());
        ByteBuf fresh = allocator.heapBuffer(256, 256);
        assertNotEquals(firstOffset, fresh.arrayOffset());
        assertNotEquals(secondOffset, fresh.arrayOffset());
        again.release();
        andAgain.release();
        fresh.release();
    }

    /**
     * The last segment of a chunk comes back after the allocator was freed, from a thread that cannot take the
     * stripe lock: it goes on the external list, and that release must be the one that deallocates the chunk.
     */
    @Test
    void segmentReturnedExternallyAfterFreeMustStillDeallocateChunk() throws Exception {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        ByteBuf buf = allocator.heapBuffer(256);
        SizeClassedChunk chunk = chunkOf(buf);
        List<StampedLock> locks = stripeLocks(allocator);
        freeHeap(allocator);
        assertFalse(spanFree(chunk));
        List<Long> stamps = new ArrayList<Long>();
        for (StampedLock l : locks) {
            stamps.add(l.writeLock());
        }
        try {
            release(buf, true);
        } finally {
            for (int i = 0; i < locks.size(); i++) {
                locks.get(i).unlockWrite(stamps.get(i));
            }
        }
        runPurgePass(heap(allocator).pageStore);
        assertTrue(spanFree(chunk), "chunk must deallocate once its last segment is returned");
    }

    /**
     * A buffer that outgrows its segment moves to a larger one: its bytes move with it, and the segment it left is
     * free again - the next buffer of that size gets it.
     */
    @Test
    void aBufferThatOutgrowsItsSegmentGivesItBack() {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, false));
        ByteBuf buf = allocator.heapBuffer(256, 8192);
        byte[] firstArray = buf.array();
        int firstOffset = buf.arrayOffset();
        for (int i = 0; i < 256; i++) {
            buf.writeByte(i);
        }
        buf.capacity(1024);
        assertTrue(buf.array() != firstArray || buf.arrayOffset() != firstOffset, "the buffer did not move");
        for (int i = 0; i < 256; i++) {
            assertEquals((byte) i, buf.getByte(i));
        }
        ByteBuf next = allocator.heapBuffer(256, 256);
        assertSame(firstArray, next.array());
        assertEquals(firstOffset, next.arrayOffset());
        next.writeLong(42);
        for (int i = 0; i < 256; i++) {
            assertEquals((byte) i, buf.getByte(i));
        }
        next.release();
        buf.release();
    }

    /**
     * The owner thread keeps allocating while other threads release what it allocated: their segments reach it
     * through the chunk's external MPSC free list, which it polls one at a time while they offer. A segment must
     * never be handed out while the buffer that holds it is still in use, whatever the interleaving.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void segmentsReleasedByOtherThreadsAreNeverHandedOutTwice() throws Exception {
        final AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(false, true));
        final int releasers = 3;
        final int allocations = 300000;
        final BlockingQueue<ByteBuf> toRelease = new ArrayBlockingQueue<ByteBuf>(256);
        final java.util.concurrent.ConcurrentHashMap<Long, Boolean> inUse =
                new java.util.concurrent.ConcurrentHashMap<Long, Boolean>();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final ByteBuf poison = Unpooled.buffer(1);
        Thread[] threads = new Thread[releasers];
        for (int i = 0; i < releasers; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (;;) {
                            ByteBuf buf = toRelease.take();
                            if (buf == poison) {
                                return;
                            }
                            // Forget the segment before releasing it: it can only be handed out again afterwards.
                            inUse.remove(buf.memoryAddress());
                            buf.release();
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }
            });
            threads[i].start();
        }
        Thread owner = new FastThreadLocalThread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < allocations && failure.get() == null; i++) {
                        ByteBuf buf = allocator.directBuffer(64, 64);
                        if (inUse.putIfAbsent(buf.memoryAddress(), Boolean.TRUE) != null) {
                            throw new AssertionError("segment at " + buf.memoryAddress() + " handed out twice");
                        }
                        toRelease.put(buf);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }
        });
        owner.start();
        owner.join();
        for (int i = 0; i < releasers; i++) {
            toRelease.put(poison);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        poison.release();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertTrue(inUse.isEmpty());
    }

    /**
     * Frees the allocator's stripes, as its {@code free()} does, but keeps its page store open: a buffer still out
     * writes its free-list link into its block when it is released, so the block must stay.
     */
    private static void freeHeap(AdaptiveByteBufAllocator allocator) throws Exception {
        Field f = AdaptiveByteBufAllocator.class.getDeclaredField("heap");
        f.setAccessible(true);
        Object inner = f.get(allocator);
        Field stripesField = inner.getClass().getDeclaredField("stripedHeaps");
        stripesField.setAccessible(true);
        for (Object stripe : (Object[]) stripesField.get(inner)) {
            Method free = stripe.getClass().getDeclaredMethod("freeStripe");
            free.setAccessible(true);
            free.invoke(stripe);
        }
    }
}
