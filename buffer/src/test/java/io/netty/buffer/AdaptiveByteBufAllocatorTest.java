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

import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AdaptiveByteBufAllocatorTest extends AbstractAdaptiveByteBufAllocatorTest<AdaptiveByteBufAllocator> {
    @Override
    protected Class<? extends ByteBuf> bufferClass() {
        return AdaptivePoolingAllocator.AdaptiveByteBuf.class;
    }

    @Override
    protected AdaptiveByteBufAllocator newDefaultAllocator() {
        return new AdaptiveByteBufAllocator();
    }

    @Override
    protected AdaptiveByteBufAllocator newAllocator(boolean preferDirect) {
        return new AdaptiveByteBufAllocator(preferDirect);
    }

    @Override
    protected AdaptiveByteBufAllocator newUnpooledAllocator() {
        return newAllocator(false);
    }

    @Override
    protected long expectedUsedMemory(AdaptiveByteBufAllocator allocator, int capacity) {
        return 128 * 1024; // Min chunk size
    }

    @Override
    protected long expectedUsedMemoryAfterRelease(AdaptiveByteBufAllocator allocator, int capacity) {
        return 128 * 1024; // Min chunk size
    }

    @Test
    public void testDirectBufferMemoryAddressFollowsItsSegment() {
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        // A direct buffer's address is the chunk's address plus the buffer's offset in the chunk. The second buffer
        // is likely at an offset above zero.
        ByteBuf first = allocator.directBuffer(16, 1 << 20);
        ByteBuf second = allocator.directBuffer(16, 1 << 20);
        AbstractByteBuf unwrappedSecond = unwrapToAbstractByteBuf(second);
        try {
            assumeTrue(first.hasMemoryAddress());
            assumeTrue(PlatformDependent.hasDirectByteBufferAddress(first.nioBuffer(0, first.capacity())));
            assertMemoryAddressMatchesTheNioBuffer(first);
            assertMemoryAddressMatchesTheNioBuffer(second);
            // growing beyond its segment moves the buffer to another segment, with another address
            second.ensureWritable(128 * 1024);
            assertMemoryAddressMatchesTheNioBuffer(second);
        } finally {
            first.release();
            second.release();
        }
        // a released buffer has no address
        assertEquals(0L, unwrappedSecond._memoryAddress());
    }

    // The NIO buffer's address is computed from the chunk's buffer separately from memoryAddress(), and comparing the
    // addresses avoids reading memory through a wrong one.
    private static void assertMemoryAddressMatchesTheNioBuffer(ByteBuf buf) {
        assertEquals(PlatformDependent.directBufferAddress(buf.nioBuffer(0, buf.capacity())), buf.memoryAddress());
    }

    // with leak detection, the allocator wraps the AdaptiveByteBuf
    private static AbstractByteBuf unwrapToAbstractByteBuf(ByteBuf buf) {
        ByteBuf unwrapped = buf;
        while (!(unwrapped instanceof AbstractByteBuf)) {
            unwrapped = unwrapped.unwrap();
        }
        return (AbstractByteBuf) unwrapped;
    }

    @Override
    @Test
    public void testUsedDirectMemory() {
        AdaptiveByteBufAllocator allocator =  newAllocator(true);
        ByteBufAllocatorMetric metric = allocator.metric();
        assertEquals(0, metric.usedDirectMemory());
        ByteBuf buffer = allocator.directBuffer(1024, 4096);
        int capacity = buffer.capacity();
        assertEquals(expectedUsedMemory(allocator, capacity), metric.usedDirectMemory());

        // Double the size of the buffer
        buffer.capacity(capacity << 1);
        capacity = buffer.capacity();
        // This is a new size class, and a new magazine with a new chunk
        assertEquals(2 * expectedUsedMemory(allocator, capacity), metric.usedDirectMemory(), buffer.toString());

        buffer.release();
        // Memory is still held by the magazines
        assertEquals(2 * expectedUsedMemory(allocator, capacity), metric.usedDirectMemory());
    }

    @Override
    @Test
    public void testUsedHeapMemory() {
        AdaptiveByteBufAllocator allocator =  newAllocator(true);
        ByteBufAllocatorMetric metric = allocator.metric();
        assertEquals(0, metric.usedHeapMemory());
        ByteBuf buffer = allocator.heapBuffer(1024, 4096);
        int capacity = buffer.capacity();
        assertEquals(expectedUsedMemory(allocator, capacity), metric.usedHeapMemory());

        // Double the size of the buffer
        buffer.capacity(capacity << 1);
        capacity = buffer.capacity();
        // This is a new size class, and a new magazine with a new chunk
        assertEquals(2 * expectedUsedMemory(allocator, capacity), metric.usedHeapMemory(), buffer.toString());

        buffer.release();
        // Memory is still held by the magazines
        assertEquals(2 * expectedUsedMemory(allocator, capacity), metric.usedHeapMemory());
    }

    @Test
    void adaptiveChunkMustDeallocateOrReuseWthBufferRelease() throws Exception {
        AdaptiveByteBufAllocator allocator = newAllocator(false);
        Deque<ByteBuf> bufs = new ArrayDeque<>();
        assertEquals(0, allocator.usedHeapMemory());
        assertEquals(0, allocator.usedHeapMemory());
        bufs.add(allocator.heapBuffer(256));
        long usedHeapMemory = allocator.usedHeapMemory();
        int buffersPerChunk = Math.toIntExact(usedHeapMemory / 256);
        for (int i = 0; i < buffersPerChunk; i++) {
            bufs.add(allocator.heapBuffer(256));
        }
        assertEquals(2 * usedHeapMemory, allocator.usedHeapMemory());
        bufs.pop().release();
        assertEquals(2 * usedHeapMemory, allocator.usedHeapMemory());
        while (!bufs.isEmpty()) {
            bufs.pop().release();
        }
        assertEquals(2 * usedHeapMemory, allocator.usedHeapMemory());
        for (int i = 0; i < 2 * buffersPerChunk; i++) {
            bufs.add(allocator.heapBuffer(256));
        }
        assertEquals(2 * usedHeapMemory, allocator.usedHeapMemory());
        while (!bufs.isEmpty()) {
            bufs.pop().release();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void purgeScanShouldEvictIdleChunks(boolean threadLocal) throws Exception {
        // Thread-local magazines require FastThreadLocalThread
        // (allocate() checks currentThreadWillCleanupFastThreadLocals)
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, threadLocal);
        long purgePolls = threadLocal ?
                AdaptivePoolingAllocator.CHUNK_PURGE_POLLS_THREAD_LOCAL :
                AdaptivePoolingAllocator.CHUNK_PURGE_POLLS_SHARED;
        Runnable test = () -> assertPurgeScanEvictsIdleChunks(allocator, purgePolls);
        if (threadLocal) {
            FastThreadLocalThread.runWithFastThreadLocal(test);
        } else {
            test.run();
        }
    }

    private static void assertPurgeScanEvictsIdleChunks(AdaptiveByteBufAllocator allocator, long purgePolls) {
        ByteBuf probe = allocator.heapBuffer(256);
        long chunkSize = allocator.usedHeapMemory();
        int buffersPerChunk = (int) (chunkSize / 256);
        probe.release();

        int totalChunks = (int) Math.max(purgePolls, AdaptivePoolingAllocator.CHUNK_REUSE_QUEUE) * 4 + 10;
        int totalBuffers = totalChunks * buffersPerChunk;
        List<ByteBuf> bufs = new ArrayList<>(totalBuffers);
        for (int i = 0; i < totalBuffers; i++) {
            bufs.add(allocator.heapBuffer(256));
        }

        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        long memoryAfterRelease = allocator.usedHeapMemory();

        int threshold = AdaptivePoolingAllocator.CHUNK_PURGE_THRESHOLD;
        // Account for pollChunk calls burned during setup (one per chunk created)
        // and partition shuffle: with N notEmpty and P polls, each chunk is polled
        // P/N of the time. Epochs advance at rate 1-P/N per cycle. Need enough cycles
        // for the slowest chunk to reach threshold.
        int setupPolls = totalChunks;
        int notEmpty = totalChunks - AdaptivePoolingAllocator.CHUNK_REUSE_QUEUE;
        double advanceRate = 1.0 - (double) purgePolls / notEmpty;
        int cyclesNeeded = advanceRate > 0 ? (int) Math.ceil((threshold + 1) / advanceRate) + 2 : threshold + 2;
        int pollsNeeded = setupPolls + (int) (cyclesNeeded * purgePolls);
        for (int poll = 0; poll < pollsNeeded; poll++) {
            for (int i = 0; i < buffersPerChunk; i++) {
                bufs.add(allocator.heapBuffer(256));
            }
            for (ByteBuf buf : bufs) {
                buf.release();
            }
            bufs.clear();
        }

        long memoryAfterPurge = allocator.usedHeapMemory();
        assertTrue(memoryAfterPurge < memoryAfterRelease,
                "Memory should decrease after purge scans evict idle chunks. " +
                "Before purge: " + memoryAfterRelease + ", after purge: " + memoryAfterPurge);
    }
}
