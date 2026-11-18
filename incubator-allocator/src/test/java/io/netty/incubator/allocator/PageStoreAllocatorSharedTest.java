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
package io.netty.incubator.allocator;

import io.netty.buffer.AbstractAdaptiveByteBufAllocatorTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocatorMetric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** The allocator tests every adaptive-contract allocator shares, on the page-store allocator. */
public class PageStoreAllocatorSharedTest extends AbstractAdaptiveByteBufAllocatorTest<AdaptiveByteBufAllocator> {
    /** Closes every allocator a test made once it ended: the inherited tests never do. */
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    @Override
    protected Class<? extends ByteBuf> bufferClass() {
        return AdaptiveByteBuf.class;
    }

    @Override
    protected AdaptiveByteBufAllocator newDefaultAllocator() {
        return closer.add(new AdaptiveByteBufAllocator());
    }

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
        return PageStoreAllocators.chunkSize(capacity, PageStoreConfig.directDefaults());
    }

    /**
     * Malloc'd regions are one block each and are accounted whole: the first buffer commits one, and the doubled
     * buffer is another size class's chunk in the same block. In {@code mmap}'d regions the slices of the chunks
     * are accounted as they are committed.
     */
    @Override
    @Test
    public void testUsedDirectMemory() {
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        try {
            ByteBufAllocatorMetric metric = allocator.metric();
            boolean mapped = PageStoreAllocators.directRegionsAreMapped();
            long block = PageStoreConfig.directDefaults().segmentSize;
            assertEquals(0, metric.usedDirectMemory());
            ByteBuf buffer = allocator.directBuffer(1024, 4096);
            try {
                int capacity = buffer.capacity();
                assertEquals(mapped ? expectedUsedMemory(allocator, capacity) : block, metric.usedDirectMemory());
                buffer.capacity(capacity << 1);
                long both = mapped ? expectedUsedMemory(allocator, capacity)
                        + expectedUsedMemory(allocator, buffer.capacity()) : block;
                assertEquals(both, metric.usedDirectMemory(), buffer.toString());
            } finally {
                buffer.release();
            }
            // kept by the store
            assertEquals(mapped ? expectedUsedMemory(allocator, 1024) + expectedUsedMemory(allocator, 2048) : block,
                    metric.usedDirectMemory());
        } finally {
            allocator.close();
        }
    }

    /** As {@link #testUsedDirectMemory()}, for the heap blocks. */
    @Override
    @Test
    public void testUsedHeapMemory() {
        AdaptiveByteBufAllocator allocator = newAllocator(true);
        try {
            ByteBufAllocatorMetric metric = allocator.metric();
            long block = PageStoreConfig.heapDefaults().segmentSize;
            assertEquals(0, metric.usedHeapMemory());
            ByteBuf buffer = allocator.heapBuffer(1024, 4096);
            try {
                assertEquals(block, metric.usedHeapMemory());
                buffer.capacity(buffer.capacity() << 1);
                assertEquals(block, metric.usedHeapMemory(), buffer.toString());
            } finally {
                buffer.release();
            }
            assertEquals(block, metric.usedHeapMemory());
            assertEquals(0, metric.usedDirectMemory());
        } finally {
            allocator.close();
        }
    }

    /** G1 regions below 16 MiB make the heap blocks smaller than 1 MiB: such buffers are not pooled. */
    @Override
    @Test
    public void shouldReuseChunks() throws Exception {
        assumeTrue(1024 * 1024 <= PageStoreConfig.heapDefaults().segmentSize, "1 MiB heap buffers are not pooled");
        super.shouldReuseChunks();
    }
}
