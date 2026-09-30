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

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Direct chunks of an {@link AdaptiveByteBufAllocator} with {@code io.netty.allocator.mmapChunks}: each is its own
 * {@code mmap(2)}, and freeing the chunk unmaps it. Isolated because it checks {@code /proc/self/maps}, which other
 * tests' mappings could otherwise reuse between an unmap and the check.
 */
@Isolated
public class AdaptiveByteBufAllocatorMmapChunksTest {

    @Test
    void defaultFollowsTheProperty() {
        boolean requested = SystemPropertyUtil.getBoolean("io.netty.allocator.mmapChunks", false);
        assertEquals(requested && PlatformDependent.hasDirectMmap(), AdaptiveByteBufAllocator.MMAP_CHUNKS);
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true);
        ByteBuf buffer = allocator.directBuffer(1024);
        try {
            assertEquals(AdaptiveByteBufAllocator.MMAP_CHUNKS, isMmapChunk(chunkBuffer(buffer)));
        } finally {
            buffer.release();
        }
    }

    @Test
    void sizeClassedChunkIsMapped() throws IOException {
        assumeTrue(PlatformDependent.hasDirectMmap());
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false, true);
        ByteBuf buffer = allocator.directBuffer(1024);
        try {
            AbstractByteBuf chunk = chunkBuffer(buffer);
            assertTrue(isMmapChunk(chunk), chunk.getClass().getName());
            assertEquals(128 * 1024, allocator.usedDirectMemory(), "one minimum-size chunk");
            assertEquals(128 * 1024, chunk.capacity());
            assertTrue(chunk.hasMemoryAddress());
            assertEquals(0, chunk.memoryAddress() % 4096, "a mapping is page-aligned");
            assertTrue(isMapped(chunk.memoryAddress()));
            for (int i = 0; i < buffer.capacity(); i++) {
                buffer.writeByte(i);
            }
            int offsetInChunk = (int) (buffer.memoryAddress() - chunk.memoryAddress());
            for (int i = 0; i < buffer.capacity(); i++) {
                assertEquals((byte) i, buffer.getByte(i));
                assertEquals((byte) i, chunk.getByte(offsetInChunk + i));
            }
        } finally {
            buffer.release();
        }
        ByteBuf heap = allocator.heapBuffer(1024);
        try {
            assertFalse(isMmapChunk(chunkBuffer(heap)), "heap chunks are unchanged");
        } finally {
            heap.release();
        }
    }

    @Test
    void oneShotChunkIsUnmappedWithItsBuffer() throws IOException {
        assumeTrue(PlatformDependent.hasDirectMmap());
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false, true);
        int size = 2 * 1024 * 1024 + 17;
        ByteBuf buffer = allocator.directBuffer(size, Integer.MAX_VALUE);
        AbstractByteBuf chunk = chunkBuffer(buffer);
        assertTrue(isMmapChunk(chunk), chunk.getClass().getName());
        assertEquals(size, allocator.usedDirectMemory());
        long address = chunk.memoryAddress();
        assertEquals(address, buffer.memoryAddress());
        assertTrue(isMapped(address), "mapped while the buffer lives");
        buffer.setLong(size - 8, 0x0123456789ABCDEFL);

        // Growing moves it to another one-shot chunk and frees the first.
        buffer.capacity(2 * size);
        AbstractByteBuf grown = chunkBuffer(buffer);
        assertNotSame(chunk, grown);
        assertTrue(isMmapChunk(grown));
        assertEquals(0x0123456789ABCDEFL, buffer.getLong(size - 8));
        assertEquals(2 * size, allocator.usedDirectMemory());
        assertFalse(isMapped(address), "the first chunk is unmapped when the buffer leaves it");

        long grownAddress = grown.memoryAddress();
        assertTrue(isMapped(grownAddress));
        assertTrue(buffer.release());
        assertEquals(0, allocator.usedDirectMemory());
        assertFalse(isMapped(grownAddress), "the chunk is unmapped when its buffer is released");
    }

    private static AbstractByteBuf chunkBuffer(ByteBuf buffer) {
        if (buffer instanceof WrappedByteBuf) {
            // Leak detection wraps what the allocator returns.
            buffer = buffer.unwrap();
        }
        assertInstanceOf(AdaptivePoolingAllocator.AdaptiveByteBuf.class, buffer);
        return ((AdaptivePoolingAllocator.AdaptiveByteBuf) buffer).chunk.delegate;
    }

    private static boolean isMmapChunk(AbstractByteBuf chunk) {
        return chunk instanceof AdaptiveByteBufAllocator.MmapUnsafeDirectChunkByteBuf ||
                chunk instanceof AdaptiveByteBufAllocator.MmapDirectChunkByteBuf;
    }

    /**
     * Whether any mapping of this process covers {@code address}. The kernel merges adjacent anonymous mappings, so
     * the mapping is looked up by containment, not by its bounds.
     */
    private static boolean isMapped(long address) throws IOException {
        for (String line : Files.readAllLines(Paths.get("/proc/self/maps"), StandardCharsets.US_ASCII)) {
            int dash = line.indexOf('-');
            int space = line.indexOf(' ', dash);
            long start = Long.parseUnsignedLong(line.substring(0, dash), 16);
            long end = Long.parseUnsignedLong(line.substring(dash + 1, space), 16);
            if (Long.compareUnsigned(address, start) >= 0 && Long.compareUnsigned(address, end) < 0) {
                return true;
            }
        }
        return false;
    }
}
