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

import io.netty.buffer.ByteBuf;
import io.netty.incubator.allocator.PageStoreAllocators.Geometry;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.netty.incubator.allocator.PageStoreAllocators.mmapWhereLinked;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AdaptiveByteBufAllocatorTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    @Test
    void heapAndDirectRoundTrip() {
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.DEFAULTS, mmapWhereLinked());
        // Everything is free after a round trip: time passes while the next ones allocate, and the allocator still
        // serves.
        for (int i = 0; i < 3; i++) {
            roundTrip(alloc);
            ticker.advance(2, TimeUnit.HOURS);
        }
    }

    private static void roundTrip(AdaptiveByteBufAllocator alloc) {
        for (int size : new int[] {1, 64, 1000, 20000, 300000, 3000000}) {
            for (boolean direct : new boolean[] {false, true}) {
                ByteBuf buf = direct ? alloc.directBuffer(size) : alloc.heapBuffer(size);
                try {
                    assertEquals(direct, buf.isDirect());
                    assertEquals(size, buf.capacity());
                    byte[] data = new byte[size];
                    for (int i = 0; i < size; i++) {
                        data[i] = (byte) (i * 31);
                    }
                    buf.writeBytes(data);
                    byte[] back = new byte[size];
                    buf.readBytes(back);
                    assertTrue(Arrays.equals(data, back));
                    if (size >= 8) {
                        buf.setLong(0, 0x0102030405060708L);
                        assertEquals(0x0102030405060708L, buf.getLong(0));
                    }
                } finally {
                    assertTrue(buf.release());
                }
            }
        }
    }

    @Test
    void memoryAddressOfReleasedBufferIsGone() {
        AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, false);
        try {
            ByteBuf buf = alloc.directBuffer(128);
            if (buf.hasMemoryAddress()) {
                assertTrue(buf.memoryAddress() != 0);
            }
            buf.release();
            assertThrows(IllegalReferenceCountException.class, buf::memoryAddress);
        } finally {
            alloc.close();
        }
    }

    /**
     * The address is the chunk's plus the buffer's offset, and the buffer's own accessors never read it (they go
     * through the root), so only a comparison with the NIO buffer's address can see a wrong one: size class (16 B,
     * 1000 B), span (above the largest pooled size), and one-shot (above a block), then growth across kinds.
     */
    @Test
    void memoryAddressIsTheAddressOfTheNioBuffer() {
        AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, false);
        int block = PageStoreConfig.directDefaults().segmentSize;
        try {
            for (int size : new int[] {16, 1000, 1024 * 1024 + 1, block / 2, block, block + 1, 2 * block + 5}) {
                // two buffers: the second is likely at an offset above its chunk's start
                ByteBuf first = alloc.directBuffer(size, Integer.MAX_VALUE);
                ByteBuf second = alloc.directBuffer(size, Integer.MAX_VALUE);
                try {
                    assumeTrue(first.hasMemoryAddress());
                    assumeTrue(PlatformDependent.hasDirectByteBufferAddress(first.nioBuffer(0, first.capacity())));
                    assertAddress(first);
                    assertAddress(second);
                    // growth moves the buffer to a chunk of another kind
                    second.ensureWritable(size + 128 * 1024);
                    assertAddress(second);
                    second.capacity(16);
                    assertAddress(second);
                } finally {
                    first.release();
                    second.release();
                }
            }
        } finally {
            alloc.close();
        }
    }

    private static void assertAddress(ByteBuf buf) {
        assertEquals(PlatformDependent.directBufferAddress(buf.nioBuffer(0, buf.capacity())), buf.memoryAddress(),
                buf.toString());
    }

    /** A copy of a one-shot buffer keeps the maximum capacity, so it grows as the original does. */
    @Test
    void copyOfAOneShotBufferCanGrow() {
        AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, false);
        try {
            for (boolean direct : new boolean[] {true, false}) {
                if (direct && PageStoreAllocators.directRegionsAreMapped()) {
                    // 3 blocks are a span of a mapped region, whose root has the region's maximum capacity: a copy
                    // of any pooled buffer takes the maximum capacity of its root. One-shot roots need a size above
                    // a whole region.
                    continue;
                }
                int size = 3 * PageStoreConfig.directDefaults().segmentSize;
                ByteBuf buf = direct ? alloc.directBuffer(size, Integer.MAX_VALUE)
                        : alloc.heapBuffer(size, Integer.MAX_VALUE);
                ByteBuf copy = null;
                try {
                    buf.writeByte(1);
                    copy = buf.copy();
                    assertEquals(buf.maxCapacity(), copy.maxCapacity());
                    copy.ensureWritable(size + 1);
                    assertTrue(copy.capacity() >= size + 1);
                } finally {
                    buf.release();
                    if (copy != null) {
                        copy.release();
                    }
                }
            }
        } finally {
            alloc.close();
        }
    }
}
