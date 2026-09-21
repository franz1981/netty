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

import io.netty.buffer.AdaptivePoolingAllocator.SizeClassChunkRecycler;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SizeClassChunkRecyclerTest {
    // 2048 and 4096 share the MIN_CHUNK_SIZE pool; 8192 has a chunk size of its own (asserted below).
    private static final int SMALL = AdaptivePoolingAllocator.sizeClassIndexOf(2048);
    private static final int SMALL2 = AdaptivePoolingAllocator.sizeClassIndexOf(4096);
    private static final int LARGE = AdaptivePoolingAllocator.sizeClassIndexOf(8192);

    private static int chunkSize(int sizeClassIndex) {
        return AdaptivePoolingAllocator.chunkSizeOf(AdaptivePoolingAllocator.getSizeClasses()[sizeClassIndex]);
    }

    private static AbstractByteBuf buffer(int sizeClassIndex) {
        return (AbstractByteBuf) Unpooled.buffer(chunkSize(sizeClassIndex));
    }

    @Test
    public void poolSharingFollowsChunkSize() {
        assertEquals(chunkSize(SMALL), chunkSize(SMALL2));
        assertNotEquals(chunkSize(SMALL2), chunkSize(LARGE));
    }

    @Test
    public void bufferComesBack() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf buf = buffer(SMALL);

        assertTrue(recycler.offer(buf, SMALL));
        assertEquals(1, recycler.size(SMALL));

        assertSame(buf, recycler.poll(SMALL));
        assertEquals(0, recycler.size(SMALL));
        assertNull(recycler.poll(SMALL));
        buf.release();
    }

    @Test
    public void sizeClassesWithTheSameChunkSizeShareOnePool() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf buf = buffer(SMALL);

        assertTrue(recycler.offer(buf, SMALL));
        assertEquals(1, recycler.size(SMALL2));
        assertEquals(0, recycler.size(LARGE));

        assertSame(buf, recycler.poll(SMALL2));
        buf.release();
    }

    @Test
    public void poolIsBoundedPerChunkSize() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        int capacity = SizeClassChunkRecycler.poolCapacity(SMALL);
        assertTrue(capacity > 1);
        List<AbstractByteBuf> offered = new ArrayList<AbstractByteBuf>();
        for (int i = 0; i < capacity; i++) {
            AbstractByteBuf buf = buffer(SMALL);
            offered.add(buf);
            assertTrue(recycler.offer(buf, SMALL));
        }
        AbstractByteBuf refused = buffer(SMALL);
        assertFalse(recycler.offer(refused, SMALL));
        assertEquals(capacity, recycler.size(SMALL));
        refused.release();

        // LIFO: the most recently freed buffer, still warm, is the first to be reused.
        for (int i = capacity - 1; i >= 0; i--) {
            assertSame(offered.get(i), recycler.poll(SMALL));
        }
        assertNull(recycler.poll(SMALL));
        for (AbstractByteBuf buf : offered) {
            buf.release();
        }
    }

    @Test
    public void oneByteBudgetBoundsAllThePools() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        // Fill the budget with buffers of one chunk size.
        int capacity = SizeClassChunkRecycler.poolCapacity(LARGE);
        List<AbstractByteBuf> offered = new ArrayList<AbstractByteBuf>();
        for (int i = 0; i < capacity; i++) {
            AbstractByteBuf buf = buffer(LARGE);
            offered.add(buf);
            assertTrue(recycler.offer(buf, LARGE));
        }
        // The budget is per recycler, not per chunk size: a buffer of another chunk size is refused too.
        AbstractByteBuf other = buffer(SMALL);
        assertFalse(recycler.offer(other, SMALL));
        // Taking one out makes room again.
        recycler.poll(LARGE).release();
        assertTrue(recycler.offer(other, SMALL));
        recycler.freeAll();
    }

    @Test
    public void freeAllReleasesPooledBuffers() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf a = buffer(SMALL);
        AbstractByteBuf b = buffer(LARGE);
        assertTrue(recycler.offer(a, SMALL));
        assertTrue(recycler.offer(b, LARGE));

        recycler.freeAll();

        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        assertEquals(0, recycler.size(SMALL));
        assertEquals(0, recycler.size(LARGE));
        assertNull(recycler.poll(SMALL));
    }

    /**
     * End to end through the allocator: chunks of one size class are freed past the cache floor, so their buffers go
     * to the recycler, and a different size class with the same chunk size must reuse them, whatever the first one
     * left in them: fewer and larger segments (4096 then 32), more and smaller (32 then 4096), or a segment size that
     * does not divide the chunk (1152: 113 segments and a tail; then 1024: 128 segments).
     * Size classes that are not adjacent share chunks too: 16896 then 67584. From 16 KiB up a whole family (2^n, and
     * 2^n plus header) shares one chunk size, as the blocks of a mimalloc medium page do: 131072 then 16384.
     */
    @ParameterizedTest
    @CsvSource({
            "4096, 32, false", "32, 4096, false", "1152, 1024, false", "16896, 67584, false", "131072, 16384, false",
            "4096, 32, true", "32, 4096, true", "1152, 1024, true", "16896, 67584, true", "131072, 16384, true",
    })
    public void chunksAreReusedAcrossSizeClasses(int freedSize, int reusingSize, boolean threadLocal)
            throws Exception {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, threadLocal);
        Runnable test = () -> assertReusedAcrossSizeClasses(allocator, freedSize, reusingSize);
        if (threadLocal) {
            FastThreadLocalThread.runWithFastThreadLocal(test);
        } else {
            test.run();
        }
    }

    private static void assertReusedAcrossSizeClasses(AdaptiveByteBufAllocator allocator, int freedSize,
                                                      int reusingSize) {
        int chunkSize = AdaptivePoolingAllocator.chunkSizeOf(freedSize);
        assertEquals(chunkSize, AdaptivePoolingAllocator.chunkSizeOf(reusingSize));
        // 64 chunks: well past the cache floor (32 chunks of MIN_CHUNK_SIZE), so the rest go to the recycler.
        int chunks = 64;

        Set<byte[]> freedArrays = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < chunks * (chunkSize / freedSize); i++) {
            ByteBuf buf = allocator.heapBuffer(freedSize, freedSize);
            freedArrays.add(buf.array());
            bufs.add(buf);
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();

        boolean reused = false;
        IdentityHashMap<byte[], Set<Integer>> segments = new IdentityHashMap<byte[], Set<Integer>>();
        int count = chunks * (chunkSize / reusingSize);
        for (int i = 0; i < count; i++) {
            ByteBuf buf = allocator.heapBuffer(reusingSize, reusingSize);
            buf.writeInt(i);
            reused |= freedArrays.contains(buf.array());
            Set<Integer> offsets = segments.get(buf.array());
            if (offsets == null) {
                offsets = new HashSet<Integer>();
                segments.put(buf.array(), offsets);
            }
            assertTrue(offsets.add(buf.arrayOffset()), "segment handed out twice");
            bufs.add(buf);
        }
        assertTrue(reused, "no chunk buffer of the first size class was reused by the second");
        for (int i = 0; i < bufs.size(); i++) {
            assertEquals(i, bufs.get(i).readInt());
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }
}
