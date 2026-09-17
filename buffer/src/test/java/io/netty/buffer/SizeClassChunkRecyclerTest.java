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
import io.netty.util.concurrent.MpscIntQueue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SizeClassChunkRecyclerTest {
    private static final int MID = AdaptivePoolingAllocator.sizeClassIndexOf(2048);
    private static final int MID2 = AdaptivePoolingAllocator.sizeClassIndexOf(4096);
    private static final int LARGE = AdaptivePoolingAllocator.sizeClassIndexOf(8192);

    private static AbstractByteBuf buffer() {
        return (AbstractByteBuf) Unpooled.buffer(AdaptivePoolingAllocator.MIN_CHUNK_SIZE);
    }

    @Test
    public void bufferComesBackWithItsFreeLists() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf buf = buffer();
        MpscIntQueue fl = MpscIntQueue.create(64, -1);

        assertTrue(recycler.offer(buf, fl, null, MID));
        assertEquals(1, recycler.size(MID));

        assertTrue(recycler.poll(MID));
        assertSame(buf, recycler.takeBuffer());
        assertSame(fl, recycler.takeFreeList());
        assertNull(recycler.takeLocalFreeList());
        assertEquals(0, recycler.size(MID));
        assertFalse(recycler.poll(MID));
        buf.release();
    }

    @Test
    public void sizeClassesWithTheSameChunkSizeShareOnePool() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf buf = buffer();
        MpscIntQueue fl = MpscIntQueue.create(64, -1);

        assertTrue(recycler.offer(buf, fl, null, MID));
        assertEquals(1, recycler.size(MID2));
        assertEquals(0, recycler.size(LARGE));

        // The other class gets the buffer, and the lists the freeing class sized: the caller checks capacity.
        assertTrue(recycler.poll(MID2));
        assertSame(buf, recycler.takeBuffer());
        assertSame(fl, recycler.takeFreeList());
        buf.release();
    }

    @Test
    public void poolIsBoundedPerChunkSize() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        int capacity = SizeClassChunkRecycler.poolCapacity(MID);
        assertTrue(capacity > 1);
        List<AbstractByteBuf> offered = new ArrayList<AbstractByteBuf>();
        for (int i = 0; i < capacity; i++) {
            AbstractByteBuf buf = buffer();
            offered.add(buf);
            assertTrue(recycler.offer(buf, null, null, MID));
        }
        AbstractByteBuf refused = buffer();
        assertFalse(recycler.offer(refused, null, null, MID));
        assertEquals(capacity, recycler.size(MID));
        refused.release();

        // LIFO: the most recently freed buffer, still warm, is the first to be reused.
        for (int i = capacity - 1; i >= 0; i--) {
            assertTrue(recycler.poll(MID));
            assertSame(offered.get(i), recycler.takeBuffer());
        }
        assertFalse(recycler.poll(MID));
        for (AbstractByteBuf buf : offered) {
            buf.release();
        }
    }

    @Test
    public void freeAllReleasesPooledBuffersAndDropsLists() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf a = buffer();
        AbstractByteBuf b = buffer();
        assertTrue(recycler.offer(a, MpscIntQueue.create(32, -1), null, MID));
        assertTrue(recycler.offer(b, MpscIntQueue.create(32, -1), null, LARGE));

        recycler.freeAll();

        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        assertEquals(0, recycler.size(MID));
        assertEquals(0, recycler.size(LARGE));
        assertFalse(recycler.poll(MID));
    }

    /**
     * End to end through the allocator, on the shared (striped) path: chunks freed by a class with few, large
     * segments are reused by the class with the most, smallest segments of the same chunk size, whose free
     * lists cannot fit in the ones that travelled with the buffer. Data must survive the swap.
     */
    @Test
    public void reuseAcrossSizeClassesWithSmallerFreeListsKeepsBuffersIntact() {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, false);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 4096; i++) {
            bufs.add(allocator.heapBuffer(4096));
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        for (int i = 0; i < 65536; i++) {
            ByteBuf buf = allocator.heapBuffer(32);
            buf.writeInt(i);
            bufs.add(buf);
        }
        for (int i = 0; i < bufs.size(); i++) {
            assertEquals(i, bufs.get(i).readInt());
        }
        for (int i = 1; i < bufs.size(); i++) {
            ByteBuf prev = bufs.get(i - 1);
            ByteBuf cur = bufs.get(i);
            if (prev.hasArray() && cur.hasArray() && prev.array() == cur.array()) {
                assertTrue(prev.arrayOffset() != cur.arrayOffset());
            }
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }
}
