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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SizeClassChunkRecyclerTest {
    private static final int SMALL = AdaptivePoolingAllocator.sizeClassIndexOf(2048);
    private static final int SMALL2 = AdaptivePoolingAllocator.sizeClassIndexOf(4096);
    private static final int MID = AdaptivePoolingAllocator.sizeClassIndexOf(8192);
    private static final int MID2 = AdaptivePoolingAllocator.sizeClassIndexOf(16384);

    private static AbstractByteBuf buffer(int sizeClassIndex) {
        int[] classes = AdaptivePoolingAllocator.getSizeClasses();
        return (AbstractByteBuf) Unpooled.buffer(AdaptivePoolingAllocator.chunkSizeFor(classes[sizeClassIndex]));
    }

    @Test
    public void sizeClassesTakeFewChunkSizes() {
        int[] classes = AdaptivePoolingAllocator.getSizeClasses();
        for (int size : classes) {
            int chunkSize = AdaptivePoolingAllocator.chunkSizeFor(size);
            boolean known = false;
            for (int c : AdaptivePoolingAllocator.CHUNK_SIZE_CLASSES) {
                known |= c == chunkSize;
            }
            assertTrue(known, "chunk size " + chunkSize + " for size class " + size);
            int segments = chunkSize / size;
            assertTrue(segments >= AdaptivePoolingAllocator.MIN_SEGMENTS_PER_CHUNK - 1,
                    "size class " + size + " gets only " + segments + " segments");
        }
        assertEquals(AdaptivePoolingAllocator.chunkSizeFor(8192), AdaptivePoolingAllocator.chunkSizeFor(16384));
        assertEquals(AdaptivePoolingAllocator.chunkSizeFor(32), AdaptivePoolingAllocator.chunkSizeFor(4096));
    }

    @Test
    public void bufferFreedByOneSizeClassServesAnotherWithTheSameChunkSize() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf small = buffer(SMALL);
        AbstractByteBuf mid = buffer(MID);
        assertTrue(recycler.offerBuffer(small, SMALL));
        assertTrue(recycler.offerBuffer(mid, MID));
        assertEquals(small.capacity() + mid.capacity(), recycler.recycledBytes());

        assertSame(mid, recycler.pollBuffer(MID2));
        assertSame(small, recycler.pollBuffer(SMALL2));
        assertNull(recycler.pollBuffer(SMALL));
        assertNull(recycler.pollBuffer(MID));
        assertEquals(0, recycler.recycledBytes());
        small.release();
        mid.release();
    }

    @Test
    public void buffersAreKeptWithinOneBudgetAcrossChunkSizes() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        List<AbstractByteBuf> kept = new ArrayList<AbstractByteBuf>();
        int chunk = buffer(MID).capacity();
        for (int i = 0; i < SizeClassChunkRecycler.RECYCLED_BYTES_BUDGET / chunk; i++) {
            AbstractByteBuf buf = buffer(MID);
            kept.add(buf);
            assertTrue(recycler.offerBuffer(buf, MID));
        }
        assertEquals(SizeClassChunkRecycler.RECYCLED_BYTES_BUDGET, recycler.recycledBytes());
        AbstractByteBuf refusedMid = buffer(MID);
        AbstractByteBuf refusedSmall = buffer(SMALL);
        assertFalse(recycler.offerBuffer(refusedMid, MID));
        assertFalse(recycler.offerBuffer(refusedSmall, SMALL), "the budget is shared, not per chunk size");
        refusedMid.release();
        refusedSmall.release();

        // LIFO: the most recently freed buffer, still warm, is the first to be reused.
        for (int i = kept.size() - 1; i >= 0; i--) {
            assertSame(kept.get(i), recycler.pollBuffer(MID2));
        }
        for (AbstractByteBuf buf : kept) {
            buf.release();
        }
    }

    @Test
    public void polledFreeListsAlwaysFitTheNeed() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        MpscIntQueue fits64 = MpscIntQueue.create(64, -1);
        MpscIntQueue fits256 = MpscIntQueue.create(256, -1);
        assertTrue(recycler.offerFreeList(fits64));
        assertTrue(recycler.offerFreeList(fits256));

        assertNull(recycler.pollFreeList(512), "nothing large enough");
        assertSame(fits256, recycler.pollFreeList(240), "a larger list serves a smaller need");
        assertSame(fits64, recycler.pollFreeList(33));
        assertNull(recycler.pollFreeList(32));
    }

    @Test
    public void freeListBucketsAreBounded() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        for (int i = 0; i < SizeClassChunkRecycler.FREE_LISTS_PER_BUCKET; i++) {
            assertTrue(recycler.offerFreeList(MpscIntQueue.create(32, -1)));
        }
        assertFalse(recycler.offerFreeList(MpscIntQueue.create(32, -1)));
        assertTrue(recycler.offerFreeList(MpscIntQueue.create(64, -1)), "other buckets are unaffected");
    }

    @Test
    public void freeAllReleasesBuffersAndDropsLists() {
        SizeClassChunkRecycler recycler = new SizeClassChunkRecycler();
        AbstractByteBuf a = buffer(SMALL);
        AbstractByteBuf b = buffer(MID);
        assertTrue(recycler.offerBuffer(a, SMALL));
        assertTrue(recycler.offerBuffer(b, MID));
        assertTrue(recycler.offerFreeList(MpscIntQueue.create(32, -1)));

        recycler.freeAll();

        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        assertEquals(0, recycler.recycledBytes());
        assertNull(recycler.pollBuffer(SMALL));
        assertNull(recycler.pollFreeList(32));
    }

    /**
     * End to end on the shared (striped) path: chunks freed by a size class are reused by another one of the
     * same chunk size, whose segments are smaller and more numerous than the free lists that were pooled with
     * the first. Data must survive the change of size class.
     */
    @Test
    public void reuseAcrossSizeClassesKeepsBuffersIntact() {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false, false);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 2048; i++) {
            bufs.add(allocator.heapBuffer(16384));
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        for (int i = 0; i < 16384; i++) {
            ByteBuf buf = allocator.heapBuffer(4352);
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
                assertTrue(Math.abs(prev.arrayOffset() - cur.arrayOffset()) >= 4352, "segments overlap");
            }
        }
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertNotNull(allocator);
    }
}
