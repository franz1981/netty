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

import io.netty.util.IllegalReferenceCountException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The shape-proving subset of section 5.0 of {@code event-loop-arena-design.md}: confinement, the
 * "no reuse before the hook" lifecycle rule, pinning, the three reallocation outcomes, and the two
 * delegate paths. Every test uses a fresh allocator instance, so it gets a fresh arena on this thread.
 */
class CycleArenaAllocatorTest {

    private static long position(ByteBuf buf) {
        if (buf.hasArray()) {
            return buf.arrayOffset();
        }
        return buf.memoryAddress();
    }

    private static CycleArenaAllocator.ArenaBuf arenaBuf(ByteBuf buf) {
        return assertInstanceOf(CycleArenaAllocator.ArenaBuf.class, buf);
    }

    private static ByteBuf allocate(CycleArenaAllocator alloc, boolean direct, int size) {
        return direct ? alloc.directBuffer(size) : alloc.heapBuffer(size);
    }

    private static int live(CycleArenaAllocator.Space space, int blockId) {
        return space.allocs[blockId] - space.frees[blockId];
    }

    private static boolean reusable(CycleArenaAllocator.Space space, int blockId) {
        return (space.reusableMask & (1 << blockId)) != 0;
    }

    private static CycleArenaAllocator.Space space(CycleArenaAllocator alloc, boolean direct) {
        CycleArenaAllocator.Arena arena = alloc.arenaForTest();
        assertNotNull(arena);
        return direct ? arena.direct : arena.heap;
    }

    /** Run something on another thread and return whatever it threw, or null. */
    private static Throwable onOtherThread(final Runnable body) throws Exception {
        final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                }
            }
        });
        t.start();
        t.join();
        return thrown.get();
    }

    // ------------------------------------------------------------------ Invariant A (confinement)

    @Test
    void releaseOnAnotherThreadThrowsHeap() throws Exception {
        confinement(false);
    }

    @Test
    void releaseOnAnotherThreadThrowsDirect() throws Exception {
        confinement(true);
    }

    private void confinement(boolean direct) throws Exception {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            final ByteBuf buf = allocate(alloc, direct, 128);
            arenaBuf(buf);
            long before = alloc.arenaForTest().violations;

            assertInstanceOf(IllegalStateException.class, onOtherThread(new Runnable() {
                @Override
                public void run() {
                    buf.release();
                }
            }));
            assertInstanceOf(IllegalStateException.class, onOtherThread(new Runnable() {
                @Override
                public void run() {
                    buf.retain();
                }
            }));
            assertInstanceOf(IllegalStateException.class, onOtherThread(new Runnable() {
                @Override
                public void run() {
                    buf.retain(2);
                }
            }));
            assertInstanceOf(IllegalStateException.class, onOtherThread(new Runnable() {
                @Override
                public void run() {
                    buf.release(2);
                }
            }));
            // The violation is counted before the throw, and nothing was touched: the buffer is intact.
            assertEquals(before + 4, alloc.arenaForTest().violations);
            assertEquals(1, buf.refCnt());
            assertTrue(buf.release());
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void releaseThroughSliceOnAnotherThreadThrows() throws Exception {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf buf = alloc.heapBuffer(128);
            final ByteBuf slice = buf.slice(0, 64);
            final ByteBuf dup = buf.duplicate();
            assertInstanceOf(IllegalStateException.class, onOtherThread(new Runnable() {
                @Override
                public void run() {
                    slice.release();
                }
            }));
            assertInstanceOf(IllegalStateException.class, onOtherThread(new Runnable() {
                @Override
                public void run() {
                    dup.retain();
                }
            }));
            assertEquals(1, buf.refCnt());
            assertTrue(buf.release());
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void doubleReleaseThrows() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            final ByteBuf buf = alloc.heapBuffer(128);
            assertTrue(buf.release());
            assertThrows(IllegalReferenceCountException.class, new org.junit.jupiter.api.function.Executable() {
                @Override
                public void execute() {
                    buf.release();
                }
            });
            assertThrows(IllegalReferenceCountException.class, new org.junit.jupiter.api.function.Executable() {
                @Override
                public void execute() {
                    buf.retain();
                }
            });
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------------------------------ the lifecycle rule

    @Test
    void freedRegionIsNotHandedOutBeforeTheHookHeap() {
        noReuseBeforeHook(false);
    }

    @Test
    void freedRegionIsNotHandedOutBeforeTheHookDirect() {
        noReuseBeforeHook(true);
    }

    private void noReuseBeforeHook(boolean direct) {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf first = allocate(alloc, direct, 1024);
            long firstPosition = position(first);
            assertTrue(first.release());

            // Same iteration: the region just freed must NOT come back.
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < 16; i++) {
                ByteBuf buf = allocate(alloc, direct, 1024);
                assertNotEquals(firstPosition, position(buf));
                live.add(buf);
            }
            for (ByteBuf buf : live) {
                assertTrue(buf.release());
            }

            // The hook is the only thing that makes freed memory reusable.
            alloc.runHookForTest();
            ByteBuf afterHook = allocate(alloc, direct, 1024);
            assertEquals(firstPosition, position(afterHook));
            assertTrue(afterHook.release());
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void blockStaysPinnedAcrossHooksUntilItsLastBufferIsReleased() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf pinning = alloc.heapBuffer(64);
            CycleArenaAllocator.Space space = space(alloc, false);
            int firstBlock = space.curId;

            // Fill the first block and let the space move on to a second one.
            List<ByteBuf> filler = new ArrayList<ByteBuf>();
            while (space.blockCount() < 2) {
                filler.add(alloc.heapBuffer(CycleArenaAllocator.CAP));
            }
            for (ByteBuf buf : filler) {
                buf.release();
            }
            assertEquals(2, space.blockCount());
            assertEquals(1, live(space, firstBlock));

            alloc.runHookForTest();
            assertEquals(1, space.pinnedBlocks);
            assertFalse(reusable(space, firstBlock));
            alloc.runHookForTest();
            assertEquals(1, space.pinnedBlocks);
            assertFalse(reusable(space, firstBlock));
            assertEquals(1, space.maxPinned);

            assertTrue(pinning.release());
            assertEquals(0, live(space, firstBlock));
            assertFalse(reusable(space, firstBlock), "release alone must not make a block reusable");

            alloc.runHookForTest();
            assertEquals(0, space.pinnedBlocks);
            assertTrue(reusable(space, firstBlock));
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------------------------------ reallocation

    @Test
    void capacityGrowsInPlaceWhenTopmost() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf buf = alloc.heapBuffer(64);
            buf.writeLong(0x0102030405060708L);
            long position = position(buf);
            buf.capacity(256);
            assertEquals(position, position(buf));
            assertEquals(256, buf.capacity());
            assertEquals(0x0102030405060708L, buf.getLong(0));
            assertEquals(1, space(alloc, false).reallocInPlace);
            assertTrue(buf.release());
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void capacityMovesWhenNotTopmostAndTheOldViewSurvivesTheIteration() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf buf = alloc.heapBuffer(64);
            buf.writeLong(0x0102030405060708L);
            ByteBuf topmost = alloc.heapBuffer(64);        // now buf is not topmost any more
            ByteBuffer oldView = buf.nioBuffer(0, 8);
            long position = position(buf);

            buf.capacity(256);
            assertNotEquals(position, position(buf));
            assertEquals(1, space(alloc, false).reallocMoved);
            assertEquals(0x0102030405060708L, buf.getLong(0));
            // The old region was released, so it cannot be handed out before the next hook: the view
            // taken during this iteration still reads what it read before.
            assertEquals(0x0102030405060708L, oldView.getLong(0));

            assertTrue(buf.release());
            assertTrue(topmost.release());
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void capacityDelegatesWhenTheNewRegionCannotComeFromTheArena() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf buf = alloc.heapBuffer(64);
            buf.writeLong(0x0102030405060708L);
            ByteBuf topmost = alloc.heapBuffer(64);        // not topmost: a move is needed
            CycleArenaAllocator.Space space = space(alloc, false);
            int blockId = arenaBuf(buf).blockIdForTest();
            int liveBefore = live(space, blockId);

            buf.capacity(CycleArenaAllocator.CAP + 1024);  // above the cap: only the delegate can serve it
            assertEquals(1, space.reallocDelegated);
            assertEquals(CycleArenaAllocator.DELEGATE_SLOT, arenaBuf(buf).blockIdForTest(),
                    "a delegated buffer has no block");
            assertEquals(0x0102030405060708L, buf.getLong(0));
            assertEquals(CycleArenaAllocator.CAP + 1024, buf.capacity());
            assertEquals(liveBefore - 1, live(space, blockId));   // the old region went back to its block

            // Releasing a DELEGATED buffer releases the delegate buffer and pools the object again.
            int freeTop = space.freeTop;
            assertTrue(buf.release());
            assertEquals(freeTop + 1, space.freeTop);
            assertEquals(0, live(space, CycleArenaAllocator.DELEGATE_SLOT));
            assertTrue(topmost.release());
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------------------------------ the delegate paths

    @Test
    void aboveTheCapIsDelegated() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf small = alloc.heapBuffer(CycleArenaAllocator.CAP);
            arenaBuf(small);
            ByteBuf big = alloc.heapBuffer(CycleArenaAllocator.CAP + 1);
            assertFalse(big instanceof CycleArenaAllocator.ArenaBuf, "above the cap must delegate");
            assertEquals(1, space(alloc, false).delegateAllocations);
            ByteBuf bigDirect = alloc.directBuffer(CycleArenaAllocator.CAP + 1);
            assertFalse(bigDirect instanceof CycleArenaAllocator.ArenaBuf);
            assertEquals(1, space(alloc, true).delegateAllocations);
            small.release();
            big.release();
            bigDirect.release();
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void exhaustedObjectPoolDelegates() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            List<ByteBuf> live = new ArrayList<ByteBuf>(CycleArenaAllocator.MAX_OBJECTS + 1);
            for (int i = 0; i < CycleArenaAllocator.MAX_OBJECTS; i++) {
                ByteBuf buf = alloc.heapBuffer(8);
                arenaBuf(buf);
                live.add(buf);
            }
            ByteBuf overflow = alloc.heapBuffer(8);
            assertFalse(overflow instanceof CycleArenaAllocator.ArenaBuf,
                    "maxObjects reached must delegate");
            live.add(overflow);
            for (ByteBuf buf : live) {
                buf.release();
            }
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void allBlocksPinnedDelegates() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            CycleArenaAllocator.Space space = null;
            // Keep one live buffer per block, and keep going until the bound is reached.
            while (true) {
                ByteBuf buf = alloc.heapBuffer(CycleArenaAllocator.CAP);
                if (space == null) {
                    space = space(alloc, false);
                }
                if (!(buf instanceof CycleArenaAllocator.ArenaBuf)) {
                    buf.release();                          // the space gave up: the delegate served it
                    break;
                }
                live.add(buf);
            }
            assertNotNull(space);
            assertEquals(CycleArenaAllocator.MAX_BLOCKS, space.blockCount());
            assertTrue(space.delegateAllocations > 0);
            for (ByteBuf buf : live) {
                buf.release();
            }
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------------------------------ trim and termination

    @Test
    void trimReturnsEveryReusableBlockButTheFirst() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            while (true) {
                ByteBuf buf = alloc.heapBuffer(CycleArenaAllocator.CAP);
                if (!(buf instanceof CycleArenaAllocator.ArenaBuf)) {
                    buf.release();
                    break;
                }
                live.add(buf);
            }
            CycleArenaAllocator.Space space = space(alloc, false);
            assertEquals(CycleArenaAllocator.MAX_BLOCKS, space.blockCount());
            for (ByteBuf buf : live) {
                buf.release();
            }
            alloc.runHookForTest();
            assertEquals(CycleArenaAllocator.MAX_BLOCKS - 1, space.reusableBlocks());

            alloc.trim();
            assertEquals(2, space.blockCount(), "the current block and one reusable block are kept");
            assertEquals(CycleArenaAllocator.MAX_BLOCKS - 2, space.trimmedBlocks);
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void terminationFreesEmptyBlocksAndCountsTheLeakedOnes() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        ByteBuf pinning = alloc.heapBuffer(64);
        CycleArenaAllocator.Space heap = space(alloc, false);
        CycleArenaAllocator.Space direct = space(alloc, true);
        int leaked = heap.curId;
        AbstractByteBuf leakedRoot = heap.roots[leaked];
        alloc.removeForTest();
        assertEquals(0, heap.blockCount());
        assertEquals(1, heap.leakedBlocks, "a block with a live buffer cannot be freed by anyone");
        assertEquals(0, direct.leakedBlocks);
        assertEquals(1, pinning.refCnt());
        // The arena leaks this block on purpose (design section 7); free it here so that the leak
        // detector of the test suite does not report what the test is asserting.
        leakedRoot.release();
    }

    // ------------------------------------------------------------------ no in-band metadata

    @Test
    void blockMemoryHoldsNothingButPayload() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf first = alloc.heapBuffer(64);
            CycleArenaAllocator.Space space = space(alloc, false);
            byte[] block = space.roots[space.curId].array();
            java.util.Arrays.fill(block, (byte) 0x5A);     // paint the whole block, payload included

            // Allocate, release, grow in place, switch nothing, run the hook, allocate again: the arena
            // must not write one byte of bookkeeping into the block.
            ByteBuf second = alloc.heapBuffer(128);
            second.capacity(256);
            assertTrue(second.release());
            assertTrue(first.release());
            alloc.runHookForTest();
            ByteBuf third = alloc.heapBuffer(64);
            assertTrue(third.release());
            alloc.trim();

            for (int i = 0; i < block.length; i++) {
                if (block[i] != (byte) 0x5A) {
                    throw new AssertionError("the arena wrote metadata into block memory at " + i
                            + ": 0x" + Integer.toHexString(block[i] & 0xff));
                }
            }
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------------------------------ metrics

    @Test
    void derivedTotalsMatchTheAllocationsMade() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            CycleArenaAllocator.Space space = null;
            int n = 5000;
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < n; i++) {
                ByteBuf buf = alloc.heapBuffer(64);
                arenaBuf(buf);
                if (space == null) {
                    space = space(alloc, false);
                }
                live.add(buf);
                if ((i & 63) == 0) {              // release and close iterations as we go
                    for (ByteBuf b : live) {
                        b.release();
                    }
                    live.clear();
                    alloc.runHookForTest();
                }
            }
            for (ByteBuf b : live) {
                b.release();
            }
            assertNotNull(space);
            assertEquals(n, space.arenaAllocations(),
                    "the totals accumulated at block reset must equal the allocations made");
            assertEquals(0, space.liveBuffers());
            assertTrue(space.bytesBumpedTotal() >= (long) n * 64);
        } finally {
            alloc.removeForTest();
        }
    }

    @Test
    void countersMentionEveryArena() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            alloc.heapBuffer(64).release();
            String counters = CycleArenaAllocator.counters();
            assertTrue(counters.startsWith("ARENATELE"), counters);
            assertTrue(counters.contains("arenaShare="), counters);
            assertTrue(counters.contains("ARENALOOP thread="), counters);
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------------------------------ the ring (-Darena.ring)

    /** A buffer's place in its block, unique across blocks: block id and offset, never an address. */
    private static long slot(ByteBuf buf) {
        CycleArenaAllocator.ArenaBuf ab = assertInstanceOf(CycleArenaAllocator.ArenaBuf.class, buf);
        return (long) ab.blockIdForTest() * CycleArenaAllocator.BLOCK_SIZE + ab.startForTest();
    }

    /**
     * (1) The shape of the ring: a freed slot is NOT the next one handed out - the tail keeps bumping
     * forward over untouched bytes - and it comes back exactly when the tail reaches the wall and wraps.
     */
    @Test
    void theFreedSlotComesBackOnlyWhenTheTailWraps() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf a = alloc.heapBuffer(1024);
            CycleArenaAllocator.Space space = space(alloc, false);
            assertEquals(0, arenaBuf(a).startForTest());
            long slotA = slot(a);
            assertTrue(a.release());

            // b is next, and the tail has room: b lands AFTER a's slot, not on it.
            ByteBuf b = alloc.heapBuffer(1024);
            assertEquals(1024, arenaBuf(b).startForTest());
            assertNotEquals(slotA, slot(b));
            assertEquals(0, space.ringWraps);

            // Run the tail to the top of the block with only b alive. b is the lowest live start and it
            // leaves 1024 free bytes below it, so the allocation that hits the wall wraps onto a's slot.
            ByteBuf wrapped = null;
            while (space.ringWraps == 0) {
                ByteBuf filler = alloc.heapBuffer(1024);
                assertEquals(1, space.blockCount());
                if (space.ringWraps == 1) {
                    wrapped = filler;
                } else {
                    assertNotEquals(slotA, slot(filler), "a's bytes came back before the tail wrapped");
                    assertTrue(filler.release());
                }
            }
            assertNotNull(wrapped);
            assertEquals(0, space.blockSwitches);
            assertEquals(slotA, slot(wrapped), "the wrapped tail hands out what the head gave back");
            assertTrue(wrapped.release());
            assertTrue(b.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * (2) A FIFO stream of far more than two blocks' worth, with never more than a quarter of a block
     * live, must never leave the first block: that is the whole point of the ring.
     */
    @Test
    void aFifoStreamStaysInOneBlock() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            CycleArenaAllocator.Space space = null;
            ArrayDeque<ByteBuf> window = new ArrayDeque<ByteBuf>();
            long bytes = 0;
            while (bytes < 4L * CycleArenaAllocator.BLOCK_SIZE) {
                ByteBuf buf = alloc.heapBuffer(1024);
                arenaBuf(buf);
                if (space == null) {
                    space = space(alloc, false);
                }
                window.addLast(buf);
                bytes += 1024;
                if (window.size() > 64) {                  // 64 KiB live, a quarter of the block
                    assertTrue(window.pollFirst().release());
                }
            }
            while (!window.isEmpty()) {
                assertTrue(window.pollFirst().release());
            }
            assertNotNull(space);
            assertEquals(1, space.blockCount(), "a FIFO stream must fit in one block");
            assertEquals(0, space.blockSwitches);
            assertTrue(space.ringWraps > 0, "the tail never wrapped: ringWraps=" + space.ringWraps);
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * (3) A survivor at the very bottom of a block leaves nothing to wrap into: the ring must never hand
     * out its bytes, and when the tail reaches the top the space must switch block instead.
     */
    @Test
    void theRingNeverHandsOutASurvivorAtTheBottom() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf survivor = alloc.heapBuffer(1024);
            CycleArenaAllocator.Space space = space(alloc, false);
            int survivorBlock = arenaBuf(survivor).blockIdForTest();
            assertEquals(0, arenaBuf(survivor).startForTest());

            long bytes = 0;
            while (bytes < 3L * CycleArenaAllocator.BLOCK_SIZE) {
                ByteBuf buf = alloc.heapBuffer(1024);
                CycleArenaAllocator.ArenaBuf ab = arenaBuf(buf);
                if (ab.blockIdForTest() == survivorBlock) {
                    // [start, end) must be disjoint from the survivor's [0, 1024).
                    assertTrue(ab.startForTest() >= 1024,
                            "the ring handed out the survivor's bytes at " + ab.startForTest());
                }
                bytes += 1024;
                assertTrue(buf.release());
            }
            assertEquals(0, space.ringWraps, "a survivor at offset 0 leaves no room to wrap into");
            assertEquals(1, space.blockSwitches, "the pinned block is left exactly once");
            assertEquals(2, space.blockCount());
            assertTrue(space.ringResets > 0, "the empty second block must restart in place");
            assertEquals(1, live(space, survivorBlock));
            assertTrue(survivor.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /** (4) Growing in place moves the tail, so it must stop at the ring's wall, not at the block's top. */
    @Test
    void inPlaceGrowthStopsAtTheRingWall() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf first = alloc.heapBuffer(1024);          // [0, 1024)
            ByteBuf wall = alloc.heapBuffer(1024);           // [1024, 2048): what the tail will stop at
            assertTrue(first.release());
            CycleArenaAllocator.Space space = space(alloc, false);

            ByteBuf topmost = null;
            while (space.ringWraps == 0) {
                ByteBuf filler = alloc.heapBuffer(1024);
                if (space.ringWraps == 1) {
                    topmost = filler;
                } else {
                    assertTrue(filler.release());
                }
            }
            assertNotNull(topmost);
            assertEquals(0, arenaBuf(topmost).startForTest());
            assertEquals(1024, space.curLimit);
            assertEquals(1024, space.curBump);               // topmost fills the whole wrapped region
            topmost.writeLong(0x0102030405060708L);

            long slotBefore = slot(topmost);
            long inPlace = space.reallocInPlace;
            topmost.capacity(2048);                          // would cross the wall: it must move instead
            assertEquals(inPlace, space.reallocInPlace, "in-place growth must stop at curLimit");
            assertEquals(1, space.reallocMoved);
            assertNotEquals(slotBefore, slot(topmost));
            assertEquals(2048, topmost.capacity());
            assertEquals(0x0102030405060708L, topmost.getLong(0));
            assertEquals(1024, arenaBuf(wall).startForTest(), "the wall buffer never moved");
            assertTrue(topmost.release());
            assertTrue(wall.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /** (5) {@code -Darena.ring=false}: nothing is handed out again before a hook, block after block. */
    @Test
    void withoutTheRingNothingComesBackBeforeTheHook() {
        assumeFalse(CycleArenaAllocator.RING, "the ring is on");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            CycleArenaAllocator.Space space = null;
            Set<Long> seen = new HashSet<Long>();
            long bytes = 0;
            while (bytes < 2L * CycleArenaAllocator.BLOCK_SIZE) {
                ByteBuf buf = alloc.heapBuffer(1024);
                if (space == null) {
                    space = space(alloc, false);
                }
                assertTrue(seen.add(slot(buf)), "a slot came back before any hook ran");
                bytes += 1024;
                assertTrue(buf.release());                   // released at once, and still never reused
            }
            assertNotNull(space);
            assertTrue(space.blockCount() > 1, "without the ring the space must switch block");
            assertEquals(0, space.ringWraps + space.ringResets);

            // The hook is still the only thing that brings a block back.
            alloc.runHookForTest();
            ByteBuf afterHook = alloc.heapBuffer(1024);
            assertFalse(seen.add(slot(afterHook)), "the hook must bring an already-used slot back");
            assertTrue(afterHook.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * {@code -Darena.ringStats=true}: the stall measurement itself. One block, two survivors with a dead
     * 1 KiB gap between them, and the tail run up to the top - so the arithmetic is known exactly.
     */
    @Test
    void ringStatsMeasureTheStrandedBytesAndTheHoles() {
        assumeTrue(CycleArenaAllocator.RING && CycleArenaAllocator.RING_STATS, "ring stats are off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf a = alloc.heapBuffer(1024);              // [0, 1024): kept
            ByteBuf gap = alloc.heapBuffer(1024);            // [1024, 2048): dies at once
            ByteBuf b = alloc.heapBuffer(256);               // [2048, 2304): kept
            CycleArenaAllocator.Space space = space(alloc, false);
            assertEquals(0, arenaBuf(a).startForTest());
            assertEquals(2048, arenaBuf(b).startForTest());
            assertTrue(gap.release());

            while (space.ringStalls == 0) {                  // run the tail to the top of the block
                assertTrue(alloc.heapBuffer(1024).release());
            }
            int blockSize = CycleArenaAllocator.BLOCK_SIZE;
            assertEquals(1, space.ringStalls);
            assertEquals(blockSize, space.stallBytes);
            // The tail stalls at the last 1 KiB slot that still fits, not at the top of the block.
            int tail = 2304;
            while (tail + 1024 <= blockSize) {
                tail += 1024;
            }
            // Live: [0,1024) and [2048,2304). Holes: [1024,2048) and [2304, tail).
            assertEquals(tail - 1280, space.strandedBytes);
            assertEquals(1, space.holes1k, "the 1 KiB gap between the two survivors");
            assertEquals(1, space.holesBig, "everything above the last survivor");
            assertEquals(0, space.holes256 + space.holes4k + space.holes8k);
            assertTrue(a.release());
            assertTrue(b.release());
        } finally {
            alloc.removeForTest();
        }
    }

    // ------------------------------------------- the bitmap at a block edge, and re-entry into a block

    /** The word of {@code blockId}'s bitmap row that holds {@code start}'s live bit. */
    private static int bitWord(int start) {
        return start >>> 9;
    }

    private static boolean liveBitSet(CycleArenaAllocator.Space space, int blockId, int start) {
        return (space.startBits[blockId][bitWord(start)] & 1L << (start >>> 3)) != 0;
    }

    /**
     * (7) The bitmap at a block boundary. The last 8-byte slot of block 0 is the LAST bit of block 0's
     * row and the first slot of block 1 the FIRST bit of block 1's - the same word index in two rows.
     * Releasing either must leave the other alone, whatever the rows are laid out in.
     */
    @Test
    void theBitmapSeparatesTheBlockBoundary() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            int blockSize = CycleArenaAllocator.BLOCK_SIZE;
            ByteBuf bottom = alloc.heapBuffer(8);              // pins [0, 8): the ring can never wrap
            CycleArenaAllocator.Space space = space(alloc, false);
            assertEquals(CycleArenaAllocator.MAX_BLOCKS + 1, space.startBits.length,
                    "one bitmap row per block, plus the delegate slot");

            ByteBuf top = null;                                // run the tail to the very top of block 0
            while (space.curBump < blockSize) {
                ByteBuf buf = alloc.heapBuffer(8);
                if (space.curBump == blockSize) {
                    top = buf;
                } else {
                    assertTrue(buf.release());
                }
            }
            assertNotNull(top);
            assertEquals(0, arenaBuf(top).blockIdForTest());
            assertEquals(blockSize - 8, arenaBuf(top).startForTest());
            int topWord = bitWord(blockSize - 8);
            assertEquals(CycleArenaAllocator.BLOCK_LONGS - 1, topWord, "the last word of block 0's row");
            assertTrue(liveBitSet(space, 0, blockSize - 8));

            // The tail is at the wall and the bottom is pinned, so the next one opens block 1 at 0.
            ByteBuf first = alloc.heapBuffer(8);
            assertEquals(2, space.blockCount());
            assertEquals(1, arenaBuf(first).blockIdForTest());
            assertEquals(0, arenaBuf(first).startForTest());
            assertEquals(0, bitWord(0), "block 1's first slot is the first word of block 1's row");
            assertNotSame(space.startBits[0], space.startBits[1], "the two blocks must not share a row");
            assertTrue(liveBitSet(space, 1, 0));

            assertTrue(first.release());
            assertFalse(liveBitSet(space, 1, 0), "block 1's bit must be the one that was cleared");
            assertTrue(liveBitSet(space, 0, blockSize - 8), "block 0's top slot must still be live");
            assertTrue(liveBitSet(space, 0, 0), "and so must its bottom");

            assertTrue(top.release());
            assertTrue(bottom.release());
            assertFalse(liveBitSet(space, 0, blockSize - 8));
            assertFalse(liveBitSet(space, 0, 0));
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * (8) Re-entry below the head. Block 0 is abandoned with its bottom pinned, then that survivor goes:
     * the block is now free but no hook has run, so nothing marks it reusable. When block 1 stalls the
     * space must come BACK to block 0 instead of taking a third block.
     */
    @Test
    void aStalledRingReentersTheBlockWhoseHeadHasDied() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf bottom0 = alloc.heapBuffer(8);             // pins block 0 at [0, 8)
            CycleArenaAllocator.Space space = space(alloc, false);
            fillToTheWall(alloc, space);
            ByteBuf bottom1 = alloc.heapBuffer(8);             // block 0 gave up: this opens block 1 at 0
            assertEquals(2, space.blockCount());
            assertEquals(1, arenaBuf(bottom1).blockIdForTest());
            assertEquals(0, space.ringReentries);

            assertTrue(bottom0.release());                     // block 0 is empty now - but no hook ran
            assertEquals(0, space.reusableMask);
            fillToTheWall(alloc, space);

            ByteBuf back = alloc.heapBuffer(8);
            assertEquals(1, space.ringReentries, "the stalled ring must re-enter block 0");
            assertEquals(2, space.blockCount(), "and must NOT have taken a third block");
            assertEquals(0, arenaBuf(back).blockIdForTest());
            assertEquals(0, arenaBuf(back).startForTest());

            assertTrue(back.release());
            assertTrue(bottom1.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * (9) Re-entry into the window at a block's TAIL. Block 0 is abandoned by a request too big for the
     * bytes left in front of its tail; those bytes are still free, and a later small request must land
     * exactly there rather than grow the space.
     */
    @Test
    void aStalledRingReentersTheWindowLeftAtATail() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        int blockSize = CycleArenaAllocator.BLOCK_SIZE;
        int window = 4096;
        assumeTrue(CycleArenaAllocator.CAP >= window * 2, "the cap cannot straddle the window");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf bottom0 = alloc.heapBuffer(8);             // pins block 0 at [0, 8): no wrap, ever
            CycleArenaAllocator.Space space = space(alloc, false);
            while (space.curBump < blockSize - window) {       // leave exactly `window` in front of the tail
                assertTrue(alloc.heapBuffer(8).release());
            }
            assertEquals(blockSize - window, space.curBump);

            ByteBuf tooBig = alloc.heapBuffer(window * 2);     // does not fit: block 0 is abandoned
            assertEquals(2, space.blockCount());
            assertEquals(1, arenaBuf(tooBig).blockIdForTest());
            assertEquals(0, space.ringReentries);

            ByteBuf bottom1 = alloc.heapBuffer(8);             // pin block 1 too, so it cannot wrap either
            fillToTheWall(alloc, space);

            ByteBuf back = alloc.heapBuffer(window);           // fits the window block 0 kept, and only it
            assertEquals(1, space.ringReentries, "the stalled ring must re-enter block 0's tail window");
            assertEquals(2, space.blockCount(), "and must NOT have taken a third block");
            assertEquals(0, arenaBuf(back).blockIdForTest());
            assertEquals(blockSize - window, arenaBuf(back).startForTest());
            assertTrue(liveBitSet(space, 0, blockSize - window));

            assertTrue(back.release());
            assertTrue(tooBig.release());
            assertTrue(bottom0.release());
            assertTrue(bottom1.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * (10) A zero-length buffer must still occupy a slot. The bitmap has one bit per 8-byte slot and
     * indexes it by START, so if a 0-byte buffer were handed out at the tail without moving it, it
     * would share its bit with the buffer that comes next: releasing the empty one would clear a LIVE
     * buffer's bit and the ring could then hand out its bytes.
     */
    @Test
    void aZeroLengthBufferTakesItsOwnSlot() {
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf empty = alloc.heapBuffer(0);
            ByteBuf next = alloc.heapBuffer(8);
            CycleArenaAllocator.Space space = space(alloc, false);
            assertEquals(0, arenaBuf(empty).startForTest());
            assertEquals(8, arenaBuf(next).startForTest(), "the empty buffer must not share a start");
            assertEquals(16, space.curBump);
            if (CycleArenaAllocator.RING) {
                assertTrue(liveBitSet(space, arenaBuf(next).blockIdForTest(), 8));
                assertTrue(empty.release());
                assertTrue(liveBitSet(space, arenaBuf(next).blockIdForTest(), 8),
                        "releasing the empty buffer must not clear its neighbour's bit");
            } else {
                assertTrue(empty.release());
            }
            assertTrue(next.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /**
     * (11) The same at the very top of a block: with the tail exactly at the wall a zero-length request
     * must move to another block, not index the bitmap one word past its end.
     */
    @Test
    void aZeroLengthBufferAtTheWallSwitchesBlock() {
        assumeTrue(CycleArenaAllocator.RING, "the ring is off");
        CycleArenaAllocator alloc = new CycleArenaAllocator();
        try {
            ByteBuf bottom = alloc.heapBuffer(8);              // pins [0, 8): the ring cannot wrap
            CycleArenaAllocator.Space space = space(alloc, false);
            fillToTheWall(alloc, space);
            assertEquals(CycleArenaAllocator.BLOCK_SIZE, space.curBump);

            ByteBuf zero = alloc.heapBuffer(0);                // the request that used to throw
            assertEquals(2, space.blockCount());
            assertEquals(1, arenaBuf(zero).blockIdForTest());
            assertEquals(0, arenaBuf(zero).startForTest());
            assertEquals(0, zero.capacity());
            assertTrue(zero.release());
            assertTrue(bottom.release());
        } finally {
            alloc.removeForTest();
        }
    }

    /** Bump the current block's tail to its wall with 8-byte buffers, keeping none of them. */
    private static void fillToTheWall(CycleArenaAllocator alloc, CycleArenaAllocator.Space space) {
        int before = space.curId;
        while (space.curBump < CycleArenaAllocator.BLOCK_SIZE && space.curId == before) {
            assertTrue(alloc.heapBuffer(8).release());
        }
        assertEquals(before, space.curId, "the fill must not have switched block by itself");
        assertEquals(CycleArenaAllocator.BLOCK_SIZE, space.curBump);
    }
}
