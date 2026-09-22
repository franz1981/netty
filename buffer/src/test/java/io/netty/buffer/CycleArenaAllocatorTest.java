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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            CycleArenaAllocator.Block firstBlock = space.current;

            // Fill the first block and let the space move on to a second one.
            List<ByteBuf> filler = new ArrayList<ByteBuf>();
            while (space.blockCount < 2) {
                filler.add(alloc.heapBuffer(CycleArenaAllocator.CAP));
            }
            for (ByteBuf buf : filler) {
                buf.release();
            }
            assertEquals(2, space.blockCount);
            assertEquals(1, firstBlock.live);

            alloc.runHookForTest();
            assertEquals(1, space.pinnedBlocks);
            assertFalse(firstBlock.reusable);
            alloc.runHookForTest();
            assertEquals(1, space.pinnedBlocks);
            assertFalse(firstBlock.reusable);
            assertEquals(1, space.maxPinned);

            assertTrue(pinning.release());
            assertEquals(0, firstBlock.live);
            assertFalse(firstBlock.reusable, "release alone must not make a block reusable");

            alloc.runHookForTest();
            assertEquals(0, space.pinnedBlocks);
            assertTrue(firstBlock.reusable);
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
            CycleArenaAllocator.Block block = arenaBuf(buf).blockForTest();

            buf.capacity(CycleArenaAllocator.CAP + 1024);  // above the cap: only the delegate can serve it
            assertEquals(1, space(alloc, false).reallocDelegated);
            assertNull(arenaBuf(buf).blockForTest(), "a delegated buffer has no block");
            assertEquals(0x0102030405060708L, buf.getLong(0));
            assertEquals(CycleArenaAllocator.CAP + 1024, buf.capacity());
            assertEquals(0, block.live - 1);               // the old region was given back to its block

            // Releasing a DELEGATED buffer releases the delegate buffer and pools the object again.
            CycleArenaAllocator.Arena arena = alloc.arenaForTest();
            int freeTop = arena.freeTop;
            assertTrue(buf.release());
            assertEquals(freeTop + 1, arena.freeTop);
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
            assertEquals(CycleArenaAllocator.MAX_BLOCKS, space.blockCount);
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
            assertEquals(CycleArenaAllocator.MAX_BLOCKS, space.blockCount);
            for (ByteBuf buf : live) {
                buf.release();
            }
            alloc.runHookForTest();
            assertEquals(CycleArenaAllocator.MAX_BLOCKS - 1, space.reusableBlocks);

            alloc.trim();
            assertEquals(2, space.blockCount, "the current block and one reusable block are kept");
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
        CycleArenaAllocator.Block leaked = heap.current;
        alloc.removeForTest();
        assertEquals(0, heap.blockCount);
        assertEquals(1, heap.leakedBlocks, "a block with a live buffer cannot be freed by anyone");
        assertEquals(0, direct.leakedBlocks);
        assertEquals(1, pinning.refCnt());
        // The arena leaks this block on purpose (design section 7); free it here so that the leak
        // detector of the test suite does not report what the test is asserting.
        leaked.root.release();
    }

    // ------------------------------------------------------------------ metrics

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
}
