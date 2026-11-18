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
import io.netty.incubator.allocator.PageStoreAllocators.Memory;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.netty.incubator.allocator.BufferChecks.assertDisjoint;
import static io.netty.incubator.allocator.BufferChecks.releaseAll;
import static io.netty.incubator.allocator.BufferChecks.stamp;
import static io.netty.incubator.allocator.BufferChecks.verify;
import static io.netty.incubator.allocator.PageStoreAllocators.aboveABlock;
import static io.netty.incubator.allocator.PageStoreAllocators.assumeThreadLocalHeaps;
import static io.netty.incubator.allocator.PageStoreAllocators.directRegionsAreMapped;
import static io.netty.incubator.allocator.PageStoreAllocators.mmapWhereLinked;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.PageStoreAllocators.pooledBuffers;
import static io.netty.incubator.allocator.PageStoreAllocators.useUntilQuiet;
import static io.netty.incubator.allocator.TestThreads.runOnThread;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The allocator's used memory, as {@code usedDirectMemory()} and {@code usedHeapMemory()} report it: what the first
 * buffers of each kind take, and that memory given back is memory the next round uses, so that nothing leaks or is
 * counted twice. Expected amounts come from the configuration and the size class table. Where regions are
 * {@code malloc}'d, the first buffer takes a block; where they are mapped, the slices it claims.
 */
class PageStoreAccountingTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    private AdaptiveByteBufAllocator allocator(Memory kind) {
        return newAllocator(closer, ticker, kind);
    }

    private static ByteBuf allocate(AdaptiveByteBufAllocator alloc, Memory kind, int size) {
        return kind.direct ? alloc.directBuffer(size) : alloc.heapBuffer(size);
    }

    private static long used(AdaptiveByteBufAllocator alloc, Memory kind) {
        return kind.direct ? alloc.usedDirectMemory() : alloc.usedHeapMemory();
    }

    /** The bytes of the chunk of the size class of {@code size}. */
    private static long chunkBytes(Memory kind, int size) {
        PageStoreConfig config = kind.config();
        return (long) new SizeClassTable(config).chunkSlices[SizeClassTable.sizeClassIndex(size)] * config.sliceSize;
    }

    /**
     * Exactly what the first buffer of each kind takes: a block, or the slices of its chunk where regions are mapped;
     * a span of 5 slices takes a block or its slices, and a second one in that block nothing more or its slices; a
     * buffer above a block takes its own memory or whole blocks; and a block's worth exactly, plus one buffer, takes
     * a second block.
     */
    @ParameterizedTest
    @EnumSource(Memory.class)
    void usedMemoryIsWhatTheFirstBufferOfEachKindTakes(Memory kind) {
        PageStoreConfig config = kind.config();
        long block = config.segmentSize;
        long slice = config.sliceSize;
        List<ByteBuf> live = new ArrayList<ByteBuf>();
        try {
            AdaptiveByteBufAllocator alloc = allocator(kind);
            live.add(allocate(alloc, kind, 1024));
            assertEquals(kind.mapped ? chunkBytes(kind, 1024) : block, used(alloc, kind),
                    "a size class's first buffer");
            releaseAll(live);

            // Spans of 5 slices, on an allocator of their own: the first takes a block, or its slices where mapped,
            // and the second, in that block, takes nothing more, or its slices.
            AdaptiveByteBufAllocator spans = allocator(kind);
            int span = (int) (4 * slice + slice / 2);
            live.add(allocate(spans, kind, span));
            assertEquals(kind.mapped ? 5 * slice : block, used(spans, kind), "a span of 5 slices");
            live.add(allocate(spans, kind, span));
            assertEquals(kind.mapped ? 10 * slice : block, used(spans, kind), "a second span of 5 slices");
            releaseAll(live);

            AdaptiveByteBufAllocator oneShot = allocator(kind);
            live.add(allocate(oneShot, kind, (int) block + 1));
            assertEquals(kind.mapped ? 2 * block : block + 1, used(oneShot, kind),
                    "a buffer above a block: a run of two blocks where mapped, else its own memory");
            releaseAll(live);

            AdaptiveByteBufAllocator exact = allocator(kind);
            live.add(allocate(exact, kind, (int) block));
            assertEquals(block, used(exact, kind), "a whole block");
            live.add(allocate(exact, kind, 1));
            assertEquals(block + (kind.mapped ? chunkBytes(kind, 1) : block), used(exact, kind),
                    "one more buffer takes a second block, or the slices of its chunk");
        } finally {
            releaseAll(live);
        }
    }

    /** One thread-local heap or stripe, and who releases. */
    static final class Round {
        final Memory kind;
        final boolean threadLocal;
        final int size;
        final boolean remote;
        final int order;

        Round(Memory kind, boolean threadLocal, int size, boolean remote, int order) {
            this.kind = kind;
            this.threadLocal = threadLocal;
            this.size = size;
            this.remote = remote;
            this.order = order;
        }

        @Override
        public String toString() {
            return kind + ", " + (threadLocal ? "thread-local heap" : "stripe") + ", " + size + " bytes, released by "
                    + (remote ? "another thread" : "the owner") + ", " + new String[] {"in order", "reversed",
                    "every other one first"}[order];
        }
    }

    static Stream<Round> rounds() {
        List<Round> rounds = new ArrayList<Round>();
        for (Memory kind : Memory.values()) {
            for (boolean threadLocal : new boolean[] {true, false}) {
                for (int size : new int[] {256, 1024, 16_896}) {
                    for (boolean remote : new boolean[] {false, true}) {
                        for (int order = 0; order < 3; order++) {
                            rounds.add(new Round(kind, threadLocal, size, remote, order));
                        }
                    }
                }
            }
        }
        return rounds.stream();
    }

    private static List<ByteBuf> inReleaseOrder(List<ByteBuf> buffers, int order) {
        List<ByteBuf> ordered = new ArrayList<ByteBuf>(buffers);
        if (order == 1) {
            Collections.reverse(ordered);
        } else if (order == 2) {
            List<ByteBuf> everyOther = new ArrayList<ByteBuf>();
            List<ByteBuf> rest = new ArrayList<ByteBuf>();
            for (int i = 0; i < ordered.size(); i++) {
                (i % 2 == 0 ? everyOther : rest).add(ordered.get(i));
            }
            everyOther.addAll(rest);
            ordered = everyOther;
        }
        return ordered;
    }

    /**
     * A store with more whole blocks held than one 64-bit word of a map covers keeps every one of them: they share no
     * memory and keep their bytes at both ends, the memory used is their blocks, and once they are released the same
     * number can be held again without more memory.
     */
    @ParameterizedTest(name = "mapped: {0}")
    @ValueSource(booleans = {false, true})
    void aStoreOfManyWholeBlocksKeepsEveryOne(boolean mapped) {
        if (mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        }
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.TWO_MIB_BLOCKS,
                mapped ? mmapWhereLinked() : null);
        int block = Geometry.TWO_MIB_BLOCKS.direct.segmentSize;
        // 66 blocks in use, more than the 64 that one word of the store's maps covers.
        int count = 66;
        for (int round = 0; round < 2; round++) {
            List<ByteBuf> held = new ArrayList<ByteBuf>();
            try {
                for (int i = 0; i < count; i++) {
                    ByteBuf buf = alloc.directBuffer(block);
                    held.add(buf);
                    buf.setLong(0, 1000L * round + i + 1);
                    buf.setLong(block - 8, -(1000L * round + i + 1));
                }
                assertDisjoint(held);
                assertEquals((long) count * block, alloc.usedDirectMemory(), "round " + round);
                for (int i = 0; i < count; i++) {
                    assertEquals(1000L * round + i + 1, held.get(i).getLong(0));
                    assertEquals(-(1000L * round + i + 1), held.get(i).getLong(block - 8));
                }
            } finally {
                releaseAll(held);
            }
        }
    }

    /**
     * A buffer of a whole region's blocks holds no memory once released, with nothing allocated after it; the same
     * buffer again takes the memory again and keeps its bytes.
     */
    @ParameterizedTest(name = "mapped: {0}")
    @ValueSource(booleans = {false, true})
    void aBufferOfAWholeRegionHoldsNoMemoryOnceReleased(boolean mapped) {
        if (mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        }
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.TWO_MIB_BLOCKS,
                mapped ? mmapWhereLinked() : null);
        int size = Geometry.TWO_MIB_BLOCKS.direct.regionSize;
        for (int round = 1; round <= 2; round++) {
            ByteBuf buf = alloc.directBuffer(size);
            try {
                assertEquals(size, alloc.usedDirectMemory(), "round " + round);
                buf.setLong(0, round).setLong(size - 8, -round);
                assertEquals(round, buf.getLong(0));
                assertEquals(-round, buf.getLong(size - 8));
            } finally {
                buf.release();
            }
            assertEquals(0, alloc.usedDirectMemory(), "round " + round + ": the released buffer kept its memory");
        }
    }

    /**
     * A released buffer of many blocks gives back its own memory and no other: buffers of a whole block allocated
     * before and after it keep their bytes, and the memory in use drops by its blocks.
     */
    @Test
    void aReleasedBufferOfManyBlocksGivesBackOnlyItsOwnMemory() {
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.TWO_MIB_BLOCKS, mmapWhereLinked());
        PageStoreConfig config = Geometry.TWO_MIB_BLOCKS.direct;
        int block = config.segmentSize;
        int size = (config.segmentsPerRegion() - 2) * block;
        List<ByteBuf> neighbours = new ArrayList<ByteBuf>();
        try {
            neighbours.add(alloc.directBuffer(block));
            ByteBuf large = alloc.directBuffer(size);
            neighbours.add(alloc.directBuffer(block));
            for (int i = 0; i < neighbours.size(); i++) {
                stamp(neighbours.get(i), 5 + i, 0, 4096);
                neighbours.get(i).setLong(block - 8, 5 + i);
            }
            long used = alloc.usedDirectMemory();
            large.release();
            for (int i = 0; i < neighbours.size(); i++) {
                verify(neighbours.get(i), 5 + i, 4096);
                assertEquals(5 + i, neighbours.get(i).getLong(block - 8));
            }
            assertEquals(used - size, alloc.usedDirectMemory());
        } finally {
            releaseAll(neighbours);
        }
    }

    /**
     * Blocks released before the store's maps grew are found after it: 33 blocks of two full regions are released, a
     * buffer of a whole region's blocks takes a third region, which makes the maps grow, and small buffers are then
     * served from the released blocks, taking no memory.
     */
    @Test
    void releasedBlocksAreFoundAfterTheMapsGrew() {
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.TWO_MIB_BLOCKS, mmapWhereLinked());
        PageStoreConfig config = Geometry.TWO_MIB_BLOCKS.direct;
        int block = config.segmentSize;
        int regionBlocks = config.segmentsPerRegion();
        List<ByteBuf> singles = new ArrayList<ByteBuf>();
        List<ByteBuf> others = new ArrayList<ByteBuf>();
        try {
            for (int i = 0; i < 2 * regionBlocks; i++) {
                singles.add(alloc.directBuffer(block));
            }
            List<ByteBuf> released = new ArrayList<ByteBuf>(singles.subList(0, 33));
            singles.subList(0, 33).clear();
            releaseAll(released);
            // Both regions have only the 33 released blocks free: a whole region's worth needs a third.
            others.add(alloc.directBuffer(regionBlocks * block));
            long used = alloc.usedDirectMemory();
            // The third region is full: only the released blocks can serve these.
            for (int i = 0; i < 4; i++) {
                others.add(alloc.directBuffer(1024));
            }
            assertEquals(used, alloc.usedDirectMemory(), "the released blocks were not found again");
            List<ByteBuf> all = new ArrayList<ByteBuf>(singles);
            all.addAll(others);
            assertDisjoint(all);
        } finally {
            releaseAll(singles);
            releaseAll(others);
        }
    }

    /**
     * Buffers of one size fill three blocks exactly, are all released, by the owner or by another thread and in
     * several orders, and the same round again needs no memory more: a slot, chunk or span that was lost would take a
     * block of its own. The size of each scenario is checked first: the fill takes three blocks' worth, and one more
     * buffer takes a block, or its chunk's slices.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("rounds")
    void aSecondRoundAfterTheReleasesNeedsNoMoreMemory(Round round) throws Throwable {
        if (round.threadLocal) {
            assumeThreadLocalHeaps();
        }
        Memory kind = round.kind;
        final AdaptiveByteBufAllocator alloc = allocator(kind);
        PageStoreConfig config = kind.config();
        SizeClassTable table = new SizeClassTable(config);
        int index = SizeClassTable.sizeClassIndex(round.size);
        long perBlock = (long) table.slots[index] * (config.slicesPerSegment() / table.chunkSlices[index]);
        assertEquals(0, config.slicesPerSegment() % table.chunkSlices[index], "chunks tile a block");
        final int count = (int) (3 * perBlock);
        final long newChunk = kind.mapped ? chunkBytes(kind, round.size) : config.segmentSize;
        // The scenario's size first, on an allocator of its own: the fill takes three blocks' worth and one more buffer
        // takes a new chunk, so a round that does not fill its blocks fails here and not silently below.
        final AdaptiveByteBufAllocator probe = allocator(kind);
        runOnThread(round.threadLocal, () -> {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < count; i++) {
                live.add(allocate(probe, kind, round.size));
            }
            assertEquals(3L * config.segmentSize, used(probe, kind), "the fill is three blocks");
            live.add(allocate(probe, kind, round.size));
            assertEquals(3L * config.segmentSize + newChunk, used(probe, kind), "one more buffer takes a new chunk");
            releaseAll(live);
        });
        final long[] firstRound = new long[1];
        runOnThread(round.threadLocal, () -> {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int pass = 0; pass < 2; pass++) {
                for (int i = 0; i < count; i++) {
                    live.add(allocate(alloc, kind, round.size));
                }
                if (pass == 0) {
                    firstRound[0] = used(alloc, kind);
                } else {
                    assertEquals(firstRound[0], used(alloc, kind), "the second round took memory");
                }
                final List<ByteBuf> ordered = inReleaseOrder(live, round.order);
                live.clear();
                if (round.remote) {
                    runOnThread(false, () -> releaseAll(ordered));
                } else {
                    releaseAll(ordered);
                }
            }
        });
        assertEquals(3L * config.segmentSize, firstRound[0]);
        assertEquals(firstRound[0], used(alloc, kind), "the releases do not change what is used");
    }

    /**
     * A heap that ended hands what it had to the next one: a second thread-local heap's identical round takes no
     * memory more than the first's.
     */
    @ParameterizedTest
    @EnumSource(Memory.class)
    void aSecondHeapUsesWhatTheFirstOneFreed(Memory kind) throws Throwable {
        assumeThreadLocalHeaps();
        final AdaptiveByteBufAllocator alloc = allocator(kind);
        PageStoreConfig config = kind.config();
        // Two blocks of 256-byte buffers, exactly: the probe on an allocator of its own shows that the fill takes two
        // blocks and one buffer more takes a new chunk, so a round that did not fill its blocks fails here.
        SizeClassTable table = new SizeClassTable(config);
        int index = SizeClassTable.sizeClassIndex(256);
        assertEquals(0, config.slicesPerSegment() % table.chunkSlices[index], "chunks tile a block");
        final int count = 2 * table.slots[index] * (config.slicesPerSegment() / table.chunkSlices[index]);
        final AdaptiveByteBufAllocator probe = allocator(kind);
        runOnThread(true, () -> {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < count; i++) {
                live.add(allocate(probe, kind, 256));
            }
            assertEquals(2L * config.segmentSize, used(probe, kind), "the fill is two blocks");
            live.add(allocate(probe, kind, 256));
            assertEquals(2L * config.segmentSize + (kind.mapped ? chunkBytes(kind, 256) : config.segmentSize),
                    used(probe, kind), "one more buffer takes a new chunk");
            releaseAll(live);
        });
        final long[] first = new long[1];
        runOnThread(true, () -> {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < count; i++) {
                live.add(allocate(alloc, kind, 256));
            }
            first[0] = used(alloc, kind);
            releaseAll(live);
        });
        assertEquals(2L * config.segmentSize, first[0], "the first heap's round");
        runOnThread(true, () -> {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < count; i++) {
                live.add(allocate(alloc, kind, 256));
            }
            assertEquals(first[0], used(alloc, kind), "the second heap took memory");
            releaseAll(live);
        });
        assertEquals(first[0], used(alloc, kind));
    }

    /**
     * Free memory serves other sizes: spans of one length, freed, serve spans of twice the length from the same
     * block, and the chunks of one size class, freed, serve another class's.
     */
    @ParameterizedTest
    @EnumSource(Memory.class)
    void freedMemoryServesOtherSizes(Memory kind) {
        PageStoreConfig config = kind.config();
        int slice = config.sliceSize;
        List<ByteBuf> live = new ArrayList<ByteBuf>();
        try {
            AdaptiveByteBufAllocator alloc = allocator(kind);
            int count = config.slicesPerSegment() / 4;
            for (int i = 0; i < count; i++) {
                live.add(allocate(alloc, kind, 3 * slice + slice / 2));
            }
            long filled = used(alloc, kind);
            assertEquals(kind.mapped ? 4L * count * slice : config.segmentSize, filled, "the spans fill one block");
            releaseAll(live);
            for (int i = 0; i < count / 2; i++) {
                live.add(allocate(alloc, kind, 7 * slice + slice / 2));
            }
            assertEquals(filled, used(alloc, kind), "spans of 8 slices from the freed spans of 4");
            releaseAll(live);

            // Chunks: the first class gives its chunks back but the few it keeps, which the second cannot have.
            SizeClassTable table = new SizeClassTable(config);
            int first = SizeClassTable.sizeClassIndex(1024);
            int second = SizeClassTable.sizeClassIndex(4096);
            assertEquals(1, table.chunkSlices[first], "one-slice chunks");
            assertEquals(1, table.chunkSlices[second], "one-slice chunks");
            AdaptiveByteBufAllocator chunks = allocator(kind);
            long slicesTaken = 2L * config.slicesPerSegment();
            for (long i = 0; i < slicesTaken * table.slots[first]; i++) {
                live.add(allocate(chunks, kind, 1024));
            }
            long both = used(chunks, kind);
            assertEquals(2L * config.segmentSize, both);
            releaseAll(live);
            // Half of what the first class freed: how many chunks a class keeps is the allocator's policy, and the test
            // leaves half. Had nothing been reused, the second class would need a block of its own.
            long room = slicesTaken / 2;
            for (long i = 0; i < room * table.slots[second]; i++) {
                live.add(allocate(chunks, kind, 4096));
            }
            assertEquals(both, used(chunks, kind), "the freed chunks of one class serve the other");
        } finally {
            releaseAll(live);
        }
    }

    /** With nothing live, the close gives back everything, and closing again is harmless. */
    @ParameterizedTest
    @EnumSource(Memory.class)
    void closeReturnsEverything(Memory kind) throws Throwable {
        AdaptiveByteBufAllocator alloc = allocator(kind);
        final List<ByteBuf> live = new ArrayList<ByteBuf>();
        for (int size : new int[] {100, 5000, 300_000, kind.config().segmentSize, kind.config().segmentSize + 1}) {
            live.add(allocate(alloc, kind, size));
        }
        runOnThread(false, () -> live.add(allocate(alloc, kind, 2000)));
        assertTrue(used(alloc, kind) > 0);
        releaseAll(live);
        alloc.close();
        assertEquals(0, alloc.usedDirectMemory());
        assertEquals(0, alloc.usedHeapMemory());
        alloc.close();
        assertEquals(0, alloc.usedDirectMemory());
        assertEquals(0, alloc.usedHeapMemory());
    }

    /**
     * A buffer that outgrows its slot leaves the slot free: as many grow-and-release cycles as would fill several
     * blocks if each kept its slot keep the memory at one block (or the slices of its chunks where mapped).
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aBufferThatOutgrowsItsSlotGivesItBack(boolean direct) {
        Memory kind = direct ? Memory.DIRECT_MALLOC : Memory.HEAP;
        AdaptiveByteBufAllocator alloc = allocator(kind);
        long block = kind.config().segmentSize;
        int cycles = (int) (4 * block / 1024);
        long perCycle = -1;
        for (int i = 0; i < cycles; i++) {
            ByteBuf buf = allocate(alloc, kind, 1000);
            buf.capacity(1500);
            buf.release();
            if (i == 0) {
                perCycle = used(alloc, kind);
                assertEquals(block, perCycle);
            }
            assertEquals(perCycle, used(alloc, kind));
        }
    }

    /**
     * A stripe that no thread allocates on keeps its chunks: purge passes and allocations above a block leave them, and
     * its next buffer of the class takes no memory.
     */
    @ParameterizedTest
    @EnumSource(value = Memory.class, names = {"DIRECT_MALLOC", "HEAP"})
    void aStripeNobodyAllocatesOnKeepsItsChunks(Memory kind) throws Exception {
        AdaptiveByteBufAllocator alloc = allocator(kind);
        int block = kind.config().segmentSize;
        int count = (int) (2L * block / 1024);
        runOnThread(false, () -> {
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            for (int i = 0; i < count; i++) {
                live.add(allocate(alloc, kind, 1024));
            }
            releaseAll(live);
        });
        useUntilQuiet(alloc, ticker, aboveABlock(alloc, kind.direct, block));
        long kept = used(alloc, kind);
        assertTrue(kept > 0, "the stripe gave its chunks up");
        useUntilQuiet(alloc, ticker, aboveABlock(alloc, kind.direct, block));
        assertEquals(kept, used(alloc, kind), "a second quiet time gave memory back");
        runOnThread(false, () -> {
            ByteBuf buf = allocate(alloc, kind, 1024);
            assertEquals(kept, used(alloc, kind), "the next buffer of the class took memory");
            buf.release();
        });
    }

    /**
     * Allocations of one size class give the chunks of another, idle one back: with a chunk of 1024-byte buffers left
     * free and 100,000-byte buffers in use, used memory drops, on a thread with a heap of its own and on one without.
     */
    @ParameterizedTest
    @CsvSource({"DIRECT_MALLOC,true", "DIRECT_MALLOC,false", "HEAP,true", "HEAP,false"})
    void allocationsOfOneSizeClassGiveAnotherIdleOnesChunksBack(Memory kind, boolean threadLocal) throws Exception {
        if (threadLocal) {
            assumeThreadLocalHeaps();
        }
        AdaptiveByteBufAllocator alloc = allocator(kind);
        runOnThread(threadLocal, () -> {
            allocate(alloc, kind, 1024).release();
            long before = used(alloc, kind);
            Runnable use = pooledBuffers(alloc, kind.direct);
            use.run();
            long afterOneStep = used(alloc, kind);
            assertTrue(afterOneStep > before, "the other size class took no region of its own");
            for (int steps = 0; used(alloc, kind) >= afterOneStep; steps++) {
                if (steps == 1000) {
                    fail("the idle size class's chunks were not given back in 1000 steps");
                }
                ticker.advance(2, TimeUnit.HOURS);
                use.run();
            }
        });
    }
}
