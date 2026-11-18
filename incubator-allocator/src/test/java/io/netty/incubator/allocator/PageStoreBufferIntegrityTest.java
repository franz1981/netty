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
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.netty.incubator.allocator.BufferChecks.assertDisjoint;
import static io.netty.incubator.allocator.BufferChecks.releaseAll;
import static io.netty.incubator.allocator.BufferChecks.stamp;
import static io.netty.incubator.allocator.BufferChecks.verify;
import static io.netty.incubator.allocator.PageStoreAllocators.assumeThreadLocalHeaps;
import static io.netty.incubator.allocator.PageStoreAllocators.aWholeBlock;
import static io.netty.incubator.allocator.PageStoreAllocators.mmapWhereLinked;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.PageStoreAllocators.useUntilQuiet;
import static io.netty.incubator.allocator.PageStoreAllocators.directRegionsAreMapped;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * That buffers keep their own bytes and their own memory, through the {@link ByteBuf} API alone: every byte of every
 * buffer is stamped with a pattern of its own and read back, and the writable ranges of the live buffers are checked
 * to be disjoint. Over direct and heap buffers, the heap of the calling thread or a stripe, and the geometries of
 * blocks and regions the page store has.
 */
class PageStoreBufferIntegrityTest {

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    enum Layout {
        DEFAULT_BLOCKS(Geometry.DEFAULTS, false),
        TWO_MIB_BLOCKS(Geometry.TWO_MIB_BLOCKS, false),
        SEVEN_SLICE_HEAP_BLOCKS(Geometry.SEVEN_SLICE_HEAP_BLOCKS, false),
        MAPPED_REGIONS(Geometry.SMALL_REGIONS, true);

        final Geometry geometry;
        final boolean mapped;

        Layout(Geometry geometry, boolean mapped) {
            this.geometry = geometry;
            this.mapped = mapped;
        }
    }

    /** Where the buffers of a test come from. */
    static final class Case {
        final boolean direct;
        final boolean threadLocal;
        final Layout layout;

        Case(boolean direct, boolean threadLocal, Layout layout) {
            this.direct = direct;
            this.threadLocal = threadLocal;
            this.layout = layout;
        }

        PageStoreConfig config() {
            return direct ? layout.geometry.direct : layout.geometry.heap;
        }

        @Override
        public String toString() {
            return (direct ? "direct" : "heap") + (threadLocal ? ", thread-local heap" : ", stripe") + ", " + layout;
        }
    }

    static Stream<Case> cases() {
        List<Case> cases = new ArrayList<Case>();
        for (boolean threadLocal : new boolean[] {true, false}) {
            for (boolean direct : new boolean[] {true, false}) {
                for (Layout layout : Layout.values()) {
                    boolean fits = layout.mapped ? direct : layout != Layout.SEVEN_SLICE_HEAP_BLOCKS || !direct;
                    if (fits) {
                        cases.add(new Case(direct, threadLocal, layout));
                    }
                }
            }
        }
        return cases.stream();
    }

    /** A cut-down list for the tests that need not every geometry. */
    static Stream<Case> casesOfDefaultAndMappedBlocks() {
        return cases().filter(c -> c.layout == Layout.DEFAULT_BLOCKS || c.layout == Layout.MAPPED_REGIONS);
    }

    /** Where the test runs the body: a thread of its own, with a heap of its own or not. */
    private interface Body {
        void run(AdaptiveByteBufAllocator alloc) throws Exception;
    }

    private AdaptiveByteBufAllocator run(Case c, Body body) throws Throwable {
        if (c.threadLocal) {
            assumeThreadLocalHeaps();
        }
        Function<AdaptiveByteBufAllocator, MmapRegionSource> mmap = null;
        if (c.layout.mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
            mmap = mmapWhereLinked();
        }
        final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.layout.geometry, mmap);
        TestThreads.runOnThread(c.threadLocal, () -> body.run(alloc));
        return alloc;
    }

    private static ByteBuf allocate(AdaptiveByteBufAllocator alloc, boolean direct, int size) {
        return direct ? alloc.directBuffer(size) : alloc.heapBuffer(size);
    }

    /**
     * Every size the allocator has a way for: each size class, the spans above them up to a block, a block, a block
     * and one byte more, and for mapped regions one more than a region. Of each size, enough to fill more than a
     * block: all stamped, read back and disjoint.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void everySizeKeepsItsBytesAndItsOwnMemory(Case c) throws Throwable {
        run(c, alloc -> {
            int block = c.config().segmentSize;
            List<Integer> sizes = new ArrayList<Integer>();
            for (int size : SizeClassTable.SIZES) {
                sizes.add(size);
                sizes.add(size - 1);
            }
            sizes.add(SizeClassTable.SIZES[SizeClassTable.SIZES_COUNT - 1] + 1);
            sizes.add(300_000);
            sizes.add(1024 * 1024);
            sizes.add(1024 * 1024 + 1);
            sizes.add(block / 2 + 1);
            sizes.add(block - 1);
            sizes.add(block);
            sizes.add(block + 1);
            if (c.layout.mapped) {
                sizes.add(c.config().regionSize + 1);
            }
            long seed = 1;
            for (int size : sizes) {
                int count = (int) Math.min(200_000, 5L * block / 4 / size + 2);
                List<ByteBuf> live = new ArrayList<ByteBuf>();
                try {
                    for (int i = 0; i < count; i++) {
                        ByteBuf buf = allocate(alloc, c.direct, size);
                        live.add(buf);
                        assertEquals(size, buf.capacity());
                        stamp(buf, seed + i);
                    }
                    assertDisjoint(live);
                    for (int i = 0; i < count; i++) {
                        verify(live.get(i), seed + i);
                    }
                } finally {
                    releaseAll(live);
                }
                seed += count;
            }
        });
    }

    /**
     * A seeded random sequence of allocations, growths and releases of every kind, with time passing while the
     * thread allocates between its phases: every live buffer keeps its bytes, and nothing is left once the allocator
     * is closed.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void randomWorkloadKeepsEveryBuffersBytes(Case c) throws Throwable {
        AdaptiveByteBufAllocator allocator = run(c, alloc -> {
            Random random = new Random(20261116L);
            int block = c.config().segmentSize;
            int[] sizes = {1, 32, 100, 500, 1024, 5000, 16_384, 20_000, 70_000, 135_168, 300_000, 700_000,
                    block / 2 + 1, block, block + 1};
            List<ByteBuf> live = new ArrayList<ByteBuf>();
            List<Long> seeds = new ArrayList<Long>();
            List<Integer> stamped = new ArrayList<Integer>();
            long liveBytes = 0;
            long seed = 1;
            try {
                for (int phase = 0; phase < 6; phase++) {
                    for (int step = 0; step < 1500; step++) {
                        int op = random.nextInt(10);
                        if (op < 5 && liveBytes < 48L * 1024 * 1024) {
                            // Mostly small: the large sizes are 1 in 8.
                            int size = random.nextInt(8) == 0 ? sizes[random.nextInt(sizes.length)]
                                    : sizes[random.nextInt(8)];
                            ByteBuf buf = allocate(alloc, c.direct, size);
                            stamp(buf, seed);
                            live.add(buf);
                            seeds.add(seed++);
                            stamped.add(size);
                            liveBytes += size;
                        } else if (op < 8 && !live.isEmpty()) {
                            int i = random.nextInt(live.size());
                            verify(live.get(i), seeds.get(i), stamped.get(i));
                            liveBytes -= stamped.get(i);
                            live.remove(i).release();
                            seeds.remove(i);
                            stamped.remove(i);
                        } else if (!live.isEmpty() && liveBytes < 32L * 1024 * 1024) {
                            int i = random.nextInt(live.size());
                            ByteBuf buf = live.get(i);
                            int old = stamped.get(i);
                            if (old >= 1_500_000) {
                                continue;
                            }
                            int grown = (int) Math.min(1_500_000L, old + 1 + random.nextInt(Math.max(1, old)));
                            buf.capacity(grown);
                            verify(buf, seeds.get(i), old);
                            stamp(buf, seeds.get(i), old, grown);
                            stamped.set(i, grown);
                            liveBytes += grown - old;
                        }
                    }
                    for (int i = 0; i < live.size(); i++) {
                        verify(live.get(i), seeds.get(i), stamped.get(i));
                    }
                    assertDisjoint(live);
                    useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.direct, block));
                    for (int i = 0; i < live.size(); i++) {
                        verify(live.get(i), seeds.get(i), stamped.get(i));
                    }
                }
            } finally {
                releaseAll(live);
            }
            useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.direct, block));
        });
        allocator.close();
        assertEquals(0, allocator.usedDirectMemory());
        assertEquals(0, allocator.usedHeapMemory());
    }

    /** Where {@code buf}'s first byte is: an address, or an array and an offset in it. */
    private static Object where(ByteBuf buf) {
        return buf.hasArray() ? Arrays.asList(buf.array(), buf.arrayOffset()) : (Object) buf.memoryAddress();
    }

    /**
     * A buffer grows from a size class to a class, to a span, to a span and to a block, by writing into the room its
     * slot has, which moves nothing, and by asking for a capacity beyond it: the bytes are kept.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void growthKeepsTheBytes(Case c) throws Throwable {
        run(c, alloc -> {
            int block = c.config().segmentSize;
            ByteBuf buf = allocate(alloc, c.direct, 100);
            try {
                stamp(buf, 7);
                for (int target : new int[] {3000, 200_000, 600_000, block}) {
                    // Into the room of the slot: the same place.
                    buf.writerIndex(buf.capacity());
                    int before = buf.capacity();
                    int room = buf.maxFastWritableBytes();
                    Object place = where(buf);
                    if (room > 0) {
                        buf.ensureWritable(room);
                        assertEquals(before + room, buf.capacity());
                        assertEquals(place, where(buf), "growing into the slot moves nothing");
                        stamp(buf, 7, before, buf.capacity());
                        verify(buf, 7);
                    }
                    // Past it: it moves, with its bytes.
                    buf.writerIndex(buf.capacity());
                    before = buf.capacity();
                    assertEquals(0, buf.maxFastWritableBytes());
                    buf.ensureWritable(1);
                    verify(buf, 7, before);
                    stamp(buf, 7, before, buf.capacity());
                    if (target > buf.capacity()) {
                        before = buf.capacity();
                        buf.capacity(target);
                        assertEquals(target, buf.capacity());
                        verify(buf, 7, before);
                        stamp(buf, 7, before, target);
                    }
                    verify(buf, 7);
                }
            } finally {
                buf.release();
            }
        });
    }

    /**
     * A buffer grows from a size class to above a block: its bytes are kept, and the slot it left is free again: as
     * many 1 KiB buffers as a chunk has slots then take no memory, which they do if one slot is still taken.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void growthAboveABlockKeepsTheBytes(Case c) throws Throwable {
        SizeClassTable table = new SizeClassTable(c.config());
        int slots = table.slots[SizeClassTable.sizeClassIndex(1024)];
        AdaptiveByteBufAllocator alloc = run(c, a -> {
            int above = 2 * c.config().segmentSize + 1;
            ByteBuf buf = allocate(a, c.direct, 1024);
            try {
                stamp(buf, 11);
                buf.writerIndex(buf.capacity());
                buf.capacity(above);
                assertEquals(above, buf.capacity());
                verify(buf, 11, 1024);
                stamp(buf, 11, 1024, above);
                verify(buf, 11);
            } finally {
                buf.release();
            }
            long used = a.usedDirectMemory() + a.usedHeapMemory();
            List<ByteBuf> again = new ArrayList<ByteBuf>();
            try {
                for (int i = 0; i < slots; i++) {
                    again.add(allocate(a, c.direct, 1024));
                }
                assertEquals(used, a.usedDirectMemory() + a.usedHeapMemory(),
                        "the slot the grown buffer left was not free again: a chunk of its size class was taken");
            } finally {
                releaseAll(again);
            }
        });
        alloc.close();
        assertEquals(0, alloc.usedDirectMemory());
        assertEquals(0, alloc.usedHeapMemory());
    }

    /**
     * A buffer of one thread's heap grows on another thread, to another size class and to a span, and is released on
     * a third that has no heap: its bytes are kept, and a round of the first thread's kind afterwards takes no more
     * memory than the one before.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("casesOfDefaultAndMappedBlocks")
    void growthOnAnotherThreadKeepsTheBytes(Case c) throws Throwable {
        if (c.threadLocal) {
            assumeThreadLocalHeaps();
        }
        if (c.layout.mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        }
        final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.layout.geometry,
                c.layout.mapped ? mmapWhereLinked() : null);
        final int block = c.config().segmentSize;
        final long[] used = new long[2];
        final List<ByteBuf> round = new ArrayList<ByteBuf>();
        // The time that passes costs what its own allocation holds, before both rounds.
        TestThreads.runOnThread(false,
                () -> useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.direct, block)));
        TestThreads.runOnThread(c.threadLocal, () -> {
            for (int i = 0; i < 100; i++) {
                round.add(allocate(alloc, c.direct, 1024));
            }
            used[0] = c.direct ? alloc.usedDirectMemory() : alloc.usedHeapMemory();
        });
        TestThreads.runOnThread(false, () -> releaseAll(round));
        TestThreads.runOnThread(false,
                () -> useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.direct, block)));
        final ByteBuf[] grown = new ByteBuf[1];
        TestThreads.runOnThread(c.threadLocal, () -> {
            grown[0] = allocate(alloc, c.direct, 1024);
            stamp(grown[0], 13);
        });
        TestThreads.runOnThread(c.threadLocal, () -> {
            ByteBuf buf = grown[0];
            int before = buf.capacity();
            buf.writerIndex(before);
            for (int target : new int[] {20_000, block / 2, block}) {
                before = buf.capacity();
                buf.capacity(target);
                verify(buf, 13, before);
                stamp(buf, 13, before, buf.capacity());
                buf.writerIndex(buf.capacity());
            }
            verify(buf, 13);
        });
        TestThreads.runOnThread(false, () -> grown[0].release());
        TestThreads.runOnThread(false,
                () -> useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.direct, block)));
        round.clear();
        TestThreads.runOnThread(c.threadLocal, () -> {
            for (int i = 0; i < 100; i++) {
                round.add(allocate(alloc, c.direct, 1024));
            }
            used[1] = c.direct ? alloc.usedDirectMemory() : alloc.usedHeapMemory();
        });
        TestThreads.runOnThread(false, () -> releaseAll(round));
        assertEquals(used[0], used[1], "the round after the growth took memory the first one did not");
    }

    /**
     * Memory a user keeps writing to after the release, through the array it kept, is not memory the allocator
     * needs: the buffers handed out next are disjoint and keep their bytes.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("heapCases")
    void writesThroughAKeptArrayAfterReleaseChangeNothingHandedOut(Case c) throws Throwable {
        run(c, alloc -> {
            for (int size : new int[] {100, 1024, 5000, 70_000, 200_000}) {
                int count = (int) Math.min(2000, 2L * c.config().segmentSize / size + 2);
                List<ByteBuf> first = new ArrayList<ByteBuf>();
                byte[][] arrays = new byte[count][];
                int[] offsets = new int[count];
                int[] lengths = new int[count];
                for (int i = 0; i < count; i++) {
                    ByteBuf buf = alloc.heapBuffer(size);
                    first.add(buf);
                    arrays[i] = buf.array();
                    offsets[i] = buf.arrayOffset();
                    lengths[i] = BufferChecks.slotLength(buf);
                }
                releaseAll(first);
                for (int i = 0; i < count; i++) {
                    Arrays.fill(arrays[i], offsets[i], offsets[i] + lengths[i], (byte) 0xA5);
                }
                List<ByteBuf> second = new ArrayList<ByteBuf>();
                try {
                    for (int i = 0; i < count; i++) {
                        ByteBuf buf = alloc.heapBuffer(size);
                        second.add(buf);
                        stamp(buf, i + 1);
                    }
                    assertDisjoint(second);
                    for (int i = 0; i < count; i++) {
                        verify(second.get(i), i + 1);
                    }
                } finally {
                    releaseAll(second);
                }
            }
        });
    }

    static Stream<Case> heapCases() {
        return cases().filter(c -> !c.direct);
    }

    /**
     * Size classes give their chunks up when they go idle, and the blocks are used again by other classes: in rounds
     * of another size each, with time passing while the thread allocates, the buffers kept from the earlier rounds keep
     * their bytes and the new ones do not overlap them.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("casesOfDefaultAndMappedBlocks")
    void buffersOfChunksGivenUpAndRemadeKeepTheirBytes(Case c) throws Throwable {
        run(c, alloc -> {
            int block = c.config().segmentSize;
            List<ByteBuf> kept = new ArrayList<ByteBuf>();
            List<Long> seeds = new ArrayList<Long>();
            long seed = 1;
            try {
                for (int size : new int[] {1024, 4096, 24_576, 20_000, 300_000, 1024, 70_000}) {
                    int count = (int) Math.min(5000, 3L * block / 2 / size + 2);
                    List<ByteBuf> batch = new ArrayList<ByteBuf>();
                    for (int i = 0; i < count; i++) {
                        ByteBuf buf = allocate(alloc, c.direct, size);
                        stamp(buf, seed + i);
                        batch.add(buf);
                    }
                    List<ByteBuf> live = new ArrayList<ByteBuf>(kept);
                    live.addAll(batch);
                    assertDisjoint(live);
                    for (int i = 0; i < count; i++) {
                        if (i % 2 == 0) {
                            kept.add(batch.get(i));
                            seeds.add(seed + i);
                        } else {
                            verify(batch.get(i), seed + i);
                            batch.get(i).release();
                        }
                    }
                    seed += count;
                    useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.direct, block));
                    for (int i = 0; i < kept.size(); i++) {
                        verify(kept.get(i), seeds.get(i));
                    }
                }
            } finally {
                releaseAll(kept);
            }
        });
    }
}
