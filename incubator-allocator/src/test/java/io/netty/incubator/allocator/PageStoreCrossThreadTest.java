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
import io.netty.incubator.allocator.PageStoreAllocators.Memory;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.netty.incubator.allocator.BufferChecks.assertDisjoint;
import static io.netty.incubator.allocator.BufferChecks.stamp;
import static io.netty.incubator.allocator.BufferChecks.verify;
import static io.netty.incubator.allocator.PageStoreAllocators.assumeThreadLocalHeaps;
import static io.netty.incubator.allocator.PageStoreAllocators.aWholeBlock;
import static io.netty.incubator.allocator.PageStoreAllocators.aboveABlock;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.PageStoreAllocators.useUntilQuiet;
import static io.netty.incubator.allocator.TestThreads.runOnThread;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Buffers released by a thread other than the one that allocated them, and heaps that die with buffers out. A release
 * by a thread that does not own a thread-local heap always takes the remote path, and on a stripe it takes the lock,
 * so each test runs its phases one after the other, separated by joins: the same paths run on any number of cores, in
 * the same order every time.
 */
class PageStoreCrossThreadTest {

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    /** What a test allocates and from where. */
    static final class Case {
        final Memory memory;
        final boolean threadLocal;
        final int kind;

        Case(Memory memory, boolean threadLocal, int kind) {
            this.memory = memory;
            this.threadLocal = threadLocal;
            this.kind = kind;
        }

        @Override
        public String toString() {
            return memory + ", " + (threadLocal ? "thread-local heap" : "stripe")
                    + (kind < 0 ? "" : ", " + KINDS[kind]);
        }
    }

    private static final String[] KINDS = {"size class", "span", "block run"};

    static Stream<Case> kinds() {
        List<Case> cases = new ArrayList<Case>();
        for (Memory memory : Memory.values()) {
            for (boolean threadLocal : new boolean[] {true, false}) {
                for (int kind = 0; kind < KINDS.length; kind++) {
                    cases.add(new Case(memory, threadLocal, kind));
                }
            }
        }
        return cases.stream();
    }

    static Stream<Case> kindsOnThreadLocalHeaps() {
        return kinds().filter(c -> c.threadLocal);
    }

    static Stream<Case> sizeClassesOnThreadLocalHeaps() {
        return kindsOnThreadLocalHeaps().filter(c -> c.kind == 0);
    }

    static Stream<Case> heaps() {
        List<Case> cases = new ArrayList<Case>();
        for (Memory memory : Memory.values()) {
            for (boolean threadLocal : new boolean[] {true, false}) {
                cases.add(new Case(memory, threadLocal, -1));
            }
        }
        return cases.stream();
    }

    private static ByteBuf allocate(AdaptiveByteBufAllocator alloc, Memory memory, int size) {
        return memory.direct ? alloc.directBuffer(size) : alloc.heapBuffer(size);
    }

    private static long used(AdaptiveByteBufAllocator alloc, Memory memory) {
        return memory.direct ? alloc.usedDirectMemory() : alloc.usedHeapMemory();
    }

    /** The buffers of a test that are live, each with its pattern. */
    private static final class Live {
        final List<ByteBuf> buffers = new ArrayList<ByteBuf>();
        final List<Long> seeds = new ArrayList<Long>();
        private long next = 1;

        ByteBuf add(ByteBuf buf) {
            long seed = next++;
            stamp(buf, seed);
            buffers.add(buf);
            seeds.add(seed);
            return buf;
        }

        void verifyAll() {
            assertDisjoint(buffers);
            for (int i = 0; i < buffers.size(); i++) {
                verify(buffers.get(i), seeds.get(i));
            }
        }

        /** Verifies and releases the buffers at {@code indices}, on the calling thread, and forgets them. */
        void release(List<Integer> indices) {
            for (int i = indices.size() - 1; i >= 0; i--) {
                int index = indices.get(i);
                verify(buffers.get(index), seeds.get(index));
                buffers.remove(index).release();
                seeds.remove(index);
            }
        }

        void releaseAll() {
            List<Integer> all = new ArrayList<Integer>();
            for (int i = 0; i < buffers.size(); i++) {
                all.add(i);
            }
            release(all);
        }
    }

    /** The size of the buffers of a {@code kind}: a class, a span of 4 slices, a buffer of a block and a byte. */
    private static int sizeOf(Case c) {
        PageStoreConfig config = c.memory.config();
        switch (c.kind) {
            case 0:
                return 1024;
            case 1:
                return 3 * config.sliceSize + config.sliceSize / 2;
            default:
                return config.segmentSize + 1;
        }
    }

    /** How many of the buffers of a {@code kind} fill two blocks, and more than a thread's retained chunks. */
    private static int countOf(Case c) {
        PageStoreConfig config = c.memory.config();
        switch (c.kind) {
            case 0: {
                SizeClassTable table = new SizeClassTable(config);
                int index = SizeClassTable.sizeClassIndex(1024);
                return 2 * table.slots[index] * (config.slicesPerSegment() / table.chunkSlices[index]);
            }
            case 1:
                return 2 * (config.slicesPerSegment() / 4);
            default:
                return 4;
        }
    }

    /**
     * The sizing of a scenario, shown on an allocator of its own before the round under test: the fill takes what the
     * kind of buffer takes (two blocks, or four own allocations or runs of two blocks), and one buffer more takes a new
     * chunk, span or run. A round that does not fill its blocks fails here and not silently in the round.
     */
    private void probe(Case c) throws Throwable {
        final AdaptiveByteBufAllocator probe = newAllocator(closer, ticker, c.memory);
        final PageStoreConfig config = c.memory.config();
        final int size = sizeOf(c);
        final int count = countOf(c);
        final long block = config.segmentSize;
        final long fill;
        final long extra;
        switch (c.kind) {
            case 0:
                fill = 2 * block;
                int chunkSlices = new SizeClassTable(config).chunkSlices[SizeClassTable.sizeClassIndex(size)];
                extra = c.memory.mapped ? (long) chunkSlices * config.sliceSize : block;
                break;
            case 1:
                fill = 2 * block;
                extra = c.memory.mapped ? 4L * config.sliceSize : block;
                break;
            default:
                fill = c.memory.mapped ? 4 * 2 * block : 4L * size;
                extra = c.memory.mapped ? 2 * block : size;
                break;
        }
        runOnThread(c.threadLocal, () -> {
            Live live = new Live();
            for (int i = 0; i < count; i++) {
                live.add(allocate(probe, c.memory, size));
            }
            assertEquals(fill, used(probe, c.memory), "the fill takes " + fill);
            live.add(allocate(probe, c.memory, size));
            assertEquals(fill + extra, used(probe, c.memory), "one more takes " + extra);
            live.releaseAll();
        });
    }

    /**
     * The owner fills two blocks with buffers of one kind and stamps them, another thread releases every other one,
     * and the owner allocates as many again: the bytes of the buffers kept are intact, no two buffers share memory,
     * and no memory was taken for the second allocation.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void buffersReleasedByAnotherThreadAreReusedWithoutOverlap(Case c) throws Throwable {
        if (c.threadLocal) {
            assumeThreadLocalHeaps();
        }
        probe(c);
        final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.memory);
        final int size = sizeOf(c);
        final int count = countOf(c);
        runOnThread(c.threadLocal, () -> {
            Live live = new Live();
            for (int i = 0; i < count; i++) {
                live.add(allocate(alloc, c.memory, size));
            }
            live.verifyAll();
            long used = used(alloc, c.memory);
            final List<Integer> everyOther = new ArrayList<Integer>();
            for (int i = 0; i < count; i += 2) {
                everyOther.add(i);
            }
            runOnThread(false, () -> live.release(everyOther));
            if (c.kind != 2 || c.memory.mapped) {
                // A buffer above a block that is its own allocation gives its memory back at once; all others keep it.
                assertEquals(used, used(alloc, c.memory), "the remote releases changed the memory used");
            }
            for (int i = 0; i < everyOther.size(); i++) {
                live.add(allocate(alloc, c.memory, size));
            }
            live.verifyAll();
            assertEquals(used, used(alloc, c.memory), "the second allocation took memory");
            live.releaseAll();
        });
    }

    /**
     * A thread-local heap that ends with buffers out loses nothing: the buffers keep their bytes until they are
     * released, by another thread or by the thread itself after its thread-locals were removed; once time has passed
     * while a thread allocates, the next heap's identical round takes no more memory; the close succeeds, which a slot
     * or span given back twice would not.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("kindsOnThreadLocalHeaps")
    void aHeapThatDiesWithBuffersOutLosesNothing(Case c) throws Throwable {
        assumeThreadLocalHeaps();
        for (boolean afterRemoveAll : new boolean[] {false, true}) {
            probe(c);
            final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.memory);
            final int size = sizeOf(c);
            final int count = countOf(c);
            final Live live = new Live();
            final long[] used = new long[2];
            runOnThread(true, () -> {
                for (int i = 0; i < count; i++) {
                    live.add(allocate(alloc, c.memory, size));
                }
                live.verifyAll();
                used[0] = used(alloc, c.memory);
                if (afterRemoveAll) {
                    FastThreadLocal.removeAll();
                    live.releaseAll();
                }
            });
            live.verifyAll();
            runOnThread(false, live::releaseAll);
            runOnThread(false, () -> useUntilQuiet(alloc, ticker,
                    aboveABlock(alloc, c.memory.direct, c.memory.config().segmentSize)));
            runOnThread(true, () -> {
                Live second = new Live();
                for (int i = 0; i < count; i++) {
                    second.add(allocate(alloc, c.memory, size));
                }
                second.verifyAll();
                used[1] = used(alloc, c.memory);
                second.releaseAll();
            });
            assertEquals(used[0], used[1], "the next heap's round took memory the first one's did not");
            alloc.close();
            assertEquals(0, used(alloc, c.memory), "the close returns everything");
        }
    }

    /**
     * A thread's heap ends with stamped buffers out, time passes while a thread allocates, and a second heap fills
     * the same size and others: both sets keep their bytes and share no memory. The first set is released by another
     * thread, time passes again, and a third heap's identical round takes no more memory than the first one's.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("sizeClassesOnThreadLocalHeaps")
    void aDeadHeapsBuffersSurviveTheTimeAndTheNextHeapsAllocations(Case c) throws Throwable {
        assumeThreadLocalHeaps();
        final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.memory);
        final int size = sizeOf(c);
        final int count = countOf(c);
        final Live first = new Live();
        final long[] used = new long[2];
        runOnThread(true, () -> {
            for (int i = 0; i < count; i++) {
                first.add(allocate(alloc, c.memory, size));
            }
            used[0] = used(alloc, c.memory);
        });
        useUntilQuiet(alloc, ticker, aboveABlock(alloc, c.memory.direct, c.memory.config().segmentSize));
        runOnThread(true, () -> {
            Live second = new Live();
            for (int i = 0; i < count; i++) {
                second.add(allocate(alloc, c.memory, size));
            }
            for (int other : new int[] {4096, 20_000, 70_000, 300_000}) {
                second.add(allocate(alloc, c.memory, other));
            }
            first.verifyAll();
            second.verifyAll();
            List<ByteBuf> both = new ArrayList<ByteBuf>(first.buffers);
            both.addAll(second.buffers);
            assertDisjoint(both);
            second.releaseAll();
        });
        first.verifyAll();
        first.releaseAll();
        useUntilQuiet(alloc, ticker, aboveABlock(alloc, c.memory.direct, c.memory.config().segmentSize));
        runOnThread(true, () -> {
            Live third = new Live();
            for (int i = 0; i < count; i++) {
                third.add(allocate(alloc, c.memory, size));
            }
            third.verifyAll();
            used[1] = used(alloc, c.memory);
            third.releaseAll();
        });
        assertEquals(used[0], used[1], "the third heap's round took memory the first one's did not");
        alloc.close();
        assertEquals(0, used(alloc, c.memory), "the close returns everything");
    }

    /**
     * A size class that went idle gives up its chunks, but never memory a buffer holds: one buffer is held, the rest
     * of its chunk is released by another thread, time passes while the owner allocates (the class is idle, its
     * chunks are given up), and other classes fill blocks. The held buffer keeps its bytes and no new buffer overlaps
     * it.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("heaps")
    void anIdleClassNeverGivesUpMemoryABufferHolds(Case c) throws Throwable {
        if (c.threadLocal) {
            assumeThreadLocalHeaps();
        }
        final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.memory);
        final PageStoreConfig config = c.memory.config();
        SizeClassTable table = new SizeClassTable(config);
        final int slots = table.slots[SizeClassTable.sizeClassIndex(1024)];
        runOnThread(c.threadLocal, () -> {
            Live live = new Live();
            for (int i = 0; i < slots; i++) {
                live.add(allocate(alloc, c.memory, 1024));
            }
            final List<Integer> rest = new ArrayList<Integer>();
            for (int i = 1; i < slots; i++) {
                rest.add(i);
            }
            runOnThread(false, () -> live.release(rest));
            useUntilQuiet(alloc, ticker, aWholeBlock(alloc, c.memory.direct, config.segmentSize));
            live.verifyAll();
            for (int size : new int[] {4096, 20_000, 70_000, 300_000}) {
                for (int i = 0; i < config.segmentSize / size + 2; i++) {
                    live.add(allocate(alloc, c.memory, size));
                }
            }
            live.verifyAll();
            live.releaseAll();
        });
    }

    /**
     * Remote releases of an idle class serve another class: a block is filled exactly with one class, all released by
     * another thread, and another class then takes half of it again: no memory is taken.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("heaps")
    void remoteReleasesOfAnIdleClassServeAnotherClass(Case c) throws Throwable {
        if (c.threadLocal) {
            assumeThreadLocalHeaps();
        }
        final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, c.memory);
        final PageStoreConfig config = c.memory.config();
        final SizeClassTable table = new SizeClassTable(config);
        final int p = SizeClassTable.sizeClassIndex(1024);
        final int q = SizeClassTable.sizeClassIndex(4096);
        assertEquals(1, table.chunkSlices[p]);
        assertEquals(1, table.chunkSlices[q]);
        final int perBlock = config.slicesPerSegment();
        runOnThread(c.threadLocal, () -> {
            Live live = new Live();
            for (int i = 0; i < perBlock * table.slots[p]; i++) {
                live.add(allocate(alloc, c.memory, 1024));
            }
            long used = used(alloc, c.memory);
            assertEquals(config.segmentSize, used, "the first class fills a block exactly");
            final List<Integer> all = new ArrayList<Integer>();
            for (int i = 0; i < live.buffers.size(); i++) {
                all.add(i);
            }
            runOnThread(false, () -> live.release(all));
            // How many chunks the first class keeps is the allocator's policy: the second asks for half of them.
            int chunks = perBlock / 2;
            for (int i = 0; i < chunks * table.slots[q]; i++) {
                live.add(allocate(alloc, c.memory, 4096));
            }
            live.verifyAll();
            assertEquals(used, used(alloc, c.memory), "the second class took memory the first one freed");
            live.releaseAll();
        });
    }
}
