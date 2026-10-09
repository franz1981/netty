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

import io.netty.buffer.PageStoreTestSupport.CountingRegionSource;
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.assertSharedAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static io.netty.buffer.PageStoreTestSupport.offsetIn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Buffers above the size classes, up to a whole block, as spans of shared slices: sized to whole slices, coloured,
 * released by any thread at once, also once the heap is gone. On regions of one block (direct or heap, as without
 * {@code mmap}) or of many.
 */
final class AdaptiveLargeSpansTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private CountingMemorySource segments = new CountingMemorySource();
    private final CountingRegionSource regions = new CountingRegionSource();

    @BeforeEach
    void pooledAboveTheSizeClasses() {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
    }

    /** The heap allocator's block: one slice less than a direct one, 63 slices. */
    private static final int HEAP_BLOCK = PageStoreConfig.clampHeapSegmentSize(SEGMENT_SIZE - SLICE);

    /** On regions of one heap block: one {@code byte[]} each. */
    private AdaptivePoolingAllocator heapAllocator() {
        segments = new CountingMemorySource(true);
        return closer.add(newAllocator(segments, HEAP_BLOCK));
    }

    private AdaptivePoolingAllocator allocator(boolean withRegions) {
        if (withRegions) {
            assumeTrue(MmapRegionSource.isAvailable(), "regions of many blocks need mmap");
            return closer.add(newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT));
        }
        return closer.add(newAllocator(segments, SEGMENT_SIZE));
    }

    enum Mode { DIRECT, DIRECT_REGIONS, HEAP }

    private AdaptivePoolingAllocator allocator(Mode mode) {
        return mode == Mode.HEAP ? heapAllocator() : allocator(mode == Mode.DIRECT_REGIONS);
    }

    private static AdaptiveByteBuf adaptive(ByteBuf buf) {
        return (AdaptiveByteBuf) (buf instanceof AdaptiveByteBuf ?
                buf : buf.unwrap());
    }

    /**
     * Up to a whole block, 64 slices direct and 63 heap: a span of whole slices, sharing its block's chunk, nothing
     * allocated per buffer. Above, on regions of one block: a buffer of its own.
     */
    @ParameterizedTest
    @EnumSource(Mode.class)
    void spansAreSizedToWholeSlicesUpToABlock(Mode mode) {
        AdaptivePoolingAllocator allocator = allocator(mode);
        int block = allocator.pageStore.config.segmentSize;
        int[] sizes = {140 * 1024, 192 * 1024, 600 * 1024, MIB + 1, 3 * MIB / 2, block / 2, block / 2 + 1,
                block - SLICE - 1, block - SLICE + 1, block - 1, block};
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        Set<Segment> blocksUsed = new HashSet<Segment>();
        for (int size : sizes) {
            ByteBuf buf = allocator.allocate(size, Integer.MAX_VALUE);
            bufs.add(buf);
            blocksUsed.add(segmentOf(buf));
            assertEquals(size, buf.capacity());
            int wholeSlices = (size + SLICE - 1) / SLICE * SLICE;
            int fast = buf.maxFastWritableBytes() + buf.writerIndex();
            assertTrue(fast >= size && fast <= wholeSlices && (wholeSlices - fast) % 64 == 0
                    && wholeSlices - fast <= Math.min(4032, wholeSlices - size),
                    "a span of whole slices, less its colour, for " + size + ": " + fast);
            buf.setByte(size - 1, 42);
        }
        assertEquals(0, segments.chunks.size(), "nothing allocated per buffer");
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        // Every slice is back: claiming the same sizes again needs no block beyond the ones already in use.
        Set<Segment> blocksReused = new HashSet<Segment>();
        List<ByteBuf> again = new ArrayList<ByteBuf>();
        for (int size : sizes) {
            ByteBuf buf = allocator.allocate(size, Integer.MAX_VALUE);
            again.add(buf);
            blocksReused.add(segmentOf(buf));
        }
        assertEquals(blocksUsed, blocksReused, "every slice is back");
        for (ByteBuf buf : again) {
            buf.release();
        }
        if (mode != Mode.DIRECT_REGIONS) {
            ByteBuf own = allocator.allocate(block + 1, block + 1);
            assertEquals(1, segments.chunks.size(), "above a block of a one-block region: its own allocation");
            own.release();
            assertAccounted(segments, allocator);
        } else {
            assertSharedAccounted(segments, allocator);
        }
    }

    private static Segment segmentOf(ByteBuf buf) {
        return ((AdaptivePoolingAllocator.SharedSpanChunk) adaptive(buf).chunk).block;
    }

    private static long colourOf(ByteBuf buf) {
        return offsetIn(buf, segmentOf(buf)) % SLICE;
    }

    /**
     * Large buffers start 64 bytes further into their span each time, round robin over as many colours as the span's
     * unused tail allows, 64 at most: none for an exact fit, four for a 200-byte tail. Released, they give back
     * exactly the slices they claimed.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void largeSpansRotateTheirColourThroughTheirTail(boolean heap) {
        AdaptivePoolingAllocator allocator = heap ? heapAllocator() : allocator(false);
        int[][] cases = {{256 * 1024 - 8192, 64}, {256 * 1024, 1}, {256 * 1024 - 200, 4}};
        for (int[] c : cases) {
            int size = c[0];
            int colours = c[1];
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < 70; i++) {
                bufs.add(allocator.allocate(size, size));
            }
            for (int i = 0; i < bufs.size(); i++) {
                ByteBuf buf = bufs.get(i);
                long colour = colourOf(buf);
                assertTrue(colour % 64 == 0 && colour < colours * 64L, "size " + size + " colour " + colour);
                if (colours > 1 && i > 0) {
                    assertEquals((colourOf(bufs.get(i - 1)) + 64) % (colours * 64L), colour, "round robin");
                }
                long offset = offsetIn(buf, segmentOf(buf));
                long spanEnd = offset / SLICE * SLICE + 4L * SLICE;
                assertTrue(offset + buf.maxFastWritableBytes() + buf.writerIndex() <= spanEnd, "inside its span");
                buf.setByte(size - 1, 42);
            }
            for (ByteBuf buf : bufs) {
                buf.release();
            }
            assertAccounted(segments, allocator);
        }
        // Nothing else uses this allocator: if every claimed slice went back, the purge gives the block back too.
        allocator.pageStore.purgeIfDue(System.nanoTime() + 2 * allocator.pageStore.config.purgeDelayNanos);
        assertEquals(0, segments.segmentsLive(), "every claimed slice went back");
    }

    /** A coloured buffer grows in place up to the end of its span, and away from it beyond, freeing the span. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aColouredBufferGrowsWithinItsSpan(boolean heap) {
        AdaptivePoolingAllocator allocator = heap ? heapAllocator() : allocator(false);
        int size = 256 * 1024 - 8192;
        allocator.allocate(size, Integer.MAX_VALUE).release(); // colour 0 taken: the next is coloured
        ByteBuf buf = allocator.allocate(size, Integer.MAX_VALUE);
        long colour = colourOf(buf);
        assertEquals(64, colour);
        Segment segment = segmentOf(buf);
        long offset = offsetIn(buf, segment);
        int fast = buf.maxFastWritableBytes();
        assertEquals(4 * SLICE - colour, fast);
        buf.writerIndex(0).writeZero(fast);
        assertEquals(offset, offsetIn(buf, segmentOf(buf)), "grown in place");
        assertSame(segment, segmentOf(buf));
        assertEquals(fast, buf.capacity());
        buf.writeByte(1);
        assertTrue(segmentOf(buf) != segment || offsetIn(buf, segment) != offset, "beyond its span: moved");
        buf.release();
        // Nothing else uses this allocator: the old span going back whole is what lets the purge give the block back.
        allocator.pageStore.purgeIfDue(System.nanoTime() + 2 * allocator.pageStore.config.purgeDelayNanos);
        assertEquals(0, segments.segmentsLive(), "the old span went back whole");
        assertAccounted(segments, allocator);
    }

    /**
     * Heaps that die while other threads still hold, and release, their buffers: every span comes back exactly once
     * (a second release of a slice throws), and every slice goes back to the store, none pinned.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void heapsDyingWithReleasesInFlightPinNothing(boolean withRegions) throws Exception {
        final AdaptivePoolingAllocator allocator = allocator(withRegions);
        final int releasers = 3;
        final BlockingQueue<ByteBuf> handOff = new ArrayBlockingQueue<ByteBuf>(64);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final ByteBuf poison = Unpooled.buffer(1);
        final Thread.UncaughtExceptionHandler record = (t, e) -> failure.compareAndSet(null, e);
        List<Thread> threads = new ArrayList<Thread>();
        for (int r = 0; r < releasers; r++) {
            final SplittableRandom random = new SplittableRandom(r);
            Thread releaser = new Thread(() -> {
                try {
                    for (;;) {
                        ByteBuf buf = handOff.take();
                        if (buf == poison) {
                            return;
                        }
                        if (random.nextInt(8) == 0) {
                            Thread.yield();
                        }
                        check(buf);
                        buf.release();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            releaser.setUncaughtExceptionHandler(record);
            releaser.start();
            threads.add(releaser);
        }
        for (int round = 0; round < 40 && failure.get() == null; round++) {
            final SplittableRandom random = new SplittableRandom(1000 + round);
            // A thread-local heap, or a stripe: a thread that cleans up its thread-locals has its own heap.
            Runnable owner = () -> {
                try {
                    List<ByteBuf> mine = new ArrayList<ByteBuf>();
                    for (int i = 0; i < 64; i++) {
                        int size = 136 * 1024 + random.nextInt(SEGMENT_SIZE + 1 - 136 * 1024);
                        ByteBuf buf = allocator.allocate(size, size);
                        stamp(buf);
                        if (random.nextBoolean()) {
                            handOff.put(buf);
                        } else {
                            mine.add(buf);
                        }
                        if (random.nextInt(4) == 0 && !mine.isEmpty()) {
                            ByteBuf last = mine.remove(mine.size() - 1);
                            check(last);
                            last.release();
                        }
                    }
                    // Half of what is left dies with the heap, handed off: released after it is gone.
                    for (ByteBuf buf : mine) {
                        if (random.nextBoolean()) {
                            handOff.put(buf);
                        } else {
                            check(buf);
                            buf.release();
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            };
            Thread thread = round % 2 == 0 ? new FastThreadLocalThread(owner) : new Thread(owner);
            thread.setUncaughtExceptionHandler(record);
            thread.start();
            thread.join();
        }
        for (int r = 0; r < releasers; r++) {
            handOff.put(poison);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        poison.release();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        // No heap holds blocks: every slice is back, so a purge to completion gives every idle block back too.
        PageStoreTestSupport.purgeUntilDone(allocator.pageStore, System.nanoTime());
        assertEquals(0, allocator.usedMemory(), "slices claimed");
        assertSharedAccounted(segments, allocator);
    }

    /**
     * Writes the buffer's own identity at both ends: a span handed out twice while in use would carry another
     * buffer's stamp by the time {@link #check} reads it.
     */
    private static void stamp(ByteBuf buf) {
        long id = System.identityHashCode(buf) ^ (long) buf.memoryAddress() << 20;
        buf.setLong(0, id);
        buf.setLong(buf.capacity() - 8, ~id);
    }

    private static void check(ByteBuf buf) {
        long id = System.identityHashCode(buf) ^ (long) buf.memoryAddress() << 20;
        assertEquals(id, buf.getLong(0), "the start of a live span was overwritten");
        assertEquals(~id, buf.getLong(buf.capacity() - 8), "the end of a live span was overwritten");
    }
}
