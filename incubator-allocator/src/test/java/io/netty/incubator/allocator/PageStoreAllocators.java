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
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.netty.incubator.allocator.PageStoreConfig.REGION_ALIGNMENT_BYTES;
import static io.netty.incubator.allocator.PageStoreConfig.SEGMENT_REGION_SIZE_BYTES;
import static io.netty.incubator.allocator.PageStoreConfig.SEGMENT_SIZE_BYTES;
import static io.netty.incubator.allocator.PageStoreConfig.SLICE_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The allocators the page store tests build: the public type, with the two configurations of a {@link Geometry}, a
 * purge delay of one hour, a {@link MockTicker} as the clock, and the source of the mmap regions the test picks. The
 * clock moves only when a test advances it: a purge happens when a test advances it past the delay and allocates.
 */
final class PageStoreAllocators {
    private static final long PURGE_DELAY_NANOS = TimeUnit.HOURS.toNanos(1);
    /** What each step of {@link #useUntilQuiet} moves the clock by. */
    private static final long STEP_NANOS = TimeUnit.HOURS.toNanos(2);
    /** How long the clock must move with nothing given back for {@link #useUntilQuiet} to return: 24 purge delays. */
    private static final long QUIET_NANOS = 24 * PURGE_DELAY_NANOS;
    /** How many buffers of {@link #POOLED_BUFFER_SIZE} one step of {@link #pooledBuffers} takes and releases. */
    private static final int POOLED_BUFFERS_PER_STEP = 256;
    private static final int POOLED_BUFFER_SIZE = 100_000;

    /** The block and region sizes of the direct allocator and the block size of the heap one. */
    enum Geometry {
        /** The sizes the system properties give. */
        DEFAULTS(SEGMENT_SIZE_BYTES, SEGMENT_REGION_SIZE_BYTES, PageStoreConfig.HEAP_SEGMENT_SIZE_BYTES),
        /** Blocks of 2 MiB, the least a block can be, and the heap blocks that follow from them. */
        TWO_MIB_BLOCKS(2 * 1024 * 1024, PageStoreConfig.defaultRegionSize(2 * 1024 * 1024),
                PageStoreConfig.clampHeapSegmentSize(2 * 1024 * 1024 - SLICE_SIZE_BYTES)),
        /** Heap blocks of 7 slices: an odd count, with a 3-slice size class not dividing it. */
        SEVEN_SLICE_HEAP_BLOCKS(SEGMENT_SIZE_BYTES, SEGMENT_REGION_SIZE_BYTES, 7 * SLICE_SIZE_BYTES),
        /** Regions of 2 blocks, the least a mapped region can be: a second region is soon needed. */
        SMALL_REGIONS(SEGMENT_SIZE_BYTES, 2 * SEGMENT_SIZE_BYTES, PageStoreConfig.HEAP_SEGMENT_SIZE_BYTES);

        final PageStoreConfig direct;
        final PageStoreConfig heap;

        Geometry(int directBlock, int directRegion, int heapBlock) {
            direct = new PageStoreConfig(directBlock, SLICE_SIZE_BYTES, PURGE_DELAY_NANOS, directRegion,
                    REGION_ALIGNMENT_BYTES, directBlock);
            heap = new PageStoreConfig(heapBlock, SLICE_SIZE_BYTES, PURGE_DELAY_NANOS, 0, 0, heapBlock);
        }
    }

    /** The memory a test allocates from: direct with {@code malloc}'d regions, direct with mapped ones, or heap. */
    enum Memory {
        DIRECT_MALLOC(true, false),
        DIRECT_MAPPED(true, true),
        HEAP(false, false);

        final boolean direct;
        final boolean mapped;

        Memory(boolean direct, boolean mapped) {
            this.direct = direct;
            this.mapped = mapped;
        }

        PageStoreConfig config() {
            return direct ? Geometry.DEFAULTS.direct : Geometry.DEFAULTS.heap;
        }
    }

    private PageStoreAllocators() {
    }

    /**
     * An allocator that serves the threads without an event loop from heaps of their own too, closed with the test.
     *
     * @param mmap where the direct regions come from, or {@code null} for one-block regions (see
     *             {@link AdaptiveByteBufAllocator})
     */
    static AdaptiveByteBufAllocator newAllocator(AllocatorCloser closer, Ticker ticker, Geometry geometry,
            Function<? super AdaptiveByteBufAllocator, ? extends MmapRegionSource> mmap) {
        return closer.add(new AdaptiveByteBufAllocator(true, true, geometry.direct, geometry.heap, mmap, ticker));
    }

    /** The mapped regions where the JVM links mmap(2) and the property asks for them, else one-block regions. */
    static Function<AdaptiveByteBufAllocator, MmapRegionSource> mmapWhereLinked() {
        if (!PageStoreConfig.MMAP || !MmapRegionSource.isAvailable()) {
            return null;
        }
        return MmapRegionSource::new;
    }

    /**
     * An allocator for {@code memory} on the default geometry, closed with the test; skipped where the memory is mapped
     * and the execution does not link mmap(2).
     */
    static AdaptiveByteBufAllocator newAllocator(AllocatorCloser closer, Ticker ticker, Memory memory) {
        if (memory.mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
            return newAllocator(closer, ticker, Geometry.DEFAULTS, mmapWhereLinked());
        }
        return newAllocator(closer, ticker, Geometry.DEFAULTS, null);
    }

    /**
     * Whether a direct {@link AdaptiveByteBufAllocator} cuts its blocks from {@code mmap} regions, and so counts their
     * slices as they are committed rather than a region whole: it does where
     * {@code -Dio.netty.allocator.pagestore.mmap} is set, the defaults have regions and they can be mapped (see its
     * constructor).
     */
    static boolean directRegionsAreMapped() {
        return PageStoreConfig.MMAP && PageStoreConfig.directDefaults().regionSize > 0
                && MmapRegionSource.isAvailable();
    }

    /** The bytes of a chunk of the size class of {@code size} under {@code config}: its page kind's slices. */
    static int chunkSize(int size, PageStoreConfig config) {
        int index = SizeClassTable.sizeClassIndex(size);
        return new SizeClassTable(config).chunkSlices[index] * config.sliceSize;
    }

    /**
     * What it takes for the allocator to give back what is free, whenever it does. The allocator does its housekeeping
     * from its allocation paths only, so time passes for it when the clock moves <em>and</em> a thread allocates: each
     * step moves the clock 2 hours and runs {@code use} on the calling thread, until a whole day of that clock went by
     * with nothing given back. What it uses is the only sign a test has that a step did something, and a step may do
     * work that shows only steps later, so a test cannot see that the allocator is done: it waits far longer than it
     * may take. A day is 24 times the purge delay these tests configure. How many steps that is, and what each one
     * gives back, is the allocator's policy; the cap is only against a loop that does not end.
     *
     * @param ticker the clock the allocator was built with
     * @param use what one step allocates, on the thread whose heap holds the memory to give back: {@link #aWholeBlock},
     * {@link #aboveABlock} or {@link #pooledBuffers}
     */
    static void useUntilQuiet(AdaptiveByteBufAllocator alloc, MockTicker ticker, Runnable use) {
        long used = alloc.usedDirectMemory() + alloc.usedHeapMemory();
        long quietNanos = 0;
        for (int steps = 0; steps < 1000; steps++) {
            ticker.advance(STEP_NANOS, TimeUnit.NANOSECONDS);
            use.run();
            long after = alloc.usedDirectMemory() + alloc.usedHeapMemory();
            if (after < used) {
                quietNanos = 0;
            } else {
                quietNanos += STEP_NANOS;
                if (quietNanos >= QUIET_NANOS) {
                    return;
                }
            }
            used = after;
        }
        fail("the allocator still gave memory back after 1000 steps");
    }

    /**
     * One buffer of {@code block} bytes, released: reaches the decay of the calling thread's heap and the store's
     * purge, and leaves one free block behind.
     */
    static Runnable aWholeBlock(AdaptiveByteBufAllocator alloc, boolean direct, int block) {
        return () -> (direct ? alloc.directBuffer(block) : alloc.heapBuffer(block)).release();
    }

    /**
     * One buffer of {@code block + 1} bytes, released: reaches the store's purge only, and with one-block regions
     * leaves nothing behind.
     */
    static Runnable aboveABlock(AdaptiveByteBufAllocator alloc, boolean direct, int block) {
        return aWholeBlock(alloc, direct, block + 1);
    }

    /**
     * Buffers of one size class, each released before the next: reach the store's purge and give nothing back to it.
     */
    static Runnable pooledBuffers(AdaptiveByteBufAllocator alloc, boolean direct) {
        return () -> {
            for (int i = 0; i < POOLED_BUFFERS_PER_STEP; i++) {
                (direct ? alloc.directBuffer(POOLED_BUFFER_SIZE) : alloc.heapBuffer(POOLED_BUFFER_SIZE)).release();
            }
        };
    }

    /** {@code count} direct buffers of {@code block} bytes: a whole block each. */
    static List<ByteBuf> wholeBlocks(AdaptiveByteBufAllocator alloc, int block, int count) {
        List<ByteBuf> buffers = new ArrayList<ByteBuf>();
        for (int i = 0; i < count; i++) {
            buffers.add(alloc.directBuffer(block));
        }
        return buffers;
    }

    /** The tests that need the heap of the calling thread: none in low-memory mode unless the property keeps them. */
    static void assumeThreadLocalHeaps() {
        assumeTrue(AdaptivePoolingAllocator.THREAD_LOCAL_HEAPS, "no thread-local heaps in this mode");
    }
}
