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
import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Buffers above the size classes in an allocator with a page store: up to a whole block each is a span of the shared
 * slices; above, a run of blocks where regions hold more than one. Nothing comes from the chunk allocator but buffers
 * larger than what the store hands out. Three modes: direct regions of one block (as
 * {@code malloc}'d ones), direct regions of many (as {@code mmap}'d ones), and heap regions of one block (one
 * {@code byte[]} each).
 */
final class AdaptiveLargeSegmentsTest {
    /** Eight spans of 512 KiB fill one 4 MiB segment. */
    private static final int POOLED = 512 * 1024;
    private static final int PER_CHUNK = SEGMENT_SIZE / POOLED;
    private static final int SPAN_SLICES = POOLED / PageStoreConfig.SLICE_SIZE_BYTES;
    private static final int PER_BLOCK = SEGMENT_SIZE / PageStoreConfig.SLICE_SIZE_BYTES;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    @BeforeEach
    void pooledAboveTheSizeClasses() {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
    }

    /** Regions of one block, from {@link #segments}. */
    private AdaptivePoolingAllocator withoutRegions() {
        return closer.add(newAllocator(segments, SEGMENT_SIZE));
    }

    private AdaptivePoolingAllocator withRegions() {
        return closer.add(newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT));
    }

    private AdaptivePoolingAllocator withoutRegions(boolean heap) {
        segments = new CountingSegmentSource(heap);
        return withoutRegions();
    }

    enum Mode { DIRECT, DIRECT_REGIONS, HEAP }

    private AdaptivePoolingAllocator allocator(Mode mode) {
        return mode == Mode.DIRECT_REGIONS ? withRegions() : withoutRegions(mode == Mode.HEAP);
    }

    private void assertAccountedIn(AdaptivePoolingAllocator allocator) {
        assertAccounted(segments, regions, allocator);
    }

    private static List<ByteBuf> allocate(AdaptivePoolingAllocator allocator, int size, int count) {
        List<ByteBuf> bufs = new ArrayList<ByteBuf>(count);
        for (int i = 0; i < count; i++) {
            ByteBuf buf = allocator.allocate(size, size);
            buf.setLong(size - 8, size);
            bufs.add(buf);
        }
        return bufs;
    }

    private static void release(List<ByteBuf> bufs) {
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }

    private void assertInRegion(ByteBuf buf) {
        long base = regions.regions.get(0).memoryAddress();
        assertTrue(buf.memoryAddress() >= base && buf.memoryAddress() + buf.capacity() <= base + REGION_SIZE);
    }

    private static int claimed(AdaptivePoolingAllocator allocator) {
        return allocator.pageStore.sliceCounts()[0];
    }

    private static int committed(AdaptivePoolingAllocator allocator) {
        int[] counts = allocator.pageStore.sliceCounts();
        return counts[0] + counts[1];
    }

    /** Runs {@code task} on a thread with its own thread-local heap, which is freed when the thread ends. */
    private static <T> T onThreadLocalHeap(Callable<T> task) throws Exception {
        AtomicReference<T> result = new AtomicReference<T>();
        AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread thread = new FastThreadLocalThread(() -> {
            try {
                result.set(task.call());
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        return result.get();
    }

    /** On regions of one block spans fill blocks, which stay until idle for the purge delay: a round reuses them. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void spansFillSegmentsKeptInTheHeapReserveWithoutRegions(boolean heap) {
        AdaptivePoolingAllocator allocator = withoutRegions(heap);
        for (int round = 0; round < 3; round++) {
            List<ByteBuf> bufs = allocate(allocator, POOLED, 2 * PER_CHUNK + 1);
            assertEquals(3, segments.segmentsAllocated(), "three segments, whatever the round");
            assertEquals(0, segments.chunks.size(), "nothing from the chunk allocator");
            assertAccounted(segments, allocator);
            release(bufs);
            assertEquals(3, segments.segmentsLive(), "all three kept");
            assertAccounted(segments, allocator);
        }
    }

    /** With regions spans are runs of the shared slices, given back at once and claimed again, committed. */
    @Test
    void spansAreSharedSlicesGivenBackAtOnce() {
        AdaptivePoolingAllocator allocator = withRegions();
        int spans = 2 * PER_CHUNK + 1;
        for (int round = 0; round < 3; round++) {
            List<ByteBuf> bufs = allocate(allocator, POOLED, spans);
            for (ByteBuf buf : bufs) {
                assertInRegion(buf);
            }
            assertEquals(spans * SPAN_SLICES, claimed(allocator));
            assertEquals(spans * SPAN_SLICES, committed(allocator), "the same slices, whatever the round");
            assertEquals(0, segments.segmentsAllocated() + segments.chunks.size());
            release(bufs);
            assertEquals(0, claimed(allocator), "nothing kept by a heap");
            assertAccounted(segments, regions, allocator);
        }
    }

    /**
     * Spans of a thread-local heap released by another thread are free at once: the owner's next large allocations
     * take their slices again.
     */
    @ParameterizedTest
    @EnumSource(Mode.class)
    void spansReleasedByAnotherThreadAreReusedByTheOwner(Mode mode) throws Exception {
        final boolean withRegions = mode == Mode.DIRECT_REGIONS;
        final AdaptivePoolingAllocator allocator = allocator(mode);
        final List<ByteBuf> first = new ArrayList<ByteBuf>();
        int taken = onThreadLocalHeap(() -> {
            first.addAll(allocate(allocator, POOLED, PER_CHUNK));
            List<ByteBuf> second = allocate(allocator, POOLED, 1);
            Thread releaser = new Thread(() -> release(first));
            releaser.start();
            releaser.join();
            // Fill the second segment and one more: the claims apply the notes, which empty the first segment.
            second.addAll(allocate(allocator, POOLED, PER_CHUNK));
            int held = withRegions ? committed(allocator) : segments.segmentsAllocated();
            release(second);
            return held;
        });
        if (withRegions) {
            // The first spans' slices were free at once: taken again, within the two blocks claimed in.
            assertTrue(taken <= 2 * PER_BLOCK, taken + " slices committed");
        } else {
            assertEquals(2, taken, "the first segment was taken again");
        }
        assertEquals(0, claimed(allocator), "the dead heap gave everything back");
        assertAccountedIn(allocator);
    }

    /**
     * A buffer outliving its thread-local heap keeps its slices; releasing it from another thread gives them straight
     * back to the store.
     */
    @ParameterizedTest
    @EnumSource(Mode.class)
    void lastReleaseOnAnotherThreadGivesTheSegmentToTheStore(Mode mode) throws Exception {
        final AdaptivePoolingAllocator allocator = allocator(mode);
        ByteBuf survivor = onThreadLocalHeap(() -> allocator.allocate(POOLED, POOLED));
        assertEquals(SPAN_SLICES, claimed(allocator));
        survivor.release();
        assertEquals(0, claimed(allocator));
        assertAccountedIn(allocator);
    }

    /**
     * Above half a block and up to a whole one: a span of whole slices as the smaller ones, so a block holds it next
     * to other spans, and a whole block's span takes another block. Released, the slices go back at once; regions of
     * one block go back once idle for the purge delay, the slices of the others are purged.
     */
    @ParameterizedTest
    @EnumSource(Mode.class)
    void spansUpToAWholeBlock(Mode mode) {
        AdaptivePoolingAllocator allocator = allocator(mode);
        int large = 2200000;
        int largeSlices = (large + PageStoreConfig.SLICE_SIZE_BYTES - 1) / PageStoreConfig.SLICE_SIZE_BYTES;
        ByteBuf first = allocate(allocator, large, 1).get(0);
        ByteBuf second = allocate(allocator, 3 * MIB / 2, 1).get(0);
        assertSame(blockOf(first), blockOf(second), "one block holds both");
        assertEquals(largeSlices + 24, claimed(allocator));
        ByteBuf whole = allocate(allocator, SEGMENT_SIZE, 1).get(0);
        assertNotSame(blockOf(first), blockOf(whole));
        assertEquals(largeSlices + 24 + PER_BLOCK, claimed(allocator));
        assertEquals(0, segments.chunks.size(), "nothing from the chunk allocator");
        assertEquals(mode == Mode.DIRECT_REGIONS ? 0 : 2, segments.segmentsAllocated(), "blocks of their own");
        assertAccountedIn(allocator);
        release(Arrays.asList(first, second, whole));
        assertEquals(0, claimed(allocator));
        allocator.pageStore.purgeIfDue(System.nanoTime() + 2 * INTERVAL);
        assertEquals(0, committed(allocator), "purged");
        assertEquals(0, allocator.usedMemory());
        assertAccountedIn(allocator);
    }

    private static Segment blockOf(ByteBuf buf) {
        AdaptivePoolingAllocator.AdaptiveByteBuf adaptive = (AdaptivePoolingAllocator.AdaptiveByteBuf)
                (buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf ? buf : buf.unwrap());
        return ((AdaptivePoolingAllocator.SharedSpanChunk) adaptive.chunk).block;
    }

    /**
     * Above a block, with regions: a run of contiguous blocks, given back on release and purged by the store like any
     * free slice. Above a region: a buffer of its own from the chunk allocator.
     */
    @Test
    void buffersAboveASegmentTakeRunsOfBlocks() {
        AdaptivePoolingAllocator allocator = withRegions();
        PageStore store = allocator.pageStore;
        ByteBuf run = allocate(allocator, 2 * SEGMENT_SIZE + 1, 1).get(0);
        assertInRegion(run);
        assertEquals(3 * PER_BLOCK, claimed(allocator));
        assertEquals(3 * PER_BLOCK, committed(allocator));
        ByteBuf huge = allocate(allocator, REGION_SIZE + 1, 1).get(0);
        assertEquals(1, segments.chunks.size(), "larger than a region: its own allocation");
        assertAccounted(segments, regions, allocator);
        run.release();
        huge.release();
        assertEquals(0, claimed(allocator));
        assertAccounted(segments, regions, allocator);
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now + INTERVAL);
        assertEquals(3 * PER_BLOCK, store.slicesPurged, "free a whole interval: purged");
        assertEquals(0, committed(allocator));
        assertAccounted(segments, regions, allocator);
    }

    /** On regions of one block a buffer above a segment is its own allocation, as before the page store. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void buffersAboveASegmentAreTheirOwnAllocationWithoutRegions(boolean heap) {
        AdaptivePoolingAllocator allocator = withoutRegions(heap);
        ByteBuf buf = allocate(allocator, SEGMENT_SIZE + 1, 1).get(0);
        assertEquals(0, segments.segmentsAllocated());
        assertEquals(1, segments.chunks.size());
        assertAccounted(segments, allocator);
        buf.release();
        assertAccounted(segments, allocator);
    }

    /** Every slice the large paths commit is counted once, and credited by the purge. */
    @Test
    void largeSlotsAreChargedAndCredited() {
        AdaptivePoolingAllocator allocator = withRegions();
        PageStore store = allocator.pageStore;
        List<ByteBuf> bufs = allocate(allocator, POOLED, PER_CHUNK + 1); // spans over two blocks
        bufs.addAll(allocate(allocator, 3 * MIB, 1)); // a span of 48 slices
        bufs.addAll(allocate(allocator, SEGMENT_SIZE + 1, 1)); // two blocks
        int slices = (PER_CHUNK + 1) * SPAN_SLICES + 3 * MIB / PageStoreConfig.SLICE_SIZE_BYTES + 2 * PER_BLOCK;
        assertEquals(slices, committed(allocator));
        assertEquals((long) slices * PageStoreConfig.SLICE_SIZE_BYTES, allocator.usedMemory());
        release(bufs);
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now + INTERVAL);
        assertEquals(0, committed(allocator), "no heap keeps any");
        assertEquals(0, allocator.usedMemory());
        store.close();
    }
}
