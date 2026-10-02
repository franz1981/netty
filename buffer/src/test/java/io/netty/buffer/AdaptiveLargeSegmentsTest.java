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
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Buffers above the size classes in an allocator with a page store: up to half a block each is a span of slices (of
 * the regions' shared slices, or of its heap's blocks without regions); above, with regions, a whole block or a run of
 * blocks. Nothing comes from the chunk allocator but buffers larger than what the store hands out. Three modes:
 * direct blocks allocated one by one, direct regions, and heap blocks (one {@code byte[]} each, no regions).
 */
@Isolated("Reads PlatformDependent's direct memory counter, which concurrent tests move")
final class AdaptiveLargeSegmentsTest {
    /** Eight spans of 512 KiB fill one 4 MiB segment. */
    private static final int POOLED = 512 * 1024;
    private static final int PER_CHUNK = SEGMENT_SIZE / POOLED;
    private static final int SPAN_SLICES = POOLED / PageStoreConfig.SLICE_SIZE_BYTES;
    private static final int PER_BLOCK = SEGMENT_SIZE / PageStoreConfig.SLICE_SIZE_BYTES;

    private CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    @BeforeEach
    void pooledAboveTheSizeClasses() {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
    }

    private AdaptivePoolingAllocator withoutRegions() {
        return newAllocator(segments, SEGMENT_SIZE);
    }

    private AdaptivePoolingAllocator withRegions() {
        return newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
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
        if (allocator.pageStore.regionSource != null) {
            assertAccounted(segments, regions, allocator);
        } else {
            assertAccounted(segments, allocator);
        }
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

    /** Without regions spans fill segments of their own; one wholly free waits in the heap's reserve for reuse. */
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
            assertEquals(3, segments.segmentsLive(), "all three in the reserve");
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
     * Spans of a thread-local heap released by another thread are applied at the owner's next large allocation, which
     * empties their segment and takes it again from the heap's reserve.
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
        assertEquals(0, withRegions ? claimed(allocator) : segments.segmentsLive(),
                "the dead heap gave everything back");
        assertAccountedIn(allocator);
    }

    /**
     * A buffer outliving its thread-local heap keeps its segment; releasing it from another thread gives the segment
     * straight back to the store.
     */
    @ParameterizedTest
    @EnumSource(Mode.class)
    void lastReleaseOnAnotherThreadGivesTheSegmentToTheStore(Mode mode) throws Exception {
        final boolean withRegions = mode == Mode.DIRECT_REGIONS;
        final AdaptivePoolingAllocator allocator = allocator(mode);
        ByteBuf survivor = onThreadLocalHeap(() -> allocator.allocate(POOLED, POOLED));
        assertEquals(withRegions ? SPAN_SLICES : 1, withRegions ? claimed(allocator) : segments.segmentsLive());
        survivor.release();
        assertEquals(0, withRegions ? claimed(allocator) : segments.segmentsLive());
        assertAccountedIn(allocator);
    }

    /**
     * Above half a segment and up to a segment, without regions: a buffer of its own, of its exact size, as before
     * the page store, not a whole segment charged up to twice the buffer's size.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void oneShotsUpToASegmentAreTheirOwnAllocationWithoutRegions(boolean heap) {
        AdaptivePoolingAllocator allocator = withoutRegions(heap);
        int size = 2200000;
        ByteBuf buf = allocate(allocator, size, 1).get(0);
        assertEquals(0, segments.segmentsAllocated());
        assertEquals(1, segments.chunks.size());
        assertEquals(size, allocator.usedMemory());
        assertAccounted(segments, allocator);
        buf.release();
        assertEquals(0, allocator.usedMemory());
        assertAccounted(segments, allocator);
    }

    /** With regions, a one-shot up to a block takes a whole block, given back to the shared slices on release. */
    @Test
    void oneShotsUpToASegmentTakeABlockWithRegions() {
        AdaptivePoolingAllocator allocator = withRegions();
        ByteBuf buf = allocate(allocator, 3 * MIB, 1).get(0);
        assertInRegion(buf);
        assertEquals(PER_BLOCK, claimed(allocator));
        assertEquals(0, segments.segmentsAllocated() + segments.chunks.size());
        buf.release();
        assertEquals(0, claimed(allocator));
        assertAccounted(segments, regions, allocator);
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

    /** Without regions a buffer above a segment is its own allocation, as before the page store. */
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

    /** Every slice the large paths commit is charged once and credited by the purge or the close. */
    @Test
    void largeSlotsAreChargedAndCredited() {
        assumeTrue(PlatformDependent.usedDirectMemory() >= 0, "the direct memory counter is off");
        // Allocators of earlier tests credit the counter when finalized: let that happen before the base is read.
        for (int i = 0; i < 3; i++) {
            System.gc();
            System.runFinalization();
        }
        long base = PlatformDependent.usedDirectMemory();
        long regionCharge = regions.mmap != null ? 0 : REGION_SIZE;
        AdaptivePoolingAllocator allocator = withRegions();
        PageStore store = allocator.pageStore;
        List<ByteBuf> bufs = allocate(allocator, POOLED, PER_CHUNK + 1); // spans over two blocks
        bufs.addAll(allocate(allocator, 3 * MIB, 1)); // above half a block: a block
        bufs.addAll(allocate(allocator, SEGMENT_SIZE + 1, 1)); // two blocks
        int slices = (PER_CHUNK + 1) * SPAN_SLICES + 3 * PER_BLOCK;
        assertEquals(slices, committed(allocator));
        assertEquals(base + regionCharge + (long) slices * PageStoreConfig.SLICE_SIZE_BYTES,
                PlatformDependent.usedDirectMemory());
        release(bufs);
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now + INTERVAL);
        assertEquals(0, committed(allocator), "no heap keeps any");
        assertEquals(base + regionCharge, PlatformDependent.usedDirectMemory());
        store.close();
        assertEquals(base, PlatformDependent.usedDirectMemory());
    }
}
