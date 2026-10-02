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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertSharedAccounted;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * An {@link AdaptivePoolingAllocator} on shared slices: size-class chunks, large-buffer spans and one-shot buffers
 * are runs of the regions' slices, a run one heap frees is reused by another at once, and buffers released from any
 * thread, after their heap died too, give their slices back.
 */
final class SharedSliceAllocatorTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();
    private AdaptivePoolingAllocator allocator;

    /** On {@code mmap} regions where they can be had, or on {@code malloc}'d ones. */
    private void use(boolean malloc) {
        allocator = newSharedAllocator(segments, malloc ? new CountingRegionSource(true) : regions, REGION_SIZE,
                INTERVAL);
    }

    @BeforeEach
    void pooledAboveTheSizeClasses() {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
    }

    private static AdaptivePoolingAllocator.AdaptiveByteBuf adaptive(ByteBuf buf) {
        return (AdaptivePoolingAllocator.AdaptiveByteBuf) (buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf ?
                buf : buf.unwrap());
    }

    private long regionOffset(ByteBuf buf) {
        return buf.memoryAddress() - allocator.pageStore.region(0).buffer.memoryAddress();
    }

    @ParameterizedTest(name = "malloc: {0}")
    @ValueSource(booleans = {false, true})
    void sizeClassChunksAreRunsOfSharedSlices(boolean malloc) {
        use(malloc);
        ByteBuf buf = allocator.allocate(1024, 1024);
        Segment block = ((AdaptivePoolingAllocator.SizeClassedChunk) adaptive(buf).chunk).segment;
        assertNotNull(block.sharedSpans, "a block of shared slices");
        assertNull(block.owner);
        assertEquals(0, segments.segmentsAllocated());
        assertTrue(regionOffset(buf) >= 0 && regionOffset(buf) < REGION_SIZE);
        assertSharedAccounted(segments, allocator);
        buf.release();
        assertSharedAccounted(segments, allocator);
    }

    /**
     * Two thread-local heaps: the span the first frees is the span the second gets, as it would be from a heap's own
     * segment, without either holding the block.
     */
    @ParameterizedTest(name = "malloc: {0}")
    @ValueSource(booleans = {false, true})
    void aSpanOneHeapFreesIsTheNextHeapsSpan(boolean malloc) throws Exception {
        use(malloc);
        final int size = 3 * MIB / 2;
        final AtomicLong first = new AtomicLong();
        final AtomicLong second = new AtomicLong();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        runOnOwnHeap(() -> {
            ByteBuf buf = allocator.allocate(size, size);
            assertTrue(adaptive(buf).chunk instanceof AdaptivePoolingAllocator.SharedSpanChunk);
            first.set(regionOffset(buf) / SLICE);
            buf.release();
        }, failure);
        runOnOwnHeap(() -> {
            ByteBuf buf = allocator.allocate(size, size);
            second.set(regionOffset(buf) / SLICE);
            buf.release();
        }, failure);
        assertNull(failure.get());
        assertEquals(first.get(), second.get(), "the same slices");
        assertEquals(0, allocator.pageStore.sliceCounts()[0]);
        assertSharedAccounted(segments, allocator);
    }

    /**
     * A thread-local heap's buffers, of the size classes and above, released by another thread after the heap died:
     * every slice comes back.
     */
    @ParameterizedTest(name = "malloc: {0}")
    @ValueSource(booleans = {false, true})
    void buffersReleasedAfterTheirHeapDiedGiveTheirSlicesBack(boolean malloc) throws Exception {
        use(malloc);
        final List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        runOnOwnHeap(() -> {
            for (int i = 0; i < 300; i++) {
                bufs.add(allocator.allocate(1024, 1024));
            }
            for (int size : new int[] {32, 16896, 135168, 200 * 1024, MIB + 1, 2 * MIB}) {
                bufs.add(allocator.allocate(size, size));
            }
        }, failure);
        assertNull(failure.get());
        assertTrue(allocator.pageStore.sliceCounts()[0] > 0);
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertEquals(0, allocator.pageStore.sliceCounts()[0], "every slice is back");
        assertSharedAccounted(segments, allocator);
    }

    /** Above half a segment, one-shot buffers: a whole block up to a segment, contiguous blocks above. */
    @ParameterizedTest(name = "malloc: {0}")
    @ValueSource(booleans = {false, true})
    void oneShotBuffersAreWholeBlocks(boolean malloc) {
        use(malloc);
        ByteBuf block = allocator.allocate(3 * MIB, 3 * MIB);
        ByteBuf blocks = allocator.allocate(SEGMENT_SIZE + 1, SEGMENT_SIZE + 1);
        assertEquals(0, regionOffset(block) % SEGMENT_SIZE);
        assertEquals(0, regionOffset(blocks) % SEGMENT_SIZE);
        assertEquals(3 * (SEGMENT_SIZE / SLICE), allocator.pageStore.sliceCounts()[0]);
        assertEquals(0, segments.segmentsAllocated());
        assertSame(allocator.pageStore.region(0), allocator.pageStore.regions[0]);
        block.release();
        blocks.release();
        assertEquals(0, allocator.pageStore.sliceCounts()[0]);
        assertSharedAccounted(segments, allocator);
    }

    private static void runOnOwnHeap(Runnable task, AtomicReference<Throwable> failure) throws InterruptedException {
        Thread thread = new FastThreadLocalThread(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
        thread.start();
        thread.join();
    }
}
