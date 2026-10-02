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
import io.netty.util.internal.OutOfDirectMemoryError;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.Field;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Region memory against Netty's direct memory limit: an {@code mmap} region's slice is charged when claimed with no
 * memory behind it, credited when purged or closed, and a claim past the limit gives its run back; a {@code malloc}'d
 * region is charged whole by its allocation. A segment allocated on its own is charged by
 * its allocation.
 */
@Isolated("Reads and fills PlatformDependent's direct memory counter, which concurrent tests move")
final class DirectMemoryChargeTest {
    private static final int SLOTS = REGION_SIZE / SEGMENT_SIZE;
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();
    /** Without mmap, the test regions are direct buffers, charged whole by their allocation. */
    private final long regionCharge = regions.mmap != null ? 0 : REGION_SIZE;

    @BeforeEach
    void counted() {
        assumeTrue(PlatformDependent.usedDirectMemory() >= 0, "the direct memory counter is off");
    }

    private static long used() {
        return PlatformDependent.usedDirectMemory();
    }

    /** Shared slices: a slice is charged by the claim that commits it, credited by its purge or the close. */
    @Test
    void sharedSlicesAreChargedOnceAndCreditedByThePurgeAndTheClose() {
        long base = used();
        AdaptivePoolingAllocator allocator = newSharedAllocator(segments, regions, REGION_SIZE, INTERVAL);
        PageStore store = allocator.pageStore;
        long run = store.claimSlices(9, 0, PageStore.NO_HEAP);
        assertEquals(base + regionCharge + 9L * SLICE, used(), "the region is not charged, the slices are");
        Segment block = store.regions[0].blocks[0];
        block.releaseRun(0, 9, System.nanoTime());
        assertEquals(run, store.claimSlices(9, 0, PageStore.NO_HEAP));
        assertEquals(base + regionCharge + 9L * SLICE, used(), "committed already: not charged again");
        block.releaseRun(3, 6, System.nanoTime());
        long now = System.nanoTime();
        store.purgeIfDue(now + 2 * INTERVAL);
        assertEquals(6, store.slicesPurged);
        assertEquals(base + regionCharge + 3L * SLICE, used(), "the purge credits them");
        assertEquals(3, (int) store.claimSlices(6, 0, PageStore.NO_HEAP));
        assertEquals(base + regionCharge + 9L * SLICE, used(), "purged: charged again");
        assertEquals(15, store.slicesCommitted);
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(base, used(), "the close credits the rest");
    }

    /**
     * Shared slices: a claim that would pass the limit throws, its slices free again with no memory behind them; a
     * claim of committed slices needs no charge.
     */
    @Test
    void aSharedClaimPastTheLimitGivesItsSlicesBack() {
        long base = used();
        AdaptivePoolingAllocator allocator = newSharedAllocator(segments, regions, REGION_SIZE, INTERVAL);
        PageStore store = allocator.pageStore;
        store.claimSlices(9, 0, PageStore.NO_HEAP);
        Segment block = store.regions[0].blocks[0];
        long filler = PlatformDependent.maxDirectMemory() - used() - 4L * SLICE;
        charge(filler);
        try {
            assertThrows(OutOfDirectMemoryError.class, () -> store.claimSlices(9, 0, PageStore.NO_HEAP));
            assertArrayEquals(new int[] {9, 0, REGION_SIZE / SLICE - 9}, store.sliceCounts(), "the run went back");
            assertEquals(Region.UNCOMMITTED, block.freedAt[9]);
            assertEquals(9, store.slicesCommitted);
            block.releaseRun(0, 9, System.nanoTime());
            assertEquals(0, (int) store.claimSlices(9, 0, PageStore.NO_HEAP), "committed: nothing to charge");
        } finally {
            credit(filler);
        }
        assertEquals(9, (int) store.claimSlices(9, 0, PageStore.NO_HEAP));
        assertEquals(base + regionCharge + 18L * SLICE, used());
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(base, used());
    }

    /**
     * {@code malloc}'d regions: charged whole by their allocation, never per slice, and credited when a wholly idle one
     * goes back, or at the close.
     */
    @Test
    void mallocRegionsAreChargedByTheirAllocation() {
        long base = used();
        CountingRegionSource malloc = new CountingRegionSource(true);
        AdaptivePoolingAllocator allocator = newSharedAllocator(segments, malloc, REGION_SIZE, INTERVAL);
        PageStore store = allocator.pageStore;
        long run = store.claimSlices(9, 0, PageStore.NO_HEAP);
        assertEquals(base + REGION_SIZE, used(), "the region, whole");
        store.takeRun(SLOTS); // a second region, whole
        assertEquals(base + 2L * REGION_SIZE, used());
        store.regions[0].blocks[0].releaseRun(0, 9, System.nanoTime());
        assertEquals((int) run, (int) store.claimSlices(9, 0, PageStore.NO_HEAP));
        assertEquals(base + 2L * REGION_SIZE, used(), "nothing per slice");
        store.regions[0].blocks[0].releaseRun(0, 9, System.nanoTime());
        store.purgeIfDue(System.nanoTime() + 4 * INTERVAL);
        assertEquals(1, store.regionsReleased, "the first region went back, the second is out");
        assertEquals(base + REGION_SIZE, used(), "credited by its free");
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(base, used(), "the close credits the rest");
    }

    /**
     * A {@code malloc}'d region past the direct memory limit: the claim that needed it throws, and regions are still
     * made once there is room.
     */
    @Test
    void aMallocRegionPastTheLimitFailsOnlyItsClaim() {
        CountingRegionSource malloc = new CountingRegionSource(true);
        AdaptivePoolingAllocator allocator = newSharedAllocator(segments, malloc, REGION_SIZE, INTERVAL);
        PageStore store = allocator.pageStore;
        long filler = PlatformDependent.maxDirectMemory() - used() - REGION_SIZE / 2;
        charge(filler);
        try {
            assertThrows(OutOfDirectMemoryError.class, () -> store.claimSlices(9, 0, PageStore.NO_HEAP));
            assertSame(malloc, store.regionSource, "regions are still made");
            assertEquals(0, store.regionCount());
        } finally {
            credit(filler);
        }
        assertEquals(0, (int) store.claimSlices(9, 0, PageStore.NO_HEAP));
        assertEquals(1, store.regionCount());
        store.close();
    }

    /** The direct allocator's regions of one block: each is charged by its allocation, credited by its release. */
    @Test
    void mallocBlocksAreChargedByTheirAllocation() throws Exception {
        AdaptiveByteBufAllocator adaptive = new AdaptiveByteBufAllocator(true, false);
        Field direct = AdaptiveByteBufAllocator.class.getDeclaredField("direct");
        direct.setAccessible(true);
        RegionSource source = ((AdaptivePoolingAllocator) direct.get(adaptive)).pageStore.segmentSource
                .mallocRegionSource();
        long base = used();
        AbstractByteBuf block = source.allocateRegion(SEGMENT_SIZE, 0);
        try {
            assertEquals(base + SEGMENT_SIZE, used());
        } finally {
            source.releaseRegion(block);
        }
        assertEquals(base, used());
    }

    private static void charge(long bytes) {
        for (long left = bytes; left > 0; left -= Integer.MAX_VALUE) {
            PlatformDependent.incrementMemoryCounter((int) Math.min(left, Integer.MAX_VALUE));
        }
    }

    private static void credit(long bytes) {
        for (long left = bytes; left > 0; left -= Integer.MAX_VALUE) {
            PlatformDependent.decrementMemoryCounter((int) Math.min(left, Integer.MAX_VALUE));
        }
    }
}
