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
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Region memory against Netty's direct memory limit: a slot is charged when taken with no memory behind it, credited
 * when purged or closed, and a take past the limit gives its slot back. A segment allocated on its own is charged by
 * its allocation.
 */
@Isolated("Reads and fills PlatformDependent's direct memory counter, which concurrent tests move")
final class DirectMemoryChargeTest {
    private static final int SLOTS = REGION_SIZE / SEGMENT_SIZE;

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

    private static Segment releaseOwnership(Segment segment) {
        assertTrue(Segment.OWNER.compareAndSet(segment, segment.owner, null));
        return segment;
    }

    @Test
    void committedSlotsAreChargedOnceAndCreditedByThePurgeAndTheClose() {
        long base = used();
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        store.take(heap);
        assertEquals(base + regionCharge + SEGMENT_SIZE, used(), "the region is not charged, its first slot is");
        Segment second = store.take(heap);
        assertEquals(base + regionCharge + 2L * SEGMENT_SIZE, used());
        store.free(releaseOwnership(second));
        assertSame(second, store.take(heap));
        assertEquals(base + regionCharge + 2L * SEGMENT_SIZE, used(), "committed already: not charged again");
        store.free(releaseOwnership(second));
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now += INTERVAL);
        assertEquals(1, store.segmentsPurged);
        assertEquals(base + regionCharge + SEGMENT_SIZE, used(), "the purge credits it");
        assertAccounted(segments, regions, allocator);
        store.close();
        assertEquals(base, used(), "the close credits the rest");
    }

    /**
     * A take that would pass the limit throws, and its slot is free again with no memory behind it; a slot with
     * memory behind it is still taken, and once there is room again the same slot is committed.
     */
    @Test
    void aTakePastTheLimitGivesItsSlotBack() {
        long base = used();
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        Segment first = store.take(heap);
        Region region = first.region;
        long filler = PlatformDependent.maxDirectMemory() - used() - SEGMENT_SIZE / 2;
        charge(filler);
        try {
            assertThrows(OutOfDirectMemoryError.class, () -> store.take(heap));
            assertEquals(SLOTS - 1, region.freeSlotCount(), "the slot went back");
            assertEquals(Region.UNCOMMITTED, region.freedAt[1]);
            assertEquals(1, store.segmentsCommitted);
            assertEquals(SEGMENT_SIZE, allocator.usedMemory());
            store.free(releaseOwnership(first));
            assertSame(first, store.take(heap), "committed: nothing to charge");
        } finally {
            credit(filler);
        }
        Segment second = store.take(heap);
        assertEquals(1, second.slot);
        assertEquals(2, store.segmentsCommitted);
        assertEquals(base + regionCharge + 2L * SEGMENT_SIZE, used());
        assertAccounted(segments, regions, allocator);
        store.close();
        assertEquals(base, used());
    }

    /** The direct allocator's own segment source, used without regions: each segment is charged by its allocation. */
    @Test
    void segmentsOfTheirOwnAreChargedByTheirAllocation() throws Exception {
        AdaptiveByteBufAllocator adaptive = new AdaptiveByteBufAllocator(true, false);
        Field direct = AdaptiveByteBufAllocator.class.getDeclaredField("direct");
        direct.setAccessible(true);
        SegmentSource source = ((AdaptivePoolingAllocator) direct.get(adaptive)).pageStore.segmentSource;
        long base = used();
        AbstractByteBuf segment = source.allocateSegment(SEGMENT_SIZE);
        try {
            assertEquals(base + SEGMENT_SIZE, used());
        } finally {
            segment.release();
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
