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
import io.netty.util.internal.OutOfDirectMemoryError;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Region memory against Netty's direct memory limit: an {@code mmap} region's slice is charged when claimed with no
 * memory behind it, credited when purged or closed, and a claim past the limit gives its run back; a {@code malloc}'d
 * region is charged whole by its allocation.
 * <p>
 * Amounts are read from the allocator's used memory, which moves with each charge and credit of its store: the JVM-wide
 * counter also moves whenever the finalizer frees another test's allocator. That only credits it, so the counter is
 * checked one way: charged by no more than the store's amounts, and back to its base or below once they are credited.
 */
@Isolated("Fills PlatformDependent's direct memory counter past its limit, which concurrent allocations would hit")
final class DirectMemoryChargeTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private final CountingMemorySource segments = new CountingMemorySource();
    private final CountingRegionSource regions = new CountingRegionSource();

    @BeforeEach
    void counted() {
        assumeTrue(PlatformDependent.usedDirectMemory() >= 0, "the direct memory counter is off");
    }

    /** The counter rose by {@code bytes} at most since {@code base}. */
    private static void assertChargedAtMost(long base, long bytes, String message) {
        long charged = PlatformDependent.usedDirectMemory() - base;
        assertTrue(charged <= bytes, message + ": charged " + charged + ", expected " + bytes + " at most");
    }

    /** The counter is back to {@code base} or below. */
    private static void assertCredited(long base) {
        long used = PlatformDependent.usedDirectMemory();
        assertTrue(used <= base, "not credited: " + (used - base) + " bytes above the base");
    }

    /** Shared slices: a slice is charged by the claim that commits it, credited by its purge or the close. */
    @Test
    void sharedSlicesAreChargedOnceAndCreditedByThePurgeAndTheClose() {
        assumeTrue(MmapRegionSource.isAvailable(), "mmap(2) regions are not available here");
        long base = PlatformDependent.usedDirectMemory();
        AdaptivePoolingAllocator allocator = closer.add(newSharedAllocator(segments, regions, REGION_SIZE, INTERVAL));
        PageStore store = allocator.pageStore;
        long run = store.claimSlices(9, false);
        assertEquals(9L * SLICE, allocator.usedMemory(), "the region is not charged, the slices are");
        assertChargedAtMost(base, 9L * SLICE, "the region is not charged, the slices are");
        Segment block = store.regions[0].blocks[0];
        block.releaseRun(0, 9, System.nanoTime());
        assertEquals(run, store.claimSlices(9, false));
        assertEquals(9L * SLICE, allocator.usedMemory(), "committed already: not charged again");
        assertChargedAtMost(base, 9L * SLICE, "committed already: not charged again");
        block.releaseRun(3, 6, System.nanoTime());
        long now = System.nanoTime();
        store.purgeIfDue(now + 2 * INTERVAL);
        assertEquals(3L * SLICE, allocator.usedMemory(), "the purge credits them");
        assertChargedAtMost(base, 3L * SLICE, "the purge credits them");
        block.releaseRun(0, 3, System.nanoTime());
        assertEquals(run, store.claimSlices(9, false), "3 slices with memory behind them, 6 purged");
        assertEquals(9L * SLICE, allocator.usedMemory(), "purged: charged again");
        assertChargedAtMost(base, 9L * SLICE, "purged: charged again");
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(0, allocator.usedMemory(), "the close credits the rest");
        assertCredited(base);
    }

    /**
     * Shared slices: a claim that would pass the limit throws, its slices free again with no memory behind them; a
     * claim of committed slices needs no charge.
     */
    @Test
    void aSharedClaimPastTheLimitGivesItsSlicesBack() {
        assumeTrue(MmapRegionSource.isAvailable(), "mmap(2) regions are not available here");
        long base = PlatformDependent.usedDirectMemory();
        AdaptivePoolingAllocator allocator = closer.add(newSharedAllocator(segments, regions, REGION_SIZE, INTERVAL));
        PageStore store = allocator.pageStore;
        store.claimSlices(9, false);
        assertChargedAtMost(base, 9L * SLICE, "the first claim");
        Segment block = store.regions[0].blocks[0];
        long filler = overfill();
        try {
            assertThrows(OutOfDirectMemoryError.class, () -> store.claimSlices(9, false));
            block.releaseRun(0, 9, System.nanoTime());
            assertEquals(0, (int) store.claimSlices(9, false), "committed: nothing to charge");
        } finally {
            credit(filler);
        }
        assertEquals(9, (int) store.claimSlices(9, false));
        assertEquals(18L * SLICE, allocator.usedMemory());
        assertChargedAtMost(base, 18L * SLICE, "the failed claim charged nothing");
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(0, allocator.usedMemory());
        assertCredited(base);
    }

    /**
     * {@code malloc}'d regions: charged whole by their allocation, never per slice, and credited when a wholly idle one
     * goes back, or at the close.
     */
    @Test
    void mallocRegionsAreChargedByTheirAllocation() {
        long base = PlatformDependent.usedDirectMemory();
        AdaptivePoolingAllocator allocator = closer.add(newSharedAllocator(segments, regions, SEGMENT_SIZE, INTERVAL));
        PageStore store = allocator.pageStore;
        long run = store.claimSlices(9, false);
        assertEquals(SEGMENT_SIZE, allocator.usedMemory(), "the region, whole");
        assertChargedAtMost(base, SEGMENT_SIZE, "the region, whole");
        store.claimBlocks(1); // a second region, whole: one block each
        assertEquals(2L * SEGMENT_SIZE, allocator.usedMemory());
        store.regions[0].blocks[0].releaseRun(0, 9, System.nanoTime());
        assertEquals((int) run, (int) store.claimSlices(9, false));
        assertEquals(2L * SEGMENT_SIZE, allocator.usedMemory(), "nothing per slice");
        assertChargedAtMost(base, 2L * SEGMENT_SIZE, "nothing per slice");
        store.regions[0].blocks[0].releaseRun(0, 9, System.nanoTime());
        store.purgeIfDue(System.nanoTime() + 4 * INTERVAL);
        assertEquals(SEGMENT_SIZE, allocator.usedMemory(), "credited by its free: the first region went back");
        assertChargedAtMost(base, SEGMENT_SIZE, "credited by its free");
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
        store.close();
        assertEquals(0, allocator.usedMemory(), "the close credits the rest");
        assertCredited(base);
    }

    /**
     * A {@code malloc}'d region past the direct memory limit: the claim that needed it throws, and regions are still
     * made once there is room.
     */
    @Test
    void aMallocRegionPastTheLimitFailsOnlyItsClaim() {
        long base = PlatformDependent.usedDirectMemory();
        AdaptivePoolingAllocator allocator = closer.add(newSharedAllocator(segments, regions, SEGMENT_SIZE, INTERVAL));
        PageStore store = allocator.pageStore;
        long filler = overfill();
        try {
            assertThrows(OutOfDirectMemoryError.class, () -> store.claimSlices(9, false));
            assertEquals(0, segments.segmentsAllocated());
        } finally {
            credit(filler);
        }
        assertEquals(0, (int) store.claimSlices(9, false));
        assertEquals(1, segments.segmentsAllocated(), "regions are still made");
        assertChargedAtMost(base, SEGMENT_SIZE, "the failed claim charged nothing");
        store.close();
        assertCredited(base);
    }

    /**
     * Charges the limit on top of what is used, unchecked: frees elsewhere credit only what they charged, so the
     * counter stays past the limit until {@link #credit}.
     */
    private static long overfill() {
        long filler = PlatformDependent.maxDirectMemory();
        // Charged as a negative credit: incrementMemoryCounter throws past the limit.
        for (long left = filler; left > 0; left -= Integer.MAX_VALUE) {
            PlatformDependent.decrementMemoryCounter((int) -Math.min(left, Integer.MAX_VALUE));
        }
        return filler;
    }

    private static void credit(long bytes) {
        for (long left = bytes; left > 0; left -= Integer.MAX_VALUE) {
            PlatformDependent.decrementMemoryCounter((int) Math.min(left, Integer.MAX_VALUE));
        }
    }
}
