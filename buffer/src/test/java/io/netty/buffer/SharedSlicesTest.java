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
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The page store's shared slices (see {@link PageStore#claimSlices}): first fit within a block, never across one,
 * heaps spread by sequence, releases from any thread reused at once, whole-block runs.
 */
final class SharedSlicesTest {
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;
    private static final int PER_BLOCK = SEGMENT_SIZE / SLICE;
    private static final int PER_REGION = REGION_SIZE / SLICE;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    private PageStore store(long purgeDelayNanos) {
        return newSharedAllocator(segments, regions, REGION_SIZE, purgeDelayNanos).pageStore;
    }

    /** The run's first slice in its region. */
    private static int slice(long run) {
        assertTrue(run >= 0, "no run");
        return (int) run;
    }

    private static Segment block(PageStore store, long run) {
        return store.region((int) (run >>> 32)).block(slice(run) / PER_BLOCK);
    }

    private static void release(PageStore store, long run, int n) {
        store.releaseSlices(block(store, run), slice(run) % PER_BLOCK, n);
    }

    @Test
    void firstFitWithinABlockNeverAcrossOne() {
        PageStore store = store(INTERVAL);
        assertEquals(0, slice(store.claimSlices(40, 0, PageStore.NO_HEAP)));
        assertEquals(PER_BLOCK, slice(store.claimSlices(40, 0, PageStore.NO_HEAP)), "24 left in block 0: block 1");
        assertEquals(40, slice(store.claimSlices(24, 0, PageStore.NO_HEAP)), "the tail of block 0 fits");
        assertEquals(PER_BLOCK + 40, slice(store.claimSlices(1, 0, PageStore.NO_HEAP)));
        assertEquals(1, regions.regions.size());
        assertEquals(0, segments.segmentsAllocated(), "no segment of its own");
    }

    /** As mimalloc's thread sequence: the blocks claimed in so far are visited from {@code seq} modulo their count. */
    @Test
    void heapsStartInTheBlockOfTheirSequence() {
        PageStore store = store(INTERVAL);
        for (int block = 0; block < 3; block++) {
            assertEquals(block * PER_BLOCK, slice(store.claimSlices(60, 0, PageStore.NO_HEAP)));
        }
        assertEquals(PER_BLOCK + 60, slice(store.claimSlices(2, 4, PageStore.NO_HEAP)), "4 % 3: block 1");
        assertEquals(2 * PER_BLOCK + 60, slice(store.claimSlices(2, 5, PageStore.NO_HEAP)), "5 % 3: block 2");
        assertEquals(60, slice(store.claimSlices(2, 3, PageStore.NO_HEAP)), "3 % 3: block 0");
        assertEquals(PER_BLOCK + 62, slice(store.claimSlices(2, 4, PageStore.NO_HEAP)));
        assertEquals(2 * PER_BLOCK + 62, slice(store.claimSlices(2, 4, PageStore.NO_HEAP)), "block 1 full: on");
        assertEquals(3 * PER_BLOCK, slice(store.claimSlices(3, 4, PageStore.NO_HEAP)), "all full: a new block");
    }

    /** A run released by another thread is free for the next claim at once, and a second release throws. */
    @Test
    void aReleaseFromAnyThreadIsReusedAtOnce() throws Exception {
        final PageStore store = store(INTERVAL);
        final AtomicLong claimed = new AtomicLong(-1);
        Thread claimer = new Thread(() -> claimed.set(store.claimSlices(9, 3, PageStore.NO_HEAP)));
        claimer.start();
        claimer.join();
        Thread releaser = new Thread(() -> release(store, claimed.get(), 9));
        releaser.start();
        releaser.join();
        assertEquals(0, store.sliceCounts()[0]);
        long again = store.claimSlices(9, 7, PageStore.NO_HEAP);
        assertEquals(slice(claimed.get()), slice(again), "the same slices");
        release(store, again, 9);
        assertThrows(IllegalStateException.class, () -> release(store, again, 9));
    }

    /**
     * One-shot runs of whole blocks: never a block with a slice claimed, and given back whole. A slice run cannot
     * take a block claimed whole.
     */
    @Test
    void wholeBlocksSkipBlocksInUse() {
        PageStore store = store(INTERVAL);
        long first = store.takeRun(2, 0);
        assertEquals(0, (int) first);
        assertEquals(2 * PER_BLOCK, slice(store.claimSlices(1, 0, PageStore.NO_HEAP)), "blocks 0 and 1 are taken");
        long second = store.takeRun(2, 0);
        assertEquals(3, (int) second, "block 2 has a slice claimed");
        Segment whole = store.takeWhole(0);
        assertEquals(5, whole.slot);
        Region region = store.region(0);
        store.freeRun(region, 0, 2);
        assertEquals(0, (int) store.takeRun(2, 0), "free again");
        store.free(whole);
        assertEquals(5, store.takeWhole(0).slot);
        // Blocks 0, 1, 3, 4 and 5, and a slice of block 2.
        assertArrayEquals(new int[] {5 * PER_BLOCK + 1, 0, PER_REGION - 5 * PER_BLOCK - 1}, store.sliceCounts());
    }
}
