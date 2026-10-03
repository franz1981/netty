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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When a purge pass runs and how far it goes: every time is a {@code now} the test passes, from a base where the raw
 * {@link System#nanoTime()} values wrap from positive to negative, cross 0, or start at 0.
 */
final class PageStorePurgePassTest {
    private static final long DELAY = 1_000_000_000L;
    private static final long CHECK = DELAY / 4;
    private static final int SLICE = PageStoreConfig.SLICE_SIZE_BYTES;
    private static final int PER_BLOCK = SEGMENT_SIZE / SLICE;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private final CountingMemorySource segments = new CountingMemorySource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** A store whose last pass was at {@code base}, with blocks 0 to {@code blocks - 1} of its region claimed whole. */
    private PageStore store(long base, int blocks) {
        return store(base, blocks, REGION_SIZE);
    }

    private PageStore store(long base, int blocks, int regionSize) {
        PageStore store = closer.add(newSharedAllocator(segments, regions, regionSize, DELAY)).pageStore;
        assertEquals(DELAY / 4, store.config.purgeCheckNanos);
        store.lastPurgeNanos = base;
        for (int i = 0; i < blocks; i++) {
            assertEquals(i * PER_BLOCK, (int) store.claimSlices(PER_BLOCK, 0, false));
        }
        return store;
    }

    private static Segment block(PageStore store, int slot) {
        return store.regions[0].blocks[slot];
    }

    /** The block of the {@code index}th purge call. */
    private int purgedBlock(int index) {
        return regions.purges.get(index)[0] / SEGMENT_SIZE;
    }

    /**
     * From {@code Long.MAX_VALUE - DELAY / 2}, the release is stamped before the wrap and due after it; from
     * {@code -DELAY / 4}, the release is stamped 0.
     */
    @ParameterizedTest
    @ValueSource(longs = {Long.MAX_VALUE - DELAY / 2, -DELAY / 4, 0})
    void noPassUntilARunIsReleasedAndIdleForTheDelay(long base) {
        PageStore store = store(base, 1);
        store.purgeIfDue(base + CHECK);
        assertEquals(0, store.purges, "nothing released: no pass");
        long released = base + DELAY / 4;
        block(store, 0).releaseRun(3, 2, released);
        store.purgeIfDue(released + CHECK / 2);
        store.purgeIfDue(released + DELAY - 1);
        assertEquals(0, store.purges, "armed, not due");
        store.purgeIfDue(released + DELAY);
        assertEquals(1, store.purges);
        assertEquals(1, regions.purgeCalls());
        store.purgeIfDue(released + 20 * DELAY);
        assertEquals(1, store.purges, "nothing released since: no pass");
    }

    /** The run released in a block the pass has passed is found only because its release arms the purge again. */
    @ParameterizedTest
    @ValueSource(longs = {Long.MAX_VALUE - DELAY / 2, -DELAY / 4, 0})
    void aReleaseDuringAPassArmsThePurgeAgain(long base) {
        PageStore store = store(base, 1);
        block(store, 0).releaseRun(1, 1, base);
        long pass = base + DELAY;
        AtomicBoolean released = new AtomicBoolean();
        regions.onPurge = () -> {
            if (released.compareAndSet(false, true)) {
                block(store, 0).releaseRun(5, 1, pass);
            }
        };
        store.purgeIfDue(pass);
        assertEquals(1, regions.purgeCalls());
        store.purgeIfDue(pass + DELAY - 1);
        assertEquals(1, store.purges, "armed by the release during the pass, not due");
        store.purgeIfDue(pass + DELAY);
        assertEquals(2, store.purges);
        assertEquals(2, regions.purgeCalls());
        assertEquals(5 * SLICE, regions.purges.get(1)[0]);
    }

    /** No release after the first pass: the slices it skipped go back once they were free for the delay. */
    @ParameterizedTest
    @ValueSource(longs = {Long.MAX_VALUE - DELAY / 2, -DELAY / 4, 0})
    void slicesAPassSkippedArePurgedWhenDueWithoutAnotherRelease(long base) {
        PageStore store = store(base, 2);
        block(store, 0).releaseRun(0, 1, base);
        block(store, 1).releaseRun(0, 1, base + DELAY / 2);
        block(store, 1).releaseRun(2, 1, base + 3 * DELAY / 4);
        store.purgeIfDue(base + DELAY);
        assertEquals(1, regions.purgeCalls(), "block 1 not idle for the delay yet");
        store.purgeIfDue(base + 3 * DELAY / 2 - 1);
        assertEquals(1, store.purges, "armed for the first slice skipped, not due");
        store.purgeIfDue(base + 3 * DELAY / 2);
        assertEquals(2, store.purges);
        assertEquals(2, regions.purgeCalls(), "the slice freed at base + DELAY / 2");
        assertEquals(SEGMENT_SIZE, regions.purges.get(1)[0]);
        store.purgeIfDue(base + 7 * DELAY / 4);
        assertEquals(3, store.purges);
        assertEquals(3, regions.purgeCalls(), "the slice freed at base + 3 * DELAY / 4");
        assertEquals(SEGMENT_SIZE + 2 * SLICE, regions.purges.get(2)[0]);
        store.purgeIfDue(base + 10 * DELAY);
        assertEquals(3, store.purges, "nothing left");
    }

    /**
     * A pass stops at the call that reaches {@link PageStore#PURGE_BYTES}, made whole, here in the middle of a block;
     * the next pass runs after the cadence floor with no release meanwhile, and goes on from that block, then the
     * blocks ahead of it, then around to the blocks behind.
     */
    @ParameterizedTest
    @ValueSource(longs = {Long.MAX_VALUE - DELAY / 2, -DELAY / 4, 0})
    void aPassStopsAtItsBudgetAndTheNextGoesOnFromThere(long base) {
        // Two runs of run slices per block, apart; an odd number of them reaches the budget: a pass ends after the
        // first run of block stop.
        int run = PER_BLOCK / 2 - 1;
        while (callsToBudget(run) % 2 == 0) {
            run--;
        }
        int calls = callsToBudget(run);
        long runBytes = (long) run * SLICE;
        assertTrue((calls - 1) * runBytes < PageStore.PURGE_BYTES && calls * runBytes > PageStore.PURGE_BYTES);
        int stop = calls / 2;
        int blocks = stop + 3;
        PageStore store = store(base, blocks, blocks * SEGMENT_SIZE);
        for (int slot = 0; slot < blocks; slot++) {
            block(store, slot).releaseRun(0, run, base);
            block(store, slot).releaseRun(PER_BLOCK / 2, run, base);
        }
        long now = base + DELAY;
        store.purgeIfDue(now);
        assertEquals(calls, regions.purgeCalls(), "the call that reaches the budget is the last");
        assertEquals(calls * runBytes, store.bytesPurged);
        assertEquals(stop * SEGMENT_SIZE, regions.purges.get(calls - 1)[0], "stopped after block stop's first run");
        // Due slices behind where the pass stopped: the next pass reaches them last.
        block(store, 0).releaseRun(PER_BLOCK / 2 - 1, 1, base);
        block(store, 0).releaseRun(PER_BLOCK - 1, 1, base);
        store.purgeIfDue(now + CHECK - 1);
        assertEquals(calls, regions.purgeCalls(), "the cadence floor");
        store.purgeIfDue(now + CHECK);
        assertEquals(calls + 7, regions.purgeCalls());
        assertEquals(stop * SEGMENT_SIZE + PER_BLOCK / 2 * SLICE, regions.purges.get(calls)[0],
                "the rest of block stop first");
        for (int i = 1; i < 5; i++) {
            assertEquals(stop + 1 + (i - 1) / 2, purgedBlock(calls + i), "then the blocks ahead");
        }
        assertEquals((PER_BLOCK / 2 - 1) * SLICE, regions.purges.get(calls + 5)[0], "then around to block 0");
        assertEquals((PER_BLOCK - 1) * SLICE, regions.purges.get(calls + 6)[0]);
        store.purgeIfDue(now + 10 * DELAY);
        assertEquals(2, store.purges, "the last pass finished: disarmed");
    }

    /** A pass whose calls reach {@link PageStore#PURGE_BYTES} exactly stops there, and the next one goes on. */
    @Test
    void aPassThatSpendsItsBudgetExactlyStopsThere() {
        int blocks = (int) (PageStore.PURGE_BYTES / SEGMENT_SIZE) + 1;
        long base = 0;
        PageStore store = store(base, blocks, blocks * SEGMENT_SIZE);
        for (int slot = 0; slot < blocks; slot++) {
            block(store, slot).releaseRun(0, PER_BLOCK, base);
        }
        store.purgeIfDue(base + DELAY);
        assertEquals(blocks - 1, regions.purgeCalls());
        assertEquals(PageStore.PURGE_BYTES, store.bytesPurged);
        store.purgeIfDue(base + DELAY + CHECK);
        assertEquals(blocks, regions.purgeCalls(), "the last block, in the next pass");
    }

    /** The calls of {@code run} slices each that reach {@link PageStore#PURGE_BYTES}. */
    private static int callsToBudget(int run) {
        long runBytes = (long) run * SLICE;
        return (int) ((PageStore.PURGE_BYTES + runBytes - 1) / runBytes);
    }

    /**
     * A pass makes one call at least, however large: a {@code malloc}'d region larger than
     * {@link PageStore#PURGE_BYTES} goes back whole, one per pass.
     */
    @Test
    void aPassMakesOneCallEvenPastItsBudget() {
        int blocks = (int) (PageStore.PURGE_BYTES / SEGMENT_SIZE) + 1;
        CountingRegionSource malloc = new CountingRegionSource(true);
        PageStore store = closer.add(newSharedAllocator(segments, malloc, blocks * SEGMENT_SIZE, DELAY)).pageStore;
        assertEquals(0, (int) store.takeRun(blocks));
        assertEquals(1L << 32, store.takeRun(blocks));
        long base = System.nanoTime();
        for (Region region : store.regions) {
            for (Segment block : region.blocks) {
                block.releaseRun(0, PER_BLOCK, base);
            }
        }
        store.purgeIfDue(base + DELAY);
        assertEquals(1, store.regionsReleased);
        assertEquals(1, malloc.released.size());
        store.purgeIfDue(base + DELAY + CHECK);
        assertEquals(2, store.regionsReleased, "the other one, in the next pass");
        assertEquals(2, store.purges);
    }

    /**
     * A release stamped {@link Long#MIN_VALUE}, a {@link System#nanoTime()} like any other: its slices keep their
     * memory, so that the next claim of them charges nothing, and are purged once free for the delay.
     */
    @Test
    void aReleaseStampedLongMinValueKeepsItsMemory() {
        long released = Long.MIN_VALUE;
        PageStore store = store(released - DELAY, 1);
        AdaptivePoolingAllocator allocator = store.allocator;
        long used = allocator.usedMemory();
        block(store, 0).releaseRun(0, 4, released);
        assertEquals(0, (int) store.claimSlices(4, 0, false));
        assertEquals(PER_BLOCK, store.slicesCommitted, "not charged again");
        assertEquals(used, allocator.usedMemory());
        block(store, 0).releaseRun(0, 4, released);
        store.purgeIfDue(released + DELAY - 1);
        assertEquals(0, store.purges, "not due");
        store.purgeIfDue(released + DELAY);
        assertEquals(1, regions.purgeCalls());
        assertEquals(4 * SLICE, regions.purges.get(0)[1]);
        assertEquals(used - 4L * SLICE, allocator.usedMemory());
        PageStoreTestSupport.assertSharedAccounted(segments, allocator);
    }
}
