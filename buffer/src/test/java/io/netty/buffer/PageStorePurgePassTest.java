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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** A store whose last pass was at {@code base}, with blocks 0 to {@code blocks - 1} of its region claimed whole. */
    private PageStore store(long base, int blocks) {
        PageStore store = closer.add(newSharedAllocator(segments, regions, REGION_SIZE, DELAY)).pageStore;
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
     * {@link PageStore#PURGE_CALLS} runs a pass at most; the next pass runs after the cadence floor with no release
     * meanwhile, and goes on from the block where the last one stopped, before the blocks ahead of it.
     */
    @ParameterizedTest
    @ValueSource(longs = {Long.MAX_VALUE - DELAY / 2, -DELAY / 4, 0})
    void aPassStopsAtItsBudgetAndTheNextGoesOnFromThere(long base) {
        int budget = PageStore.PURGE_CALLS;
        PageStore store = store(base, 3);
        // Single free slices, apart: one call each. Block 0, then 3 in block 1 fill the first pass.
        int first = budget - 3;
        releaseApart(block(store, 0), 0, first, base);
        releaseApart(block(store, 1), 0, 5, base);
        releaseApart(block(store, 2), 0, 3, base);
        long now = base + DELAY;
        store.purgeIfDue(now);
        assertEquals(budget, regions.purgeCalls());
        assertEquals(1, purgedBlock(budget - 1), "stopped in block 1");
        // Due runs ahead of where the pass stopped: the next pass reaches them last.
        releaseApart(block(store, 0), 2 * first, budget, base);
        store.purgeIfDue(now + CHECK - 1);
        assertEquals(budget, regions.purgeCalls(), "the cadence floor");
        store.purgeIfDue(now + CHECK);
        assertEquals(2 * budget, regions.purgeCalls());
        assertEquals(1, purgedBlock(budget), "the rest of block 1 first");
        assertEquals(1, purgedBlock(budget + 1));
        for (int i = budget + 2; i < budget + 5; i++) {
            assertEquals(2, purgedBlock(i));
        }
        for (int i = budget + 5; i < 2 * budget; i++) {
            assertEquals(0, purgedBlock(i), "then around to block 0");
        }
        store.purgeIfDue(now + 2 * CHECK);
        assertEquals(budget + 5 + budget, regions.purgeCalls(), "the rest of block 0, then nothing left");
        assertEquals(3, store.purges);
        store.purgeIfDue(now + 10 * DELAY);
        assertEquals(3, store.purges, "the last pass finished: disarmed");
    }

    /** {@code n} single slices of {@code block} from {@code start}, one free and one claimed. */
    private static void releaseApart(Segment block, int start, int n, long now) {
        for (int i = 0; i < n; i++) {
            block.releaseRun(start + 2 * i, 1, now);
        }
    }
}
