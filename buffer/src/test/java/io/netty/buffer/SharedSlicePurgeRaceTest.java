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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared slices: a claim racing a purge that holds the only free run.
 */
final class SharedSlicePurgeRaceTest {
    private static final int PER_BLOCK = SEGMENT_SIZE / PageStoreConfig.SLICE_SIZE_BYTES;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private final CountingMemorySource segments = new CountingMemorySource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /**
     * The purge holds its run until the claim returned: a claim that waited for the purger would never return, and
     * fail by the timeout, which runs the test in its own thread for that. The claim maps one region at most, and the
     * purged run is free again once the purge gave it back.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aClaimRacingAPurgeNeverWaitsAndMapsOneRegionAtMost() throws Exception {
        final PageStore store = closer.add(newSharedAllocator(segments, regions, REGION_SIZE, INTERVAL)).pageStore;
        int blocks = REGION_SIZE / SEGMENT_SIZE;
        int purgedBlock = (blocks - 1) * PER_BLOCK;
        assertEquals(0, (int) store.takeRun(blocks), "the whole region");
        Region region = store.regions[0];
        store.freeRun(region, blocks - 1, 1); // the last block is the only free memory
        final CountDownLatch purging = new CountDownLatch(1);
        final AtomicBoolean claimed = new AtomicBoolean();
        final AtomicReference<String> failure = new AtomicReference<String>();
        regions.onPurge = () -> {
            purging.countDown();
            while (!claimed.get()) {
                Thread.yield();
            }
        };
        Thread purger = new Thread(() -> {
            try {
                store.purgeIfDue(System.nanoTime() + 2 * INTERVAL);
            } catch (Throwable t) {
                failure.compareAndSet(null, t.toString());
            } finally {
                purging.countDown();
            }
        });
        purger.start();
        purging.await();
        long run;
        try {
            run = store.claimSlices(8, 0, false);
        } finally {
            claimed.set(true);
        }
        purger.join();
        assertNull(failure.get());
        assertEquals(1, regions.purgeCalls());
        assertArrayEquals(new int[] {(blocks - 1) * SEGMENT_SIZE, SEGMENT_SIZE}, regions.purges.get(0));
        assertNotEquals((long) purgedBlock, run, "the run the purger held");
        assertTrue(store.regionCount() <= 2, store.regionCount() + " regions");
        int regionsMapped = store.regionCount();
        assertTrue(region.blocks[blocks - 1].isWhollyFree(), "the purged block, given back");
        // Not 8 slices: the new region's block has a fit of that bin.
        assertEquals(purgedBlock, (int) store.claimSlices(32, 0, false), "the lowest wholly free block");
        assertEquals(regionsMapped, store.regionCount());
        store.close();
    }
}
