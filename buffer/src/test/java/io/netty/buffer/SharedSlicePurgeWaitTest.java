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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newSharedAllocator;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared slices: a claim and a purge that hold the same run. Isolated: the claim waits for the purger a bounded time,
 * which other tests' threads must not eat.
 */
@Isolated("The claim's wait for the purger is bounded in time")
final class SharedSlicePurgeWaitTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private static final int PER_BLOCK = SEGMENT_SIZE / PageStoreConfig.SLICE_SIZE_BYTES;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /**
     * A claim that finds no fit while the purger holds the only free run waits for it to give the run back and takes
     * it, instead of mapping a region; the purge itself holds one block's run at most.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aClaimWaitsForThePurgersRunInsteadOfMappingARegion() throws Exception {
        final PageStore store = closer.add(newSharedAllocator(segments, regions, REGION_SIZE, INTERVAL)).pageStore;
        int blocks = REGION_SIZE / SEGMENT_SIZE;
        assertEquals(0, (int) store.takeRun(blocks), "the whole region");
        Region region = store.regions[0];
        store.freeRun(region, blocks - 1, 1); // the last block is the only free memory
        // A first purge, so that the one under test does not pay for the first call's linking.
        store.purgeIfDue(System.nanoTime() + 2 * INTERVAL);
        assertEquals(blocks - 1, (int) store.takeRun(1));
        store.freeRun(region, blocks - 1, 1);
        final CountDownLatch purging = new CountDownLatch(1);
        final AtomicReference<String> failure = new AtomicReference<String>();
        regions.onPurge = () -> {
            purging.countDown();
            // Hold the run until the claim waits for it, or long enough to see that it does not.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (store.purgeWaits == 0 && System.nanoTime() - deadline < 0) {
                Thread.yield();
            }
        };
        Thread purger = new Thread(() -> {
            try {
                store.purgeIfDue(System.nanoTime() + 4 * INTERVAL);
            } catch (Throwable t) {
                failure.compareAndSet(null, t.toString());
            }
        });
        purger.start();
        assertTrue(purging.await(10, TimeUnit.SECONDS));
        long run = store.claimSlices(9, 0, false);
        purger.join();
        assertNull(failure.get());
        assertEquals(1, store.purgeWaits);
        assertEquals((blocks - 1) * PER_BLOCK, (int) run, "the purged block");
        assertEquals(1, store.regionCount(), "no region mapped for slices out for their purge");
        assertEquals(2, regions.purgeCalls());
        assertArrayEquals(new int[] {(blocks - 1) * SEGMENT_SIZE, SEGMENT_SIZE}, regions.purges.get(1));
        store.close();
    }

}
