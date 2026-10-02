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
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The page store's JFR events: the system calls (map, purge, unmap), segments taken and given back with where they
 * came from and went, and the periodic map of a store's memory.
 */
@Timeout(30)
@EnabledForJreRange(min = JRE.JAVA_17) // RecordingStream
@Isolated
public class PageStoreJfrTest {
    private final List<RecordedEvent> events = new CopyOnWriteArrayList<RecordedEvent>();

    /** Events of this test's thread only: other stores of the same JVM record theirs too. */
    private void add(RecordedEvent event, String thread) {
        if (event.getThread() != null && thread.equals(event.getThread().getJavaName())) {
            events.add(event);
        }
    }

    /** The region under test: events of other stores that ran on this thread's name are elsewhere. */
    private long base;

    private List<RecordedEvent> named(String name) {
        List<RecordedEvent> found = new ArrayList<RecordedEvent>();
        for (RecordedEvent event : events) {
            if (event.getEventType().getName().equals(name) && inRegion(event)) {
                found.add(event);
            }
        }
        return found;
    }

    private boolean inRegion(RecordedEvent event) {
        String field = event.hasField("address") ? "address" : "base";
        long address = event.getLong(field);
        return address >= base && address < base + REGION_SIZE;
    }

    /**
     * Shared slices: the purge purges the slices of each idle run with one call, and the periodic state event maps the
     * region slice by slice.
     */
    @SuppressWarnings("Since15")
    @Test
    public void sharedSliceEventsFollowTheirRuns() throws Exception {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
        final CountDownLatch stateSeen = new CountDownLatch(1);
        final CountDownLatch unmapped = new CountDownLatch(1);
        final String thread = Thread.currentThread().getName();
        final int slice = PageStoreConfig.SLICE_SIZE_BYTES;
        final long out = 8L * slice + SEGMENT_SIZE;
        CountingRegionSource regions = new CountingRegionSource();
        try (RecordingStream stream = new RecordingStream()) {
            stream.setReuse(false);
            stream.enable(PageStorePurgeEvent.NAME);
            stream.onEvent(PageStorePurgeEvent.NAME, event -> add(event, thread));
            stream.enable(PageStoreUnmapEvent.NAME);
            stream.onEvent(PageStoreUnmapEvent.NAME, event -> {
                add(event, thread);
                unmapped.countDown();
            });
            stream.enable(PageStoreStateEvent.NAME).withPeriod(Duration.ofMillis(50));
            stream.startAsync();
            AdaptivePoolingAllocator allocator = PageStoreTestSupport.newSharedAllocator(new CountingSegmentSource(),
                    regions, REGION_SIZE, INTERVAL);
            PageStore store = allocator.pageStore;
            final int id = System.identityHashCode(allocator);
            stream.onEvent(PageStoreStateEvent.NAME, event -> {
                if (event.getInt("store") == id && event.getInt("region") == 0 && event.getLong("outBytes") == out) {
                    events.add(event);
                    stateSeen.countDown();
                }
            });
            ByteBuf span = allocator.allocate(512 * 1024, 512 * 1024); // a span of 8 slices
            ByteBuf whole = allocator.allocate(SEGMENT_SIZE, SEGMENT_SIZE); // a span of a whole block
            base = store.regions[0].buffer.memoryAddress();
            assertTrue(stateSeen.await(10, TimeUnit.SECONDS), "a periodic state event with 8 slices and a block out");
            span.release();
            whole.release();
            allocator.allocate(512 * 1024, 512 * 1024).release(); // committed already
            store.purgeIfDue(System.nanoTime() + 2 * INTERVAL);
            store.close();
            assertTrue(unmapped.await(10, TimeUnit.SECONDS), "the unmap event");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (named(PageStorePurgeEvent.NAME).size() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
        }
        long purged = 0;
        for (RecordedEvent purge : named(PageStorePurgeEvent.NAME)) {
            assertEquals("slices", purge.getString("unit"));
            assertEquals(0, purge.getInt("errno"));
            purged += purge.getLong("length");
        }
        assertEquals(out, purged, "the span's slices and the block's");
        RecordedEvent state = named(PageStoreStateEvent.NAME).get(0);
        assertEquals(REGION_SIZE, state.getLong("length"));
        assertEquals(REGION_SIZE - out, state.getLong("untouchedBytes"));
    }
}
