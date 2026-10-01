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
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    private List<String> sources(String name, String field) {
        List<String> values = new ArrayList<String>();
        for (RecordedEvent event : named(name)) {
            values.add(event.getString(field));
        }
        return values;
    }

    @SuppressWarnings("Since15")
    @Test
    public void eventsFollowTheMemoryOfARegion() throws Exception {
        assumeFalse(AdaptivePoolingAllocator.IS_LOW_MEM, "low-memory mode pools nothing above the size classes");
        final CountDownLatch stateSeen = new CountDownLatch(1);
        final CountDownLatch unmapped = new CountDownLatch(1);
        final String thread = Thread.currentThread().getName();
        CountingRegionSource regions = new CountingRegionSource();
        final AdaptivePoolingAllocator allocator;
        try (RecordingStream stream = new RecordingStream()) {
            // The events are kept past their handler: a reused one would be overwritten by the next.
            stream.setReuse(false);
            for (String name : new String[] {PageStoreMapEvent.NAME, PageStorePurgeEvent.NAME,
                    SegmentTakeEvent.NAME, SegmentGiveBackEvent.NAME}) {
                stream.enable(name);
                stream.onEvent(name, event -> add(event, thread));
            }
            // The unmap is the last event: once it is seen, so are the others of this thread.
            stream.enable(PageStoreUnmapEvent.NAME);
            stream.onEvent(PageStoreUnmapEvent.NAME, event -> {
                add(event, thread);
                unmapped.countDown();
            });
            stream.enable(PageStoreStateEvent.NAME).withPeriod(Duration.ofMillis(50));
            stream.startAsync();
            allocator = newAllocator(new CountingSegmentSource(), regions, REGION_SIZE, REGION_ALIGNMENT);
            PageStore store = allocator.pageStore;
            final int id = System.identityHashCode(allocator);
            stream.onEvent(PageStoreStateEvent.NAME, event -> {
                if (event.getInt("store") == id && event.getInt("region") == 0
                        && event.getLong("outBytes") == 4L * SEGMENT_SIZE) {
                    events.add(event);
                    stateSeen.countDown();
                }
            });

            ByteBuf pooled = allocator.allocate(512 * 1024, 512 * 1024); // a buddy chunk: a fresh slot
            ByteBuf oneShot = allocator.allocate(3 * MIB, 3 * MIB); // a whole segment: another fresh slot
            ByteBuf run = allocator.allocate(SEGMENT_SIZE + 1, SEGMENT_SIZE + 1); // a run of two slots
            base = store.region(0).buffer.memoryAddress();
            assertTrue(stateSeen.await(10, TimeUnit.SECONDS), "a periodic state event with the four slots out");
            oneShot.release();
            run.release();
            long now = System.nanoTime();
            store.purgeIfDue(now += INTERVAL);
            store.purgeIfDue(now + INTERVAL);
            ByteBuf again = allocator.allocate(3 * MIB, 3 * MIB); // a purged slot: will page-fault
            again.release();
            pooled.release();
            store.close();
            assertTrue(unmapped.await(10, TimeUnit.SECONDS), "the unmap event");
            // Events of one thread may still be on their way from an earlier buffer: wait for all four takes.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (named(SegmentTakeEvent.NAME).size() < 4 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
        }
        List<RecordedEvent> maps = named(PageStoreMapEvent.NAME);
        assertEquals(1, maps.size());
        assertEquals(base, maps.get(0).getLong("address"));
        assertEquals(REGION_SIZE, maps.get(0).getLong("length"));
        assertEquals(0, maps.get(0).getInt("errno"));

        List<String> taken = sources(SegmentTakeEvent.NAME, "source");
        assertEquals(4, taken.size(), taken.toString());
        assertEquals(PageStore.FRESH_SLOT, taken.get(0), taken.toString());
        assertEquals(PageStore.FRESH_SLOT, taken.get(1), taken.toString());
        assertEquals(PageStore.SLOT_RUN, taken.get(2), taken.toString());
        assertEquals(PageStore.PURGED_SLOT, taken.get(3), taken.toString());
        RecordedEvent runTaken = named(SegmentTakeEvent.NAME).get(2);
        assertEquals(2, runTaken.getInt("segments"));
        assertEquals(2L * SEGMENT_SIZE, runTaken.getLong("length"));

        List<String> givenBack = sources(SegmentGiveBackEvent.NAME, "destination");
        assertTrue(givenBack.contains(PageStore.FREE_SLOT), givenBack.toString());
        assertTrue(givenBack.contains(PageStore.SLOT_RUN), givenBack.toString());

        List<RecordedEvent> purges = named(PageStorePurgeEvent.NAME);
        long purged = 0;
        for (RecordedEvent purge : purges) {
            assertEquals("slots", purge.getString("unit"));
            assertEquals(0, purge.getInt("errno"));
            purged += purge.getLong("length");
        }
        assertEquals(3L * SEGMENT_SIZE, purged, "the one-shot's slot and the run's two, coalesced or not");

        List<RecordedEvent> unmaps = named(PageStoreUnmapEvent.NAME);
        assertEquals(1, unmaps.size());
        assertEquals(base, unmaps.get(0).getLong("address"));

        RecordedEvent state = named(PageStoreStateEvent.NAME).get(0);
        assertEquals(base, state.getLong("base"));
        assertEquals(REGION_SIZE, state.getLong("length"));
        assertEquals(4L * SEGMENT_SIZE, state.getLong("outBytes"), "the chunk, the one-shot and the run");
        assertTrue(state.getString("out").startsWith(Long.toHexString(base) + '-'), state.getString("out"));
    }

    /**
     * The segments allocated on their own are tracked for the periodic state event only while that event is enabled:
     * with JFR available but no recording, the store keeps no reference to them.
     */
    @Test
    @EnabledForJreRange(min = JRE.JAVA_17)
    void ownSegmentsAreTrackedOnlyWhileTheStateEventIsEnabled() {
        AdaptivePoolingAllocator allocator = newAllocator(new CountingSegmentSource(), SEGMENT_SIZE);
        PageStore store = allocator.pageStore;
        assumeFalse(store.ownSegments == null, "JFR is not available");
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        Segment untracked = heap.claim(63);
        assertFalse(store.ownSegments.contains(untracked), "no recording: not tracked");
        try (RecordingStream stream = new RecordingStream()) {
            stream.enable(PageStoreStateEvent.NAME);
            stream.startAsync();
            Segment tracked = heap.claim(63); // no room left in the first
            assertTrue(untracked != tracked);
            assertTrue(store.ownSegments.contains(tracked), "recorded: tracked");
            PageStoreTestSupport.giveBack(heap, tracked, 0, 63);
            assertFalse(store.ownSegments.contains(tracked), "given back: forgotten");
        }
    }
}
