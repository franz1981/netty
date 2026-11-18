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
package io.netty.incubator.allocator;

import io.netty.buffer.ByteBuf;
import io.netty.incubator.allocator.PageStoreAllocators.Geometry;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import io.netty.util.internal.PlatformDependent;
import jdk.jfr.EventType;
import jdk.jfr.FlightRecorder;
import jdk.jfr.consumer.RecordedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * That using the allocator registers none of its events while no recorder exists, and that a recording started
 * afterwards receives them. It runs in a JVM of its own (the {@code test-jfr-lazy} execution): any other test of the
 * module that records would have initialized the Flight Recorder before. Another user of JFR in the JVM, a shaded
 * Netty of a test tool for one, may well have registered its own events: only the module's nine are looked for, by
 * name, in JFR's list of registered event types.
 */
@EnabledForJreRange(min = JRE.JAVA_11)
class PageStoreJfrLazyTest {
    private static final String[] NAMES = {
            PageStoreMapEvent.NAME, PageStoreUnmapEvent.NAME, PageStorePurgeEvent.NAME,
            AllocateChunkEvent.NAME, FreeChunkEvent.NAME,
            AllocateBufferEvent.NAME, ReallocateBufferEvent.NAME, FreeBufferEvent.NAME,
            PageStoreStateEvent.NAME};

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    @TempDir
    Path dir;

    /** The names of the module's event types that JFR lists as registered. */
    private static Set<String> registeredOfTheModule() {
        Set<String> found = new HashSet<String>();
        for (EventType type : FlightRecorder.getFlightRecorder().getEventTypes()) {
            if (Arrays.asList(NAMES).contains(type.getName())) {
                found.add(type.getName());
            }
        }
        return found;
    }

    @Test
    void usingTheAllocatorRegistersNoneOfItsEventsAndALaterRecordingReceivesThem() throws Exception {
        assumeFalse(FlightRecorder.isInitialized(), "a Flight Recorder was started with the JVM");
        assumeTrue(Boolean.getBoolean("pagestore.test.jfrLazyExecution"), "runs in the test-jfr-lazy execution only");
        assumeTrue(PlatformDependent.isJfrEnabled(), "JFR is not available");
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.DEFAULTS, null);
        int block = Geometry.DEFAULTS.direct.segmentSize;
        for (int size : new int[] {100, 1024, 100_000, block, block + 1}) {
            ByteBuf direct = alloc.directBuffer(size);
            ByteBuf heap = alloc.heapBuffer(size);
            ticker.advance(2, TimeUnit.HOURS);
            direct.release();
            heap.release();
        }
        // A last pass of the purge, with no recorder, as the last round had.
        ticker.advance(2, TimeUnit.HOURS);
        alloc.directBuffer(block + 1).release();
        alloc.heapBuffer(block + 1).release();
        assertFalse(FlightRecorder.isInitialized(), "nothing initialized the Flight Recorder");

        // Asking JFR for its event types initializes it (the flag flips here, read in both JDK 11 and 25), which is
        // what the allocator looks for: it is the only user of the allocator and does not allocate until the recording
        // below, so nothing can register between this look and the one at the end.
        assertEquals(Collections.emptySet(), registeredOfTheModule(), "the allocator registered events");
        assertTrue(FlightRecorder.isInitialized(), "listing the event types did not initialize the recorder");

        // A recording starts, and nothing registers the events for it: only the allocator can, from one of the places
        // where it looks for a due purge. A buffer above the size classes is one of them (its span counts toward the
        // heap's decay clock, which asks the store). The buffer stays out until the recording is over.
        ByteBuf large = null;
        try (JfrEvents jfr = new JfrEvents(dir, true, false)) {
            large = alloc.directBuffer(200_000);
            // The request was queued by this thread: the executor runs its tasks in order, so when a task submitted
            // now has run, the registration has.
            GlobalEventExecutor.INSTANCE.submit(new Runnable() {
                @Override
                public void run() {
                }
            }).sync();
            alloc.directBuffer(1024).release();
            jfr.stop();
            boolean seen = false;
            for (RecordedEvent event : jfr.events(AllocateBufferEvent.NAME)) {
                seen |= event.getInt("size") == 1024;
            }
            assertTrue(seen, "the recording received the allocation");
            assertEquals(1, jfr.events(FreeBufferEvent.NAME).size());
            // The region was mapped before the recording: the periodic event reports it all the same.
            assertFalse(jfr.eventsOfAnyThread(PageStoreStateEvent.NAME).isEmpty(), "no state event");
        } finally {
            if (large != null) {
                large.release();
            }
        }
        // The positive control of the name match above, and of the periodic event's class being registered.
        assertEquals(new HashSet<String>(Arrays.asList(NAMES)), registeredOfTheModule(), "the events registered");

        // The first recording is over and the allocator is still open. A large buffer asks the store again: nothing
        // enables the state event now, so the periodic hook has no reason to stay.
        alloc.directBuffer(200_000).release();
        awaitTheExecutor();

        // A recording that enables the state event, on an allocator that allocates nothing: the hook is not there,
        // so no state event is taken at its end.
        try (JfrEvents jfr = new JfrEvents(dir, true, false)) {
            jfr.stop();
            assertTrue(jfr.eventsOfAnyThread(PageStoreStateEvent.NAME).isEmpty(),
                    "the state event of a recording that started on a quiet allocator");
        }

        // The next recording gets the state event from the large buffer that starts it.
        try (JfrEvents jfr = new JfrEvents(dir, true, false)) {
            ByteBuf span = alloc.directBuffer(200_000);
            awaitTheExecutor();
            span.release();
            jfr.stop();
            assertFalse(jfr.eventsOfAnyThread(PageStoreStateEvent.NAME).isEmpty(), "no state event");
        }
    }

    /** Returns once every task the calling thread queued on the executor has run, which are in order. */
    private static void awaitTheExecutor() throws InterruptedException {
        GlobalEventExecutor.INSTANCE.submit(new Runnable() {
            @Override
            public void run() {
            }
        }).sync();
    }
}
