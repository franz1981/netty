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

import io.netty.incubator.allocator.TestThreads.Body;
import io.netty.util.concurrent.FastThreadLocalThread;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A recording of the page store's events, from {@link #JfrEvents} to {@link #stop()}: the events are read back from
 * the dumped file once it is stopped, and only those of the threads the test owns are kept, as every other test of
 * the JVM allocates too. Keeping them is not enough: on JDK 11 the recorder loses a recording's data, the test's own
 * threads' included, when other threads commit these events fast enough, so a class that records runs @Isolated. The
 * events are enabled by name, so the recording can start before the allocator exists.
 */
@SuppressWarnings("deprecation") // Thread.getId() is the only id there is before Java 19
final class JfrEvents implements AutoCloseable {
    private static final String[] EVENTS = {
            PageStoreMapEvent.NAME, PageStoreUnmapEvent.NAME, PageStorePurgeEvent.NAME,
            AllocateChunkEvent.NAME, FreeChunkEvent.NAME,
            AllocateBufferEvent.NAME, ReallocateBufferEvent.NAME, FreeBufferEvent.NAME};

    private final Recording recording = new Recording();
    private final Path file;
    private final Set<Long> threads = ConcurrentHashMap.newKeySet();
    private final boolean register;
    private List<RecordedEvent> events;
    private boolean stopped;

    /**
     * Starts recording; the calling thread is one of the test's. With {@code state}, the periodic state event is
     * taken once, at the stop.
     */
    JfrEvents(Path dir, boolean state) {
        this(dir, state, true);
    }

    /**
     * As above; {@code register} says whether the page store's events are registered and the hook synchronized
     * right after the start, and again first thing at the stop, on this thread (in a production JVM the executor
     * does, after whoever polls first).
     */
    JfrEvents(Path dir, boolean state, boolean register) {
        this.register = register;
        file = dir.resolve("page-store.jfr");
        for (String name : EVENTS) {
            recording.enable(name);
        }
        if (state) {
            recording.enable(PageStoreStateEvent.NAME).with("period", "endChunk");
        }
        threads.add(Thread.currentThread().getId());
        recording.start();
        if (register) {
            PageStoreJfr.sync();
        }
    }

    /** Runs {@code body} on a new thread, a {@link FastThreadLocalThread} or not, joins it, and returns it. */
    Thread runOnThread(boolean fastThreadLocal, Body body) throws Exception {
        return TestThreads.runOnThread(fastThreadLocal, body, thread -> threads.add(thread.getId()));
    }

    /** Starts {@code body} on a new thread whose events are kept and returns at once, see {@link TestThreads#start}. */
    TestThreads.Started start(boolean fastThreadLocal, Body body) {
        return TestThreads.start(fastThreadLocal, body, thread -> threads.add(thread.getId()));
    }

    /** Stops the recording and reads every event of it back. */
    void stop() throws IOException {
        stopped = true;
        if (register) {
            // An allocator built after the start has no hook yet, and the executor may not have run.
            PageStoreJfr.sync();
        }
        recording.stop();
        recording.dump(file);
        recording.close();
        List<RecordedEvent> all = new ArrayList<RecordedEvent>();
        RecordingFile reader = new RecordingFile(file);
        try {
            while (reader.hasMoreEvents()) {
                RecordedEvent event = reader.readEvent();
                // Every thread of the JVM committed events: only the test's are kept, and the periodic state event,
                // which the JVM's own thread commits.
                if (event.getEventType().getName().equals(PageStoreStateEvent.NAME) || event.getThread() != null
                        && threads.contains(event.getThread().getJavaThreadId())) {
                    all.add(event);
                }
            }
        } finally {
            reader.close();
        }
        events = all;
    }

    /** Ends a recording that a failed test did not stop. */
    @Override
    public void close() {
        if (!stopped) {
            stopped = true;
            recording.stop();
            recording.close();
        }
    }

    /** The events of this name of the test's threads, in the order they were committed. */
    List<RecordedEvent> events(String name) {
        List<RecordedEvent> found = new ArrayList<RecordedEvent>();
        for (RecordedEvent event : events) {
            if (event.getEventType().getName().equals(name) && event.getThread() != null
                    && threads.contains(event.getThread().getJavaThreadId())) {
                found.add(event);
            }
        }
        return found;
    }

    /** The events of this name of the test's threads that one thread committed. */
    List<RecordedEvent> eventsOf(String name, Thread thread) {
        List<RecordedEvent> found = new ArrayList<RecordedEvent>();
        for (RecordedEvent event : events(name)) {
            if (event.getThread().getJavaThreadId() == thread.getId()) {
                found.add(event);
            }
        }
        return found;
    }

    /** The events of this name of every thread: the periodic one is taken by a thread of the JVM's. */
    List<RecordedEvent> eventsOfAnyThread(String name) {
        List<RecordedEvent> found = new ArrayList<RecordedEvent>();
        for (RecordedEvent event : events) {
            if (event.getEventType().getName().equals(name)) {
                found.add(event);
            }
        }
        return Collections.unmodifiableList(found);
    }
}
