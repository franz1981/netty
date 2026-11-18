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

import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers the page store's JFR events once a Flight Recorder exists, and keeps the stores the periodic
 * {@link PageStoreStateEvent} reports on. Touched only where {@code PlatformDependent.isJfrEnabled()}, as the events
 * are.
 * <p>
 * The events are {@code @Registered(false)}: using the allocator without recording sets nothing of JFR up. Where they
 * are registered is not a {@link jdk.jfr.FlightRecorderListener}: a listener stays in JFR's static list when no
 * recorder is ever initialized, and keeps the class loader of this module reachable.
 * <p>
 * Instead, the places that already do something rare ask {@link #poll()}: an allocator being built, a region being
 * added, a store being closed, and every {@code PageStore#purgeIfDue}, which the allocation paths call where they
 * look at the clock (a size class's tick, a span, a one-shot buffer), never an allocation of a size class's slot or
 * a release. Asking is a few reads when nothing changes, and never waits: the first thread that finds something to
 * do hands it to the {@link GlobalEventExecutor} and goes on, so no allocating thread, an event loop included,
 * registers events, adds or removes the hook, or waits for it.
 * <p>
 * The events are registered once and never unregistered. The periodic hook of the state event is installed exactly
 * while some recording has {@link PageStoreStateEvent} enabled and at least one {@link PageStore} is open. A JVM that
 * records without that event (it is off unless a setting turns it on) never installs it.
 * <p>
 * What a recording started on a running JVM loses: the page store events committed from its start until the
 * registration is over, which is the wait for the first thread that asks, the hand-off and the registration, the
 * event of the operation that asked included. The same holds for the state event of every recording: an allocator
 * that allocates nothing asks nothing, so a recording started on it receives no state event, and none of the
 * others, until some thread's next tick, large buffer or region. A size class ticks every few chunks' worth of
 * allocations, so "quiet" is bounded in allocations, not in time. Its first run after the hook is added reports
 * every region, those mapped before as well.
 * <p>
 * What keeps the class loader of this module. Measured on JDK 25.0.3, with the page store as the default allocator
 * of the loader: of what this module installs, only its hook keeps the loader, and only while a recording has the
 * state event enabled and a store is open (so the loader of a static default allocator is kept as long as that
 * holds). Not this module's: with no recorder at all, the loader was also kept when its default allocator was
 * netty-buffer's pooled or adaptive one, and was not with {@code -Dio.netty.jfr.enabled=false}; the cause was not
 * determined. On JDK 11, 17 and 21 any event class registered while a recording runs keeps its loader, whatever is
 * done with the hook and also after {@code unregister}, as it does for every library's events (measured on toy
 * event classes). Separately, JFR's own periodic-task thread takes the context class loader
 * of the thread that first initializes JFR; this module never does that, as it never calls
 * {@code FlightRecorder.getFlightRecorder()} nor starts a recording in production code.
 * <p>
 * No event class declares a field whose type is another event class, nor does a superclass: before the fix of
 * JDK-8368670 {@link FlightRecorder#register} deadlocks with a thread that defines the other event class, when the
 * registering thread loads it for the field's type while holding JFR's metadata lock. {@code
 * PageStoreJfrEventShapeTest} checks it.
 */
final class PageStoreJfr {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStoreJfr.class);
    private static final List<WeakReference<PageStore>> STORES = new CopyOnWriteArrayList<WeakReference<PageStore>>();
    private static final int NEW = 0;
    private static final int REGISTERED = 1;
    private static final int FAILED = 2;
    /** Whether the whole of {@link #sync()} was queued and has not started: one at a time. */
    private static final AtomicBoolean PENDING = new AtomicBoolean();
    /** Whether the failure to register was said at the warn level. */
    private static final AtomicBoolean WARNED = new AtomicBoolean();
    /** One instance: the JDK removes a hook by identity, and refuses the same one added twice. */
    private static final Runnable HOOK = new Runnable() {
        @Override
        public void run() {
            emitAll();
        }
    };
    private static final Runnable SYNC = new Runnable() {
        @Override
        public void run() {
            sync();
        }
    };
    /** Written under the class monitor only. FAILED is final: it is not tried again. */
    private static volatile int state = NEW;
    /** Whether the hand-off to the executor failed: final too, as the events will not be recorded. */
    private static volatile boolean handOffFailed;
    /** Written under the class monitor only: whether {@link #HOOK} is added. */
    private static volatile boolean hooked;

    private PageStoreJfr() {
    }

    /** Makes {@code store} one the periodic event reports on. */
    static void add(PageStore store) {
        for (WeakReference<PageStore> ref : STORES) {
            if (ref.get() == null) {
                STORES.remove(ref);
            }
        }
        STORES.add(new WeakReference<PageStore>(store));
    }

    /** Makes {@code store} no longer one the periodic event reports on, and asks if the hook is still wanted. */
    static void remove(PageStore store) {
        for (WeakReference<PageStore> ref : STORES) {
            PageStore stored = ref.get();
            if (stored == null || stored == store) {
                STORES.remove(ref);
            }
        }
        poll();
    }

    /** Whether the hook is wanted: a recording has the state event enabled, and a store is open. */
    private static boolean wanted() {
        if (!PageStoreStateEvent.isEventEnabled()) {
            return false;
        }
        for (WeakReference<PageStore> ref : STORES) {
            if (ref.get() != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Any thread: if the events are not registered yet and a Flight Recorder was initialized by now, or the hook is
     * added and not wanted or the other way round, asks the {@link GlobalEventExecutor} to {@link #sync()}. Never
     * waits.
     */
    static void poll() {
        int current = state;
        if (current == FAILED || handOffFailed) {
            return;
        }
        if (current == NEW) {
            if (!FlightRecorder.isInitialized()) {
                return;
            }
        } else if (wanted() == hooked) {
            return;
        }
        if (PENDING.compareAndSet(false, true)) {
            try {
                GlobalEventExecutor.INSTANCE.execute(SYNC);
            } catch (Throwable t) {
                // Not tried again: the events will not be recorded, as the message says.
                handOffFailed = true;
                PENDING.set(false);
                failed("the registration could not be handed to the GlobalEventExecutor", t);
            }
        }
    }

    /**
     * Registers the events if that was not done and a recorder exists, then adds or removes the periodic hook until
     * it is as wanted. The executor's task, and the tests'; the thread that calls it may wait for another's.
     */
    static synchronized void sync() {
        // Before anything is read: a change from here on queues another one.
        PENDING.set(false);
        if (state == FAILED) {
            return;
        }
        try {
            if (state == NEW) {
                if (!FlightRecorder.isInitialized()) {
                    return;
                }
                // All nine are loaded here, by this thread, before the first is registered: no other thread defines
                // one while the registration holds JFR's metadata lock.
                Class<?>[] classes = {
                        PageStoreMapEvent.class, PageStoreUnmapEvent.class, PageStorePurgeEvent.class,
                        AllocateChunkEvent.class, FreeChunkEvent.class, AllocateBufferEvent.class,
                        ReallocateBufferEvent.class, FreeBufferEvent.class,
                        // The periodic event is registered too: the JDK refuses a hook for a class that is not.
                        PageStoreStateEvent.class};
                for (Class<?> type : classes) {
                    FlightRecorder.register(type.asSubclass(Event.class));
                }
                state = REGISTERED;
            }
            for (;;) {
                boolean want = wanted();
                if (want == hooked) {
                    return;
                }
                if (want) {
                    FlightRecorder.addPeriodicEvent(PageStoreStateEvent.class, HOOK);
                } else {
                    FlightRecorder.removePeriodicEvent(HOOK);
                }
                hooked = want;
            }
        } catch (Throwable t) {
            state = FAILED;
            failed("they could not be registered", t);
        }
    }

    /** Says once per JVM that the events will not be recorded, and why; the stack is for the debug level. */
    private static void failed(String reason, Throwable t) {
        if (WARNED.compareAndSet(false, true)) {
            logger.warn("The page store's JFR events will not be recorded: {}: {}", reason, t.toString());
        }
        logger.debug("Page store JFR events: {}.", reason, t);
    }

    private static void emitAll() {
        boolean pruned = false;
        for (WeakReference<PageStore> ref : STORES) {
            PageStore store = ref.get();
            if (store == null) {
                STORES.remove(ref);
                pruned = true;
            } else {
                PageStoreStateEvent.emit(store);
            }
        }
        if (pruned) {
            // The last open store may have been the one dropped: ask again whether the hook is still wanted.
            poll();
        }
    }
}
