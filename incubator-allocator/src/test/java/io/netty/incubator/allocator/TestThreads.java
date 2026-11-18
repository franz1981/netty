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

import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Threads the tests start for one body of work and join at once, so that their phases follow one another. */
final class TestThreads {
    /** A workload that may fail. */
    interface Body {
        void run() throws Exception;
    }

    private TestThreads() {
    }

    /** A started thread: {@link #join()} waits for it and rethrows what it threw. */
    static final class Started {
        private final Thread thread;
        private final AtomicReference<Throwable> failure;

        Started(Thread thread, AtomicReference<Throwable> failure) {
            this.thread = thread;
            this.failure = failure;
        }

        Thread thread() {
            return thread;
        }

        /** Waits for the thread to end and rethrows what it threw past its body, as the first of them. */
        Thread join() throws Exception {
            thread.join();
            Throwable t = failure.get();
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            if (t != null) {
                PlatformDependent.throwException(t);
            }
            return thread;
        }
    }

    /** Runs {@code body} on a new thread, with a heap of its own if {@code fastThreadLocal}, joins it, returns it. */
    static Thread runOnThread(boolean fastThreadLocal, Body body) throws Exception {
        return runOnThread(fastThreadLocal, body, null);
    }

    /** As above; {@code beforeStart} sees the thread before it starts. */
    static Thread runOnThread(boolean fastThreadLocal, final Body body, Consumer<Thread> beforeStart)
            throws Exception {
        return start(fastThreadLocal, body, beforeStart).join();
    }

    /** Starts {@code body} on a new thread and returns at once; see {@link Started#join()}. */
    static Started start(boolean fastThreadLocal, Body body) {
        return start(fastThreadLocal, body, null);
    }

    /**
     * As above; {@code beforeStart} sees the thread before it starts. What the body throws, and what the thread throws
     * after the body (the close of its heap, which happens as it ends), is kept, the first of them, for the join.
     */
    static Started start(boolean fastThreadLocal, final Body body, Consumer<Thread> beforeStart) {
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    body.run();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }
        };
        Thread thread = fastThreadLocal ? new FastThreadLocalThread(task) : new Thread(task);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        if (beforeStart != null) {
            beforeStart.accept(thread);
        }
        thread.start();
        return new Started(thread, failure);
    }
}
