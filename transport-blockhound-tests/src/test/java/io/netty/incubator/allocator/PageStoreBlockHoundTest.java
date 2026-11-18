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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The page store's lock and the heaps' lock are contended by any thread, event loops included. Each test holds the
 * lock on the test thread, and runs the operation on a {@link FastThreadLocalThread}, which BlockHound treats as
 * non-blocking: the operation must park in the method the integration allows, and finish once the lock is free.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PageStoreBlockHoundTest {
    private final MockTicker ticker = Ticker.newMockTicker();

    @BeforeAll
    static void installBlockHound() {
        BlockHound.install();
    }

    @Test
    void addRegionMayBlockOnTheStoreLock() throws Exception {
        AdaptivePoolingAllocator allocator = newAllocator();
        try {
            ReentrantLock lock = storeLock(allocator.pageStore);
            runWhileHeld(lock, "PageStore", "addRegion", () -> {
                allocator.allocate(32, 32).release();
                return null;
            });
        } finally {
            allocator.pageStore.close();
        }
    }

    @Test
    void closeMayBlockOnTheStoreLock() throws Exception {
        AdaptivePoolingAllocator allocator = newAllocator();
        try {
            runWhileHeld(storeLock(allocator.pageStore), "PageStore", "close", () -> {
                allocator.pageStore.close();
                return null;
            });
        } finally {
            allocator.pageStore.close();
        }
    }

    @Test
    void releaseIdleRegionMayBlockOnTheStoreLock() throws Exception {
        AdaptivePoolingAllocator allocator = newAllocator();
        try {
            // a region of one block that stayed free for the purge delay
            ByteBuf whole = allocator.allocate(SEGMENT_SIZE, SEGMENT_SIZE);
            whole.release();
            ticker.advance(2 * allocator.pageStore.config.purgeDelayNanos, TimeUnit.NANOSECONDS);
            long now = ticker.nanoTime();
            runWhileHeld(storeLock(allocator.pageStore), "PageStore", "releaseIdleRegion", () -> {
                allocator.pageStore.purgeIfDue(now);
                return null;
            });
        } finally {
            allocator.pageStore.close();
        }
    }

    @Test
    void closingAStripedHeapMayBlockOnItsLock() throws Exception {
        AdaptivePoolingAllocator allocator = newAllocator();
        try {
            Field stripes = AdaptivePoolingAllocator.class.getDeclaredField("stripes");
            stripes.setAccessible(true);
            AdaptivePoolingAllocator.Heap heap = ((AdaptivePoolingAllocator.Heap[]) stripes.get(allocator))[0];
            long stamp = heap.lock.writeLock();
            try {
                runWhileLocked("AdaptivePoolingAllocator$Heap", "close", () -> {
                    heap.close();
                    return null;
                });
            } finally {
                heap.lock.unlockWrite(stamp);
            }
            awaitDone();
        } finally {
            allocator.pageStore.close();
        }
    }

    /** BlockHound reports a contended lock that no integration allows: the tests above would prove nothing else. */
    @Test
    void aContendedLockOutsideTheAllowListIsReported() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        FutureTask<Void> task = new FutureTask<>(() -> {
            lock.lock();
            lock.unlock();
            return null;
        });
        try {
            Thread thread = new FastThreadLocalThread(task);
            thread.start();
            ExecutionException e = assertThrows(ExecutionException.class, () -> task.get());
            assertInstanceOf(BlockingOperationError.class, e.getCause());
            thread.join();
        } finally {
            lock.unlock();
        }
    }

    private static final int SEGMENT_SIZE = 4 * 1024 * 1024;

    /** Regions of one block of direct memory, as the allocator makes them without mmap. */
    private AdaptivePoolingAllocator newAllocator() {
        MemorySource memory = (initialCapacity, maxCapacity) ->
                Roots.direct(UnpooledByteBufAllocator.DEFAULT, initialCapacity, maxCapacity);
        return new AdaptivePoolingAllocator(memory, true, null,
                new PageStoreConfig(SEGMENT_SIZE, PageStoreConfig.SLICE_SIZE_BYTES,
                        AdaptivePoolingAllocator.Heap.DECAY_INTERVAL_NANOS, 0, 0, SEGMENT_SIZE), ticker);
    }

    private FutureTask<Void> running;
    private Thread runner;

    private static ReentrantLock storeLock(PageStore store) throws Exception {
        Field field = PageStore.class.getDeclaredField("lock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(store);
    }

    private void runWhileHeld(ReentrantLock lock, String type, String method, Callable<Void> operation)
            throws Exception {
        lock.lock();
        try {
            runWhileLocked(type, method, operation);
        } finally {
            lock.unlock();
        }
        awaitDone();
    }

    /** Starts the operation on a non-blocking thread, and returns once it waits inside {@code type.method}. */
    private void runWhileLocked(String type, String method, Callable<Void> operation) throws Exception {
        running = new FutureTask<>(operation);
        runner = new FastThreadLocalThread(running);
        runner.start();
        // Bounded by the class's @Timeout only: the runner parks inside type.method, or ends (a BlockingOperationError,
        // or an operation that never waited).
        boolean parked = false;
        while (!parked && !running.isDone()) {
            parked = parkedIn(type, method);
            if (!parked) {
                Thread.yield();
            }
        }
        if (!parked) {
            running.get(); // rethrows a BlockingOperationError; else the operation never waited
        }
        assertTrue(parked, "the operation must wait inside " + type + "." + method);
    }

    private boolean parkedIn(String type, String method) {
        if (runner.getState() != Thread.State.WAITING) {
            return false;
        }
        for (StackTraceElement frame : runner.getStackTrace()) {
            if (frame.getClassName().equals("io.netty.incubator.allocator." + type)
                    && frame.getMethodName().equals(method)) {
                return true;
            }
        }
        return false;
    }

    private void awaitDone() throws Exception {
        running.get();
        runner.join();
    }
}
