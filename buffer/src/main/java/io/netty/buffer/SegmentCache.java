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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * The wholly free segments of one allocator, shared by its heaps: a stack, newest on top, of at most
 * {@code segmentCacheBytes / segmentSize} segments. A segment offered beyond that is freed at once.
 * <p>
 * Ageing: at most once per {@code decayIntervalNanos}, run by whichever heap's decay comes first, it frees
 * {@code decayFraction} (rounded up) of the segments that stayed through the whole interval, oldest (bottom) first.
 * <p>
 * Guarded by its monitor; slow paths only: a heap taking a segment, a segment leaving its heap, decays. Freeing a
 * segment may return memory to the OS: outside the monitor on {@link #offer}, inside it on decay and close.
 */
final class SegmentCache {
    private static final AtomicLongFieldUpdater<SegmentCache> LAST_DECAY_NANOS =
            AtomicLongFieldUpdater.newUpdater(SegmentCache.class, "lastDecayNanos");

    private final PageStore store;
    private final long decayIntervalNanos;
    private final Segment[] stack;
    private int size;
    /** The fewest segments held since the last decay: the bottom ones up to it were not touched since. */
    private int coldCount;
    private boolean closed;
    private volatile long lastDecayNanos = System.nanoTime();
    // Read by tests and dumps.
    long taken;
    long returned;
    long freed;

    SegmentCache(PageStore store, int capacity) {
        this.store = store;
        decayIntervalNanos = store.config.decayIntervalNanos;
        stack = new Segment[Math.max(0, capacity)];
    }

    synchronized Segment poll() {
        if (size == 0) {
            return null;
        }
        Segment segment = stack[--size];
        stack[size] = null;
        if (size < coldCount) {
            coldCount = size;
        }
        taken++;
        return segment;
    }

    /** {@code segment} is wholly free and owned by no heap. */
    void offer(Segment segment) {
        assert segment.isWhollyFree() && segment.owner == null;
        synchronized (this) {
            if (!closed && size < stack.length) {
                stack[size++] = segment;
                returned++;
                return;
            }
            freed++;
        }
        store.free(segment);
    }

    /** Any thread; one caller per interval runs {@link #decay}. */
    void decayIfDue(long now) {
        long last = lastDecayNanos;
        if (now - last >= decayIntervalNanos && LAST_DECAY_NANOS.compareAndSet(this, last, now)) {
            decay();
        }
    }

    // Visible for testing.
    synchronized void decay() {
        int free = store.config.toFree(coldCount);
        for (int i = 0; i < free; i++) {
            Segment segment = stack[i];
            freed++;
            store.free(segment);
        }
        if (free > 0) {
            int kept = size - free;
            System.arraycopy(stack, free, stack, 0, kept);
            Arrays.fill(stack, kept, size, null);
            size = kept;
        }
        coldCount = size;
    }

    /** Also frees every segment offered from now on. */
    synchronized void close() {
        closed = true;
        for (int i = 0; i < size; i++) {
            freed++;
            store.free(stack[i]);
            stack[i] = null;
        }
        size = 0;
        coldCount = 0;
    }

    synchronized int size() {
        return size;
    }

    int capacity() {
        return stack.length;
    }
}
