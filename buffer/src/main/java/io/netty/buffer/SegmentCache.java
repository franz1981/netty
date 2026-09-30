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
 * The wholly free segments of an allocator, which any of its heaps takes before a new one from the
 * {@link SegmentSource}: a stack, newest on top, bounded by {@link PageStoreConfig#segmentCacheBytes}; a segment
 * beyond the bound (every one, with a bound of 0) is freed at once. Taken from and given to only when a heap takes
 * a segment or one leaves its heap, never per buffer.
 * <p>
 * It ages like the recycler of a heap: at most once per {@link PageStoreConfig#decayIntervalNanos}, run by the
 * decay of whichever heap comes first after the interval, it frees {@link PageStoreConfig#decayFraction} (half by
 * default), rounded up, of the segments that stayed in it through the whole interval, oldest first. Taking from
 * the top leaves the bottom untouched, so the fewest segments it held since the last decay are exactly those.
 * <p>
 * Guarded by its monitor: the operations are a few field writes, one per segment taken or given up, and the aging
 * has to take the oldest segments from the bottom, which a lock-free stack cannot.
 */
final class SegmentCache {
    private static final AtomicLongFieldUpdater<SegmentCache> LAST_DECAY_NANOS =
            AtomicLongFieldUpdater.newUpdater(SegmentCache.class, "lastDecayNanos");

    private final PageStore store;
    private final long decayIntervalNanos;
    private final Segment[] stack;
    private int size;
    /** The fewest segments held since the last decay: the bottom ones up to it were not taken since. */
    private int coldCount;
    private boolean closed;
    private volatile long lastDecayNanos = System.nanoTime();
    // Counters, for dumps and tests: segments taken from here, given to here and kept, freed by the bound, the
    // aging or the close.
    long taken;
    long returned;
    long freed;

    SegmentCache(PageStore store, int capacity) {
        this.store = store;
        decayIntervalNanos = store.config.decayIntervalNanos;
        stack = new Segment[Math.max(0, capacity)];
    }

    /** The newest segment, or {@code null}. */
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

    /** Keep {@code segment}, wholly free and owned by no heap, or free it when the cache is full. */
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

    /** Age the cache if no decay did during the last interval: any heap's decay calls this. */
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

    /** Free every segment, and every one offered from now on; for an allocator being freed. */
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
