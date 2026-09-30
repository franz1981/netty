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
import java.util.concurrent.locks.StampedLock;

/**
 * The segments one heap has spans in, oldest first, and the rule that packs its spans: the fullest segment with a
 * long enough free run wins (the oldest on a tie), then the {@link #spare}, and a segment is taken from the
 * {@link PageStore} only when none fits. A segment that becomes wholly free leaves the list: it becomes the spare,
 * and the previous spare goes back to the store. The spare goes back too once it stayed unused from one
 * {@link #decay} to the next.
 * <p>
 * Single writer: the holder of the stripe lock, or the thread of a thread-local heap. Chunk creation ({@link #claim}),
 * chunk deallocation ({@link #release}) and decays only. After {@link #markFreed}, releases may come from any thread:
 * the release that empties a segment gives it back to the store, and the list is no longer touched.
 */
final class HeapSegments {
    private final PageStore store;
    /** For assertions only. */
    private final StampedLock stripeLock;
    private final Thread ownerThread;
    /** Where this heap starts looking among the store's regions: spreads equally full regions over the heaps. */
    int regionOffset = System.identityHashCode(this) & Integer.MAX_VALUE;
    // Read by tests and dumps.
    Segment[] segments = new Segment[4];
    int count;
    /** The heap's one wholly free segment, not in {@link #segments}; {@code null} when none. */
    Segment spare;
    /** Whether a decay saw the {@link #spare} already: the next one gives it back. */
    private boolean spareSeenByDecay;
    private volatile boolean freed;
    private int claimedStart = -1;

    HeapSegments(PageStore store, StampedLock stripeLock, Thread ownerThread) {
        assert (stripeLock == null) != (ownerThread == null);
        this.store = store;
        this.stripeLock = stripeLock;
        this.ownerThread = ownerThread;
    }

    boolean isThreadLocal() {
        return ownerThread != null;
    }

    private boolean inOwnerContext() {
        return ownerThread != null ? Thread.currentThread() == ownerThread : stripeLock.isWriteLocked();
    }

    /** Returns the segment of the span; {@link #claimedStart()} is its first slice. Scans the heap's segments once. */
    Segment claim(int slices) {
        assert inOwnerContext() && !freed;
        Segment best = null;
        int bestFree = Integer.MAX_VALUE;
        Segment[] segments = this.segments;
        for (int i = 0, n = count; i < n; i++) {
            Segment segment = segments[i];
            long free = segment.free;
            int freeSlices = Long.bitCount(free);
            if (freeSlices >= slices && freeSlices < bestFree && Segment.firstFit(free, slices) >= 0) {
                best = segment;
                bestFree = freeSlices;
                if (freeSlices == slices) {
                    break;
                }
            }
        }
        if (best == null) {
            best = spare;
            if (best != null) {
                spare = null;
            } else {
                best = store.take(this);
            }
            add(best);
        }
        int start = best.claim(slices);
        assert start >= 0 : best;
        claimedStart = start;
        return best;
    }

    int claimedStart() {
        return claimedStart;
    }

    /** Owner only until {@link #markFreed}, then any thread. */
    void release(Segment segment, int start, int slices) {
        long free = segment.release(start, slices);
        if (free != segment.allFree) {
            return;
        }
        // The chunk's deallocation follows the free() that marked it, so a release on a freed heap sees the flag.
        if (freed) {
            dispose(segment);
            return;
        }
        assert inOwnerContext();
        remove(segment);
        Segment previous = spare;
        spare = segment;
        spareSeenByDecay = false;
        if (previous != null) {
            dispose(previous);
        }
    }

    /**
     * The release that emptied it and {@link #afterFree} may both get here: only the owner CAS winner gives it back.
     */
    private void dispose(Segment segment) {
        if (Segment.OWNER.compareAndSet(segment, this, null)) {
            store.free(segment);
        }
    }

    private void add(Segment segment) {
        if (count == segments.length) {
            segments = Arrays.copyOf(segments, count << 1);
        }
        segments[count++] = segment;
    }

    private void remove(Segment segment) {
        Segment[] segments = this.segments;
        for (int i = 0; i < count; i++) {
            if (segments[i] == segment) {
                System.arraycopy(segments, i + 1, segments, i, count - i - 1);
                segments[--count] = null;
                return;
            }
        }
        throw new IllegalStateException(segment + " is not a segment of this heap");
    }

    /** Call before the heap frees its chunks, then {@link #afterFree}. */
    void markFreed() {
        assert inOwnerContext();
        freed = true;
    }

    /**
     * Gives back the spare and the segments the heap's frees emptied, forgets all; the others go with their last span.
     */
    void afterFree() {
        assert freed;
        if (spare != null) {
            dispose(spare);
            spare = null;
        }
        for (int i = 0; i < count; i++) {
            Segment segment = segments[i];
            // A volatile read after the volatile write of freed: a release that emptied it and read freed as
            // false is seen here (it could not have, see release), one that read it as true disposes of it too.
            if (segment.isWhollyFree()) {
                dispose(segment);
            }
            segments[i] = null;
        }
        count = 0;
    }

    /**
     * Owner only. Gives back the spare if the previous decay saw it already: it stayed unused a whole interval. Then
     * lets the store purge its idle free slots, if due and no other heap's decay is purging.
     */
    void decay(long now) {
        assert inOwnerContext();
        Segment spare = this.spare;
        if (spare != null) {
            if (spareSeenByDecay) {
                this.spare = null;
                dispose(spare);
            } else {
                spareSeenByDecay = true;
            }
        }
        store.purgeIfDue(now);
    }
}
