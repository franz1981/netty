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
 * The segments of one heap (a stripe, or a thread-local heap), in the order the heap took them, and the rules that
 * pack its spans: a span is claimed from the fullest segment that has room for it (the oldest of equally full
 * ones), first fit from the lowest slice in it, and a new segment is taken only when none has room. Filling the
 * fullest segments first leaves the emptiest ones to empty, as tcmalloc's filler and mimalloc's page queues do;
 * a segment that becomes wholly free leaves the heap at once, for the allocator's {@link SegmentCache}.
 * <p>
 * Guarded like the heap: by the stripe lock, or by the owner thread of a thread-local heap. Nothing here runs per
 * buffer: only chunk creation (a claim) and chunk deallocation (a release) come here, and both run in the heap's
 * own slow paths. Once the heap is freed ({@link #markFreed}), the chunks still holding spans are deallocated by
 * whichever thread releases their last buffer: from then on a segment is disposed of by the release that empties
 * it, whatever the thread, and the list is not touched again.
 */
final class HeapSegments {
    private final AdaptivePoolingAllocator allocator;
    /** The stripe's lock, or {@code null} on a thread-local heap. For the assertions only. */
    private final StampedLock stripeLock;
    /** The thread of a thread-local heap, or {@code null} on a stripe. */
    private final Thread ownerThread;
    // Visible for testing (and read by dumps): the segments with a span out, oldest first.
    Segment[] segments = new Segment[4];
    int count;
    /** Set once by {@link #markFreed}: the heap is gone, its segments are disposed of by their last release. */
    private volatile boolean freed;
    /** The first slice of the span the last {@link #claim} returned the segment of. */
    private int claimedStart = -1;

    HeapSegments(AdaptivePoolingAllocator allocator, StampedLock stripeLock, Thread ownerThread) {
        assert allocator.segmentSource != null;
        assert (stripeLock == null) != (ownerThread == null);
        this.allocator = allocator;
        this.stripeLock = stripeLock;
        this.ownerThread = ownerThread;
    }

    boolean isThreadLocal() {
        return ownerThread != null;
    }

    private boolean inOwnerContext() {
        return ownerThread != null ? Thread.currentThread() == ownerThread : stripeLock.isWriteLocked();
    }

    /**
     * Claim a span of {@code slices} for a new chunk and return its segment; {@link #claimedStart()} tells where in
     * it. The fullest segment with a free run long enough wins, the oldest on a tie; with none, the heap takes a
     * segment (see {@link AdaptivePoolingAllocator#takeSegment}). Scans the heap's segments once.
     */
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
            best = allocator.takeSegment(this);
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

    /**
     * Free the span of {@code slices} from {@code start} in {@code segment}, one of this heap's, when the chunk it
     * served is deallocated. A segment that becomes wholly free leaves the heap for the {@link SegmentCache}. On a
     * live heap this runs in the owner's context only: a chunk is deallocated there unless its heap was freed.
     */
    void release(Segment segment, int start, int slices) {
        long free = segment.release(start, slices);
        if (free != segment.allFree) {
            return;
        }
        // The chunk's deallocation follows the free() that marked it, so a release on a freed heap sees the flag.
        if (!freed) {
            assert inOwnerContext();
            remove(segment);
        }
        dispose(segment);
    }

    /**
     * Hand {@code segment} to the cache, if it is still this heap's: of the release that emptied it and the sweep
     * of {@link #afterFree}, which may both see it wholly free once the heap was freed, only one gets it.
     */
    private void dispose(Segment segment) {
        if (Segment.OWNER.compareAndSet(segment, this, null)) {
            allocator.segmentCache.offer(segment);
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

    /**
     * The heap is being freed: from now on the release that empties a segment disposes of it, from any thread.
     * Call before the heap frees its chunks, then {@link #afterFree}.
     */
    void markFreed() {
        assert inOwnerContext();
        freed = true;
    }

    /**
     * After the heap freed its chunks: dispose of the segments they emptied, then forget them all; the others go
     * when their last span does (see {@link #release}).
     */
    void afterFree() {
        assert freed;
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

    /** Called by the heap's {@link IdleDecay}: the {@link SegmentCache} ages at most once per interval. */
    void decay(long now) {
        allocator.segmentCache.decayIfDue(now);
    }
}
