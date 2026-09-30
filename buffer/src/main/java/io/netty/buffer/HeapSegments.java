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
 * long enough free run wins (the oldest on a tie), then the newest segment of the {@link #reserve}, and a segment is
 * taken from the {@link PageStore} only when none fits. A segment that becomes wholly free leaves the list for the
 * reserve, whole: taken again, it costs no allocation. The reserve holds {@link PageStore#reserveLimit} segments at
 * most: when full, its oldest goes back to the store. Each {@link #decay} gives back, oldest first, half (rounded up)
 * of the segments that stayed in the reserve since the previous one, as mimalloc does with its per-heap segment
 * reserve.
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
    /** Wholly free segments, not in {@link #segments}, oldest first. */
    final Segment[] reserve;
    int reserved;
    /** How many of the oldest reserved segments stayed unused since the previous decay. */
    private int cold;
    private volatile boolean freed;
    private int claimedStart = -1;
    /** The segments {@link #markEvacuees} marked, until {@link #clearEvacuees}. */
    private Segment[] evacuees = new Segment[4];
    private int evacueeCount;
    /** The evacuee given back at once when it empties, instead of reserved; see {@link #markEvacuees}. */
    private Segment renewed;

    HeapSegments(PageStore store, StampedLock stripeLock, Thread ownerThread) {
        assert (stripeLock == null) != (ownerThread == null);
        this.store = store;
        this.stripeLock = stripeLock;
        this.ownerThread = ownerThread;
        reserve = new Segment[store.maxReserveLimit()];
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
            best = reserved != 0 ? takeNewestReserved() : store.take(this);
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
        if (segment == renewed) {
            renewed = null;
            dispose(segment);
            return;
        }
        if (reserved >= Math.min(store.reserveLimit(), reserve.length)) {
            // The oldest, cold if any is, makes room: at a limit of one, the newest wholly free segment is kept.
            disposeOldestReserved(1);
            if (cold > 0) {
                cold--;
            }
        }
        reserve[reserved++] = segment;
    }

    private Segment takeNewestReserved() {
        Segment segment = reserve[--reserved];
        reserve[reserved] = null;
        cold = Math.min(cold, reserved);
        return segment;
    }

    private void disposeOldestReserved(int n) {
        Segment[] reserve = this.reserve;
        for (int i = 0; i < n; i++) {
            dispose(reserve[i]);
        }
        int left = reserved - n;
        System.arraycopy(reserve, n, reserve, 0, left);
        Arrays.fill(reserve, left, reserved, null);
        reserved = left;
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
        segment.freeAtDecay = 0;
        segment.claimedSinceDecay = 0;
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
     * Owner only, from a decay, once the heap's chunks that hold no buffer added their slices to their segments'
     * {@link Segment#movableSlices}. Marks {@link Segment#evacuate}, sparsest first and never the fullest, each segment
     * whose every used slice is such a chunk, while the segments left keep as many free slices as all the marked ones
     * use: freeing those chunks empties the marked segments, and {@link #claim} packs the chunks their size classes
     * make next into the fullest segments with room. Returns whether it marked any; clears every movable count.
     * <p>
     * Without regions no free slice is purged in place (see {@link #purgeIdleSlices}): a heap's last segment keeps
     * every page a burst touched in it. When the reserve is empty, nothing in that segment holds a buffer, and more of
     * its free slices were claimed once than it uses, it is marked too, and given back at once when it empties: the
     * heap's next chunk is made in a new segment, whose pages cost nothing until touched.
     */
    boolean markEvacuees() {
        assert inOwnerContext() && !freed && evacueeCount == 0;
        Segment[] segments = this.segments;
        int n = count;
        int fullest = 0;
        int room = 0;
        for (int i = 0; i < n; i++) {
            room += segments[i].freeSlices();
            if (segments[i].usedSlices() > segments[fullest].usedSlices()) {
                fullest = i;
            }
        }
        int moved = 0;
        for (;;) {
            Segment sparsest = null;
            for (int i = 0; i < n; i++) {
                Segment segment = segments[i];
                if (i != fullest && !segment.evacuate && segment.movableSlices == segment.usedSlices()
                        && (sparsest == null || segment.usedSlices() < sparsest.usedSlices())) {
                    sparsest = segment;
                }
            }
            // The room left must hold what moves: room - free >= moved + used, that is room >= moved + slices.
            if (sparsest == null || room < moved + sparsest.slices) {
                break;
            }
            room -= sparsest.freeSlices();
            moved += sparsest.usedSlices();
            sparsest.evacuate = true;
            if (evacueeCount == evacuees.length) {
                evacuees = Arrays.copyOf(evacuees, evacueeCount << 1);
            }
            evacuees[evacueeCount++] = sparsest;
        }
        if (n == 1 && reserved == 0) {
            Segment last = segments[0];
            int used = last.usedSlices();
            if (last.region == null && last.movableSlices == used && Long.bitCount(last.resident & last.free) > used) {
                renewed = last;
                last.evacuate = true;
                evacuees[evacueeCount++] = last;
            }
        }
        for (int i = 0; i < n; i++) {
            segments[i].movableSlices = 0;
        }
        return evacueeCount != 0;
    }

    /** Owner only: unmarks what {@link #markEvacuees} marked. */
    void clearEvacuees() {
        renewed = null;
        for (int i = 0; i < evacueeCount; i++) {
            evacuees[i].evacuate = false;
            evacuees[i] = null;
        }
        evacueeCount = 0;
    }

    /** Call before the heap frees its chunks, then {@link #afterFree}. */
    void markFreed() {
        assert inOwnerContext();
        freed = true;
    }

    /**
     * Gives back the reserve and the segments the heap's frees emptied, forgets all; the others go with their last
     * span.
     */
    void afterFree() {
        assert freed;
        disposeOldestReserved(reserved);
        cold = 0;
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
     * Owner only. Gives back half, rounded up, of the reserved segments unused since the previous decay, oldest
     * first: a reserve of one goes back once it stayed unused a whole interval. Then purges the idle slices of its
     * region segments, and lets the store purge its idle free slots, if due and no other heap's decay is purging.
     */
    void decay(long now) {
        assert inOwnerContext();
        disposeOldestReserved(cold + 1 >>> 1);
        cold = reserved;
        if (store.regionSource != null) {
            purgeIdleSlices();
        }
        store.purgeIfDue(now);
    }

    /**
     * The memory of the free slices of the heap's region segments that stayed free, unclaimed, since the previous
     * decay goes back to the OS, one call per run (see {@link PageStore#purgeSlices}): the heap keeps the segments,
     * and a purged slice claimed again costs page faults only. A slice is purged once until it is claimed again.
     */
    private void purgeIdleSlices() {
        Segment[] segments = this.segments;
        for (int i = 0, n = count; i < n; i++) {
            Segment segment = segments[i];
            if (segment.region == null) {
                continue; // allocated on its own: nothing to purge in place
            }
            long free = segment.free;
            long idle = free & segment.freeAtDecay & ~segment.claimedSinceDecay & segment.resident;
            segment.freeAtDecay = free;
            segment.claimedSinceDecay = 0;
            if (idle != 0) {
                segment.resident &= ~store.purgeSlices(segment, idle);
            }
        }
    }
}
