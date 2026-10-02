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
 * <p>
 * With shared slices ({@link PageStoreConfig#sharesSlices}) the heap holds no region segment: {@link #claim} takes
 * a run of the store's shared slices, and {@link #release} gives it back from any thread, heap freed or not. Only when
 * no region can be mapped does the heap take segments of its own, as without regions.
 */
final class HeapSegments {
    private final PageStore store;
    /** For assertions only. */
    private final StampedLock stripeLock;
    private final Thread ownerThread;
    /** Where this heap starts looking among the store's regions: spreads equally full regions over the heaps. */
    int regionOffset = System.identityHashCode(this) & Integer.MAX_VALUE;
    /** Shared slices: where this heap starts looking among a region's blocks, as mimalloc's thread sequence. */
    final int seq;
    private final boolean sharesSlices;
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
    /** Whether its segments' free slices are purged in place: region segments only. */
    final boolean purgesSlices;
    private long lastPurgeTick = System.nanoTime();
    /** The segments {@link #markEvacuees} marked, until {@link #clearEvacuees}. */
    private Segment[] evacuees = new Segment[4];
    private int evacueeCount;

    HeapSegments(PageStore store, StampedLock stripeLock, Thread ownerThread) {
        assert (stripeLock == null) != (ownerThread == null);
        this.store = store;
        this.stripeLock = stripeLock;
        this.ownerThread = ownerThread;
        reserve = new Segment[store.maxReserveLimit()];
        purgesSlices = store.regionSource != null;
        sharesSlices = store.config.sharesSlices && store.regionSource != null;
        seq = store.nextHeapSequence();
        store.registerHeap(this);
    }

    /** {@link PageStore#STRIPE} or {@link PageStore#THREAD_LOCAL}. */
    String kind() {
        return ownerThread != null ? PageStore.THREAD_LOCAL : PageStore.STRIPE;
    }

    boolean isFreed() {
        return freed;
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
        if (sharesSlices) {
            long run = store.claimSlices(slices, seq, kind());
            if (run >= 0) {
                int perBlock = store.config.slicesPerSegment();
                int slice = (int) run;
                claimedStart = slice % perBlock;
                return store.region((int) (run >>> 32)).block(slice / perBlock);
            }
            // No region can be mapped: segments of the heap's own.
        }
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
            best = reserved != 0 ? takeReservedFor(kind()) : store.take(this);
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

    /** Owner only until {@link #markFreed}, then any thread; any thread for shared slices. */
    void release(Segment segment, int start, int slices) {
        if (segment.sharedSpans != null) {
            store.releaseSlices(segment, start, slices);
            return;
        }
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
        pushReserved(segment);
    }

    private void pushReserved(Segment segment) {
        if (reserved >= Math.min(store.reserveLimit(), reserve.length)) {
            // The oldest, cold if any is, makes room: at a limit of one, the newest wholly free segment is kept.
            disposeOldestReserved(1);
            if (cold > 0) {
                cold--;
            }
        }
        reserve[reserved++] = segment;
        PageStore.givenBack(segment.memoryAddress(), segment.capacity(), 1, regionIndex(segment),
                PageStore.HEAP_RESERVE, kind());
    }

    private Segment takeReservedFor(String heap) {
        Segment segment = takeNewestReserved();
        PageStore.taken(segment.memoryAddress(), segment.capacity(), 1, regionIndex(segment), PageStore.HEAP_RESERVE,
                heap);
        return segment;
    }

    private static int regionIndex(Segment segment) {
        return segment.region != null ? segment.region.index : -1;
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
            segment.evacuate = false;
            // The segment is this thread's alone until the store has it: its large-buffer chunk, kept through the
            // heap's reserve, ends with its stay in the heap. No span is out, and a note left for it finds it retired.
            AdaptivePoolingAllocator.SpanChunk spans = segment.spanChunk;
            if (spans != null) {
                spans.retired = true;
                segment.spanChunk = null;
            }
            // After the heap was freed, any thread: on behalf of no heap.
            store.free(segment, freed ? PageStore.NO_HEAP : kind());
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
     * Owner only, from a decay, once the heap's chunks that hold no buffer added their slices to their segments'
     * {@link Segment#movableSlices}. Marks {@link Segment#evacuate}, sparsest first and never the fullest, each segment
     * whose every used slice is such a chunk, while the segments left keep as many free slices as all the marked ones
     * use: freeing those chunks empties the marked segments, and {@link #claim} packs the chunks their size classes
     * make next into the fullest segments with room. Returns whether it marked any; clears every movable count.
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
        for (int i = 0; i < n; i++) {
            segments[i].movableSlices = 0;
        }
        return evacueeCount != 0;
    }

    /**
     * Owner only: unmarks what {@link #markEvacuees} marked and is still this heap's. One the freed chunks emptied and
     * {@link #dispose} gave back was unmarked there, and may be another heap's by now.
     */
    void clearEvacuees() {
        for (int i = 0; i < evacueeCount; i++) {
            Segment segment = evacuees[i];
            if (segment.owner == this) {
                segment.evacuate = false;
            }
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
        store.unregisterHeap(this);
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
     * first: a reserve of one goes back once it stayed unused a whole interval. Then a {@link #purgeTick}.
     */
    void decay(long now) {
        assert inOwnerContext();
        disposeOldestReserved(cold + 1 >>> 1);
        cold = reserved;
        purgeTick(now);
    }

    /**
     * Owner only, from the heap's decay ticks (see {@code IdleDecay#count}) and decays. At most once per
     * {@link PageStoreConfig#purgeCheckNanos}: purges the idle slices of its region segments, and lets the store
     * purge its idle free slots, if due and no other heap is purging them.
     */
    void purgeTick(long now) {
        assert inOwnerContext();
        if (now - lastPurgeTick < store.config.purgeCheckNanos) {
            return;
        }
        lastPurgeTick = now;
        if (purgesSlices) {
            purgeIdleSlices(now);
        }
        store.purgeIfDue(now);
    }

    /**
     * The memory of the free slices of the heap's region segments released {@link PageStoreConfig#purgeDelayNanos}
     * ago or earlier goes back to the OS, one call per run (see {@link PageStore#purgeSlices}): the heap keeps the
     * segments, and a purged slice claimed again costs page faults only. A slice is purged once until it is claimed
     * again.
     */
    private void purgeIdleSlices(long now) {
        long delay = store.config.purgeDelayNanos;
        Segment[] segments = this.segments;
        for (int i = 0, n = count; i < n; i++) {
            Segment segment = segments[i];
            if (segment.region == null) {
                continue; // allocated on its own: nothing to purge in place
            }
            long idle = 0;
            for (long bits = segment.free & segment.resident; bits != 0; bits &= bits - 1) {
                int slice = Long.numberOfTrailingZeros(bits);
                if (now - segment.freedAt[slice] >= delay) {
                    idle |= 1L << slice;
                }
            }
            if (idle != 0) {
                segment.resident &= ~store.purgeSlices(segment, idle);
            }
        }
    }
}
