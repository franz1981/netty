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
 * A block of a {@link Region}: memory cut into equal slices, at most {@link Long#SIZE}, with one bit per slice in
 * {@link #free}. It has no owner. Any thread claims a span, consecutive free slices, with one CAS on {@link #free}
 * ({@link #claimRun}) and frees it with one ({@link #releaseRun}); between the two, the claimer alone touches those
 * slices' bits of {@link #committed} and their {@link #freedAt} and {@link #everCommitted}. The purger claims free
 * slices the same way before it purges them. A block holds spans of one length, its {@link #bin}: see
 * {@link PageStore#binOf}.
 */
final class Segment {
    private static final AtomicLongFieldUpdater<Segment> FREE =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "free");
    private static final AtomicLongFieldUpdater<Segment> COMMITTED =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "committed");
    /** Set in what {@link #claimRun} returns when the block was empty: this claim set its {@link #bin}. */
    static final int FIRST = 1 << 8;
    /** The mask of the span's first slice in what {@link #claimRun} returns. */
    static final int START = Long.SIZE - 1;

    final AbstractByteBuf buffer;
    /** Where this block starts in {@link #buffer}, the region's. */
    final int base;
    final int sliceSize;
    final int slices;
    /** {@link #free} with every slice free: the low {@link #slices} bits. */
    final long allFree;
    /** Bit {@code i} set when slice {@code i} is free. Changed only by CAS; the one truth about the block. */
    volatile long free;
    /**
     * Bit {@code i} set when slice {@code i} has memory behind it: set on the first claim of the slice, cleared by
     * the purger. Each holder changes its own slices' bits only, by CAS, since holders of other slices race it.
     */
    volatile long committed;
    final Region region;
    final int slot;
    /** Per slice, its holder only: when it was last freed, for the purge delay. */
    final long[] freedAt;
    /**
     * Per slice, its holder only: whether it ever had memory behind it, so a free slice with none now is known to
     * have been purged ({@link #sliceState}).
     */
    final boolean[] everCommitted;
    /**
     * The bin of the first span claimed since the block was last empty: see {@link PageStore#binOf}. A hint, not
     * CASed: written by that claim after the CAS that found the block empty, so a claim racing it may still read
     * the previous bin.
     */
    volatile byte bin;
    /**
     * The chunk every span of a buffer above the size classes belongs to, when a stripe claimed it: one object per
     * block, with no state of its own, so such a buffer costs no chunk object.
     */
    final AdaptivePoolingAllocator.Chunk sharedSpans;
    /** As {@link #sharedSpans}, for the spans of thread-local heaps: so the JFR events tell them apart. */
    final AdaptivePoolingAllocator.Chunk threadLocalSpans;

    Segment(AbstractByteBuf buffer, int base, int size, int sliceSize, Region region, int slot,
            boolean committed, long committedAt) {
        this.region = region;
        this.slot = slot;
        this.base = base;
        int slices = size / sliceSize;
        assert slices > 0 && slices <= Long.SIZE && size == slices * sliceSize;
        this.buffer = buffer;
        this.sliceSize = sliceSize;
        this.slices = slices;
        allFree = slices == Long.SIZE ? -1L : (1L << slices) - 1;
        free = allFree;
        freedAt = new long[slices];
        everCommitted = new boolean[slices];
        if (committed) {
            this.committed = allFree;
            Arrays.fill(freedAt, committedAt);
            Arrays.fill(everCommitted, true);
        }
        sharedSpans = new AdaptivePoolingAllocator.SharedSpanChunk(this, region.store, false);
        threadLocalSpans = new AdaptivePoolingAllocator.SharedSpanChunk(this, region.store, true);
    }

    /**
     * The first of the lowest {@code n} consecutive free slices in {@code free}, or -1: after {@code k} rounds of
     * {@code m &= m >>> 1}, bit {@code i} of {@code m} is set when slices {@code i} to {@code i + k} are all free.
     */
    static int firstFit(long free, int n) {
        long m = free;
        for (int i = 1; i < n; i++) {
            m &= m >>> 1;
        }
        return m == 0 ? -1 : Long.numberOfTrailingZeros(m);
    }

    static boolean hasFit(long free, int n) {
        return firstFit(free, n) >= 0;
    }

    boolean hasFit(int n) {
        return hasFit(free, n);
    }

    private static long mask(int start, int n) {
        assert n > 0 && n < Long.SIZE && start >= 0 && start + n <= Long.SIZE;
        return (1L << n) - 1 << start;
    }

    /** The bits of slices {@code start} to {@code start + n - 1}. */
    long bits(int start, int n) {
        return n == slices ? allFree : mask(start, n);
    }

    /**
     * Any thread: claims the lowest {@code n} consecutive free slices (a "run", the span) with one CAS, for a span of
     * {@code bin}. Returns the first slice, or'ed with {@link #FIRST} when the block was empty, or -1 when no
     * {@code n} consecutive slices are free.
     */
    int claimRun(int n, int bin) {
        if (n == slices) {
            if (!claimWhole()) {
                return -1;
            }
            this.bin = (byte) bin;
            return FIRST;
        }
        for (;;) {
            long current = free;
            int start = firstFit(current, n);
            if (start < 0) {
                return -1;
            }
            if (FREE.compareAndSet(this, current, current & ~mask(start, n))) {
                if (current != allFree) {
                    return start;
                }
                this.bin = (byte) bin;
                return start | FIRST;
            }
        }
    }

    /** Any thread: claims the first {@code n} slices if the block is empty, making it {@code bin}'s. */
    boolean claimFirst(int n, int bin) {
        if (!FREE.compareAndSet(this, allFree, allFree & ~bits(0, n))) {
            return false;
        }
        this.bin = (byte) bin;
        return true;
    }

    /** Any thread: claims every slice if the block is empty. */
    boolean claimWhole() {
        return FREE.compareAndSet(this, allFree, 0);
    }

    /** Purger: claims those of {@code bits} that are free, and returns them; a claim that raced it keeps its own. */
    long claimFree(long bits) {
        for (;;) {
            long current = free;
            long claimed = current & bits;
            if (claimed == 0 || FREE.compareAndSet(this, current, current & ~claimed)) {
                return claimed;
            }
        }
    }

    /**
     * Any thread holding the span of {@code n} slices from {@code start}: stamps them freed at {@code now}, frees
     * them with one CAS and arms the purge. Throws if one is free already.
     */
    void releaseRun(int start, int n, long now) {
        long[] freedAt = this.freedAt;
        for (int i = start; i < start + n; i++) {
            freedAt[i] = now;
        }
        unclaim(bits(start, n));
        region.store.armPurge(now);
    }

    /** The holder of {@code bits}: frees them with one CAS, then lets the store set the block's bit in its maps. */
    void unclaim(long bits) {
        for (;;) {
            long current = free;
            if ((current & bits) != 0) {
                throw new IllegalStateException("slices " + Long.toHexString(current & bits) + " are already free");
            }
            if (FREE.compareAndSet(this, current, current | bits)) {
                region.store.slicesReleased(this, current | bits);
                return;
            }
        }
    }

    /** Of {@code bits}, those with no memory behind them. */
    long fresh(long bits) {
        return bits & ~committed;
    }

    /** The holder of {@code bits}: memory is behind them now. */
    void commit(long bits) {
        long current;
        do {
            current = committed;
        } while (!COMMITTED.compareAndSet(this, current, current | bits));
        for (long b = bits; b != 0; b &= b - 1) {
            everCommitted[Long.numberOfTrailingZeros(b)] = true;
        }
    }

    /** The purger, holding {@code bits}: their memory was given back. */
    void uncommit(long bits) {
        for (;;) {
            long current = committed;
            if (COMMITTED.compareAndSet(this, current, current & ~bits)) {
                return;
            }
        }
    }

    /** Counts and clears every committed slice, for the region's release: only once nothing else touches the block. */
    int takeCommitted() {
        int n = Long.bitCount(committed);
        committed = 0;
        return n;
    }

    /**
     * Purger: of {@code bits}, those committed and freed {@code delay} or more before {@code now}. The rest are
     * reported to {@link PageStore#skip}, so the next pass comes when they are due. Racy on slices the purger does
     * not hold; it holds them before the call that decides.
     */
    long idleOf(long bits, long now, long delay) {
        long idle = 0;
        for (long b = bits & committed; b != 0; b &= b - 1) {
            int slice = Long.numberOfTrailingZeros(b);
            long waited = now - freedAt[slice];
            if (waited >= delay) {
                idle |= 1L << slice;
            } else {
                region.store.skip(waited);
            }
        }
        return idle;
    }

    /** How long ago the slice freed last was freed. Racy unless the caller holds every slice. */
    long shortestWait(long now) {
        long shortest = Long.MAX_VALUE;
        for (long freedAt : this.freedAt) {
            shortest = Math.min(shortest, now - freedAt);
        }
        return shortest;
    }

    long address() {
        return buffer._memoryAddress() + base;
    }

    static final int SLICE_OUT = 0;
    static final int SLICE_COMMITTED = 1;
    static final int SLICE_PURGED = 2;
    static final int SLICE_UNTOUCHED = 3;

    /** Of slice {@code i}, for the JFR state event: claimed, free with memory behind it, purged, or never used. */
    int sliceState(int i) {
        long bit = 1L << i;
        if ((free & bit) == 0) {
            return SLICE_OUT;
        }
        if ((committed & bit) != 0) {
            return SLICE_COMMITTED;
        }
        return everCommitted[i] ? SLICE_PURGED : SLICE_UNTOUCHED;
    }

    boolean isEmpty() {
        return free == allFree;
    }

    @Override
    public String toString() {
        return "Segment[slices: " + slices + ", used: " + (slices - Long.bitCount(free)) + ']';
    }
}
