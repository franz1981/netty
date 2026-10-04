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
 * A block: memory cut into equal slices (at most {@link Long#SIZE}), claimed and released as runs of contiguous
 * slices, one bit per slice in {@link #free}. A block has no owner: any thread claims and releases runs by CAS
 * ({@link #claimRun}, {@link #releaseRun}); a cleared bit belongs to the thread that cleared it, which alone touches
 * that slice's bit of {@link #committed} and its {@link #freedAt}, until the CAS that sets the bit again.
 */
final class Segment {
    private static final AtomicLongFieldUpdater<Segment> FREE =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "free");
    private static final AtomicLongFieldUpdater<Segment> COMMITTED =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "committed");
    /** Set in what {@link #claimRun} returns when the block was empty: the claim labelled it. */
    static final int FIRST = 1 << 8;
    /** The first slice of a run, in what {@link #claimRun} returns. */
    static final int START = Long.SIZE - 1;

    final AbstractByteBuf buffer;
    final int base;
    final int sliceSize;
    final int slices;
    final long allFree;
    /** Bit {@code i} set when slice {@code i} is free. */
    volatile long free;
    /** Bit {@code i} set when slice {@code i} has memory behind it; each holder changes its own bits only, by CAS. */
    volatile long committed;
    final Region region;
    final int slot;
    /** Per slice, slice owner only: the {@link System#nanoTime()} of its last release. */
    final long[] freedAt;
    /** Per slice, slice owner only: whether it ever had memory behind it, so that free with none now was purged. */
    final boolean[] everCommitted;
    /** The bin of the first claim since the block was last empty (see {@link PageStore#binOf}): a hint. */
    volatile byte bin;
    /** The chunk of every large-buffer span a stripe claimed in it. */
    final AdaptivePoolingAllocator.Chunk sharedSpans;
    /** As {@link #sharedSpans}, for the spans of thread-local heaps. */
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
     * The first slice of the lowest run of {@code n} free slices in {@code free}, or -1: after {@code k} rounds of
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

    long bits(int start, int n) {
        return n == slices ? allFree : mask(start, n);
    }

    /** Any thread: claims the lowest run of {@code n} free slices, for a claim of {@code bin}, or -1. */
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

    boolean claimFirst(int n, int bin) {
        if (!FREE.compareAndSet(this, allFree, allFree & ~bits(0, n))) {
            return false;
        }
        this.bin = (byte) bin;
        return true;
    }

    boolean claimWhole() {
        return FREE.compareAndSet(this, allFree, 0);
    }

    long claimFree(long bits) {
        for (;;) {
            long current = free;
            long claimed = current & bits;
            if (claimed == 0 || FREE.compareAndSet(this, current, current & ~claimed)) {
                return claimed;
            }
        }
    }

    /** The run of {@code n} slices from {@code start}, claimed by the caller, is free again. Throws if free already. */
    void releaseRun(int start, int n, long now) {
        long[] freedAt = this.freedAt;
        for (int i = start; i < start + n; i++) {
            freedAt[i] = now;
        }
        unclaim(bits(start, n));
        region.store.armPurge(now);
    }

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

    long fresh(long bits) {
        return bits & ~committed;
    }

    void commit(long bits) {
        long current;
        do {
            current = committed;
        } while (!COMMITTED.compareAndSet(this, current, current | bits));
        for (long b = bits; b != 0; b &= b - 1) {
            everCommitted[Long.numberOfTrailingZeros(b)] = true;
        }
    }

    void uncommit(long bits) {
        for (;;) {
            long current = committed;
            if (COMMITTED.compareAndSet(this, current, current & ~bits)) {
                return;
            }
        }
    }

    /** Every slice still committed, as a count, and uncommitted: only once nothing else touches this block. */
    int takeCommitted() {
        int n = Long.bitCount(committed);
        committed = 0;
        return n;
    }

    /** Of {@code bits}, those committed and freed {@code delay} or more before {@code now}; the rest skip the purge. */
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

    /** Of slice {@code i}: claimed, free with memory behind it, purged since used, or never used. */
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
