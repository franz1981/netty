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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A block: memory cut into equal slices (at most {@link Long#SIZE}), claimed and released as runs of contiguous
 * slices. The state is one bit per slice in {@link #free}, none in the memory: claims are first fit from the lowest
 * slice, and a released run merges with its free neighbours by construction. Chunk creation and deallocation only,
 * never per buffer.
 * <p>
 * A block of a {@link Region} has no owner: any thread claims and releases runs of its slices by CAS
 * ({@link #claimRun}, {@link #releaseRun}), and a cleared bit belongs to the thread that cleared it, which alone
 * touches that slice's {@link #freedAt}; the CAS that sets the bit again publishes it.
 */
final class Segment {
    private static final AtomicLongFieldUpdater<Segment> FREE =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "free");

    final AbstractByteBuf buffer;
    final int sliceSize;
    final int slices;
    final long allFree;
    /** Bit {@code i} set when slice {@code i} is free. */
    volatile long free;
    final Region region;
    final int slot;
    /**
     * Per slice, slice owner only: {@link Region#UNCOMMITTED}, else the {@link System#nanoTime()} of its last
     * release.
     */
    final long[] freedAt;
    /** The chunk of every large-buffer span a stripe claimed in it. Set before its region is published. */
    AdaptivePoolingAllocator.Chunk sharedSpans;
    /** As {@link #sharedSpans}, for the spans of thread-local heaps. */
    AdaptivePoolingAllocator.Chunk threadLocalSpans;

    Segment(AbstractByteBuf buffer, int sliceSize, Region region, int slot) {
        this.region = region;
        this.slot = slot;
        int slices = buffer.capacity() / sliceSize;
        assert slices > 0 && slices <= Long.SIZE && buffer.capacity() == slices * sliceSize;
        this.buffer = buffer;
        this.sliceSize = sliceSize;
        this.slices = slices;
        allFree = slices == Long.SIZE ? -1L : (1L << slices) - 1;
        free = allFree;
        freedAt = new long[slices];
    }

    /**
     * The first slice of the lowest run of {@code n} free slices in {@code free}, or -1. After {@code k} rounds of
     * {@code m &= m >>> 1}, bit {@code i} of {@code m} is set when slices {@code i} to {@code i + k} are all free.
     */
    static int firstFit(long free, int n) {
        long m = free;
        for (int i = 1; i < n; i++) {
            m &= m >>> 1;
        }
        return m == 0 ? -1 : Long.numberOfTrailingZeros(m);
    }

    private static long mask(int start, int n) {
        assert n > 0 && n < Long.SIZE && start >= 0 && start + n <= Long.SIZE;
        return (1L << n) - 1 << start;
    }

    /** Any thread: claims the lowest run of {@code n} free slices. Returns its first slice, or -1. */
    int claimRun(int n) {
        if (n == slices) {
            return claimWhole() ? 0 : -1;
        }
        for (;;) {
            long current = free;
            int start = firstFit(current, n);
            if (start < 0) {
                return -1;
            }
            if (FREE.compareAndSet(this, current, current & ~mask(start, n))) {
                return start;
            }
        }
    }

    /** Claims every slice if all are free. */
    boolean claimWhole() {
        return FREE.compareAndSet(this, allFree, 0);
    }

    /** Claims the slices of {@code bits} that are still free, and returns them. */
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
     * The run of {@code n} slices from {@code start}, which the caller claimed, is free again, its
     * slices with memory behind them stamped with {@code now} first. Throws if any of them is free already.
     */
    void releaseRun(int start, int n, long now) {
        long[] freedAt = this.freedAt;
        for (int i = start; i < start + n; i++) {
            if (freedAt[i] != Region.UNCOMMITTED) {
                freedAt[i] = now;
            }
        }
        giveBack(n == slices ? allFree : mask(start, n));
    }

    /** The slices of {@code bits}, which the caller claimed, are free again, unstamped. */
    void giveBack(long bits) {
        for (;;) {
            long current = free;
            if ((current & bits) != 0) {
                throw new IllegalStateException("slices " + Long.toHexString(current & bits) + " are already free");
            }
            if (FREE.compareAndSet(this, current, current | bits)) {
                return;
            }
        }
    }

    boolean isWhollyFree() {
        return free == allFree;
    }

    int freeSlices() {
        return Long.bitCount(free);
    }

    int usedSlices() {
        return slices - freeSlices();
    }

    @Override
    public String toString() {
        return "Segment[slices: " + slices + ", used: " + usedSlices() + ']';
    }
}
