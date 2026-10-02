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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Memory cut into equal slices (at most {@link Long#SIZE}), claimed and released as spans of contiguous slices. The
 * state is one bit per slice in {@link #free}, none in the memory: claims are first fit from the lowest slice, and a
 * released span merges with its free neighbours by construction.
 * <p>
 * Single writer: the {@link #owner} heap claims spans and releases them; their chunks read {@link #buffer} itself.
 * Once that heap is freed, the last releases of its spans may come from any thread, hence the CAS on {@link #free}.
 * Chunk creation and deallocation only, never per buffer.
 * <p>
 * A block of a region's shared slices (see {@link PageStore}) has no owner: any thread claims and releases runs of
 * its slices by CAS ({@link #claimRun}, {@link #releaseRun}), and a cleared bit belongs to the thread that cleared it,
 * which alone touches that slice's {@link #freedAt}; the CAS that sets the bit again publishes it.
 */
final class Segment implements ChunkInfo {
    private static final AtomicLongFieldUpdater<Segment> FREE =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "free");
    static final AtomicReferenceFieldUpdater<Segment, HeapSegments> OWNER =
            AtomicReferenceFieldUpdater.newUpdater(Segment.class, HeapSegments.class, "owner");

    final AbstractByteBuf buffer;
    final int sliceSize;
    final int slices;
    final long allFree;
    /** Bit {@code i} set when slice {@code i} is free. */
    volatile long free;
    /** {@code null} once given back to the {@link PageStore}, or on its way there. */
    volatile HeapSegments owner;
    /** {@code null} for a segment allocated on its own. */
    final Region region;
    final int slot;
    /** By first slice: a span claimed again with the same length reuses its buffer: re-creating a chunk is GC-free. */
    // Owner only, for the purge of idle slices (see HeapSegments#purgeTick).
    /**
     * Slices that may have memory behind them: claimed since the last purge of their memory, or, for a segment
     * allocated on its own, since its allocation.
     */
    long resident;
    /** Per slice, the {@link System#nanoTime()} of its last release. */
    final long[] freedAt;
    // Owner only, within one HeapSegments#markEvacuees round.
    /** Slices of the owner's chunks in this segment that hold no buffer. */
    int movableSlices;
    /** Whether the owner frees its chunks here that hold no buffer, which empties this segment. */
    boolean evacuate;
    /**
     * The chunk of the large-buffer spans made here since the segment joined its heap, or {@code null}: set by the
     * owner, dropped by whichever thread gives the segment back to the store (see {@link HeapSegments#dispose}).
     */
    AdaptivePoolingAllocator.SpanChunk spanChunk;
    /**
     * A block of shared slices only, else {@code null}: the chunk of every large-buffer span a stripe claimed in it.
     * Set before its region is published.
     */
    AdaptivePoolingAllocator.Chunk sharedSpans;
    /** As {@link #sharedSpans}, for the spans of thread-local heaps. */
    AdaptivePoolingAllocator.Chunk threadLocalSpans;

    Segment(AbstractByteBuf buffer, int sliceSize) {
        this(buffer, sliceSize, null, -1);
    }

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

    /**
     * Its whole memory was used, and is released at {@code now}, by a chunk or buffer that took the segment whole:
     * every slice may have memory behind it, and is purged in place a whole delay from now at the earliest. By the
     * thread that owns the segment alone: the one giving it back.
     */
    void releasedWhole(long now) {
        resident = allFree;
        Arrays.fill(freedAt, now);
    }

    /** The first slice of the lowest run of {@code n} free slices, now claimed, or -1. */
    int claim(int n) {
        for (;;) {
            long current = free;
            int start = firstFit(current, n);
            if (start < 0) {
                return -1;
            }
            long bits = mask(start, n);
            if (FREE.compareAndSet(this, current, current & ~bits)) {
                resident |= bits;
                return start;
            }
        }
    }

    /** Returns {@link #free} after. Throws if any of the slices is not claimed. */
    long release(int start, int n) {
        long bits = mask(start, n);
        for (;;) {
            long current = free;
            if ((current & bits) != 0) {
                throw new IllegalStateException("slices " + start + ".." + (start + n - 1) + " are not claimed: "
                        + Long.toHexString(current));
            }
            long next = current | bits;
            if (FREE.compareAndSet(this, current, next)) {
                long now = System.nanoTime();
                for (int i = start; i < start + n; i++) {
                    freedAt[i] = now;
                }
                return next;
            }
        }
    }

    /**
     * Shared slices: claims the lowest run of {@code n} free slices, from any thread. Returns its first slice, or -1.
     */
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

    /** Shared slices: claims every slice if all are free. */
    boolean claimWhole() {
        return FREE.compareAndSet(this, allFree, 0);
    }

    /** Shared slices: claims the slices of {@code bits} that are still free, and returns them. */
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
     * Shared slices: the run of {@code n} slices from {@code start}, which the caller claimed, is free again, its
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

    /** Shared slices: the slices of {@code bits}, which the caller claimed, are free again, unstamped. */
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
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public boolean isDirect() {
        return buffer.isDirect();
    }

    @Override
    public long memoryAddress() {
        return buffer._memoryAddress();
    }

    @Override
    public String toString() {
        return "Segment[slices: " + slices + ", used: " + usedSlices() + ']';
    }
}
