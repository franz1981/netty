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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A uniform piece of memory from the {@link SegmentSource}, handed out in slices of its allocator's
 * {@link PageStoreConfig#sliceSize} (at most {@link Long#SIZE} of them): a span is a
 * run of contiguous slices. The free slices are one bit each in {@link #free}, so claiming a span, freeing it and
 * merging it with its free neighbours are bit operations (a freed span is simply free bits next to other free
 * bits), and no metadata lives in the segment's memory. Spans are claimed first fit from the lowest slice, so live
 * spans pack towards the start of a segment.
 * <p>
 * A segment belongs to one {@link HeapSegments} at a time ({@link #owner}), which alone claims spans from it; it
 * leaves its heap once wholly free, for the allocator's {@link SegmentCache}. Nothing here runs per buffer: a span
 * is claimed when a chunk is created and freed when it is deallocated.
 */
final class Segment implements ChunkInfo {
    private static final AtomicLongFieldUpdater<Segment> FREE =
            AtomicLongFieldUpdater.newUpdater(Segment.class, "free");
    static final AtomicReferenceFieldUpdater<Segment, HeapSegments> OWNER =
            AtomicReferenceFieldUpdater.newUpdater(Segment.class, HeapSegments.class, "owner");

    final AbstractByteBuf buffer;
    /** The size of a slice, and how many the segment has. */
    final int sliceSize;
    final int slices;
    /** {@link #free} of a wholly free segment: one bit per slice. */
    final long allFree;
    /**
     * Bit {@code i} set when slice {@code i} is free. Changed by compare-and-set: the owner claims and frees
     * spans, and once its heap was freed, the last releases of its chunks free theirs from any thread.
     */
    volatile long free;
    /** The heap whose chunks it holds, or {@code null} in the {@link SegmentCache} or on its way to it. */
    volatile HeapSegments owner;
    /** The region this segment is carved out of, at slot {@link #slot}; {@code null} for a segment of its own. */
    final Region region;
    final int slot;
    /**
     * The span buffers made so far, by first slice: a span claimed again at the same place with the same length
     * reuses its buffer, so that re-creating a chunk allocates no buffer object. At most one per slice.
     */
    private final AbstractByteBuf[] spans;

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
        spans = new AbstractByteBuf[slices];
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

    /** Claim the lowest run of {@code n} free slices and return its first slice, or -1 when there is none. */
    int claim(int n) {
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

    /** Free the {@code n} slices from {@code start}, claimed before; return the free bits after. Any thread. */
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
                return next;
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

    /**
     * The buffer over the {@code n} slices from {@code start}: the one made last time for that exact span, else a
     * new one from {@code source}. Owner only, when a chunk is created.
     */
    AbstractByteBuf span(SegmentSource source, int start, int n) {
        int length = n * sliceSize;
        AbstractByteBuf span = spans[start];
        if (span == null || span.capacity() != length) {
            span = source.span(buffer, start * sliceSize, length);
            spans[start] = span;
        }
        return span;
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
