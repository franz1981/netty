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

import io.netty.util.internal.SystemPropertyUtil;
import java.util.concurrent.TimeUnit;

/**
 * The immutable parameters of one allocator's page store, and the direct and heap defaults, read once from the
 * {@code io.netty.allocator.segment*} properties. Read on slow paths only.
 */
final class PageStoreConfig {
    /** The slice of an allocator's {@link Segment}s: 64 KiB. A chunk carved from a segment is whole slices. */
    static final int SLICE_SIZE_BYTES = 64 * 1024;
    /** One bit per slice in one {@code long}: 4 MiB. */
    static final int MAX_SEGMENT_SIZE_BYTES = Long.SIZE * SLICE_SIZE_BYTES;
    /** Room for the largest size class (3 slices) with some to spare. */
    static final int MIN_SEGMENT_SIZE_BYTES = 1024 * 1024;
    /**
     * {@code io.netty.allocator.segmentSize}: a multiple of {@link #SLICE_SIZE_BYTES} from
     * {@link #MIN_SEGMENT_SIZE_BYTES} to {@link #MAX_SEGMENT_SIZE_BYTES}, rounded to the nearest multiple of
     * {@link #REGION_ALIGNMENT_BYTES} so that the segments of a default region always keep its alignment. Default:
     * 4 MiB, 2 MiB in low-memory mode (where nothing else the allocator holds is above 2 MiB either).
     */
    static final int SEGMENT_SIZE_BYTES = segmentSizeOf(SystemPropertyUtil.getInt("io.netty.allocator.segmentSize",
            AdaptivePoolingAllocator.IS_LOW_MEM ? 2 * 1024 * 1024 : MAX_SEGMENT_SIZE_BYTES));
    /** A direct allocator's regions start at a multiple of 2 MiB, so that a THP-enabled kernel can back them whole. */
    static final int REGION_ALIGNMENT_BYTES = 2 * 1024 * 1024;
    /**
     * {@code io.netty.allocator.segmentRegionSize}: the size of the {@code mmap} regions a direct allocator carves
     * its {@link Segment}s out of (see {@link PageStore}), rounded to the nearest multiple of the segment size from 2
     * up to {@link Long#SIZE} segments; 0: none, {@code malloc}'d regions of one segment instead. Default:
     * {@link Long#SIZE} segments (256 MiB with 4 MiB segments, 128 MiB in low-memory mode). Address space only: a
     * segment's pages are committed as they are touched. Regions need an {@link MmapRegionSource}, for the direct
     * allocator; without one, {@link PageStore} falls back to regions of one block.
     */
    static final int SEGMENT_REGION_SIZE_BYTES = regionSizeOf(SystemPropertyUtil.getInt(
            "io.netty.allocator.segmentRegionSize", defaultRegionSize(SEGMENT_SIZE_BYTES)), SEGMENT_SIZE_BYTES);

    /**
     * {@code io.netty.allocator.segmentPurgeDelay}: milliseconds a free slice, or a region of one block, stays free
     * before its memory goes back to the OS, as mimalloc's purge delay times its arena multiplier (1000 ms x 4). From
     * 10 ms to 10 minutes. Default: 4000.
     */
    static final long PURGE_DELAY_MILLIS = purgeDelayMillisOf(
            SystemPropertyUtil.getLong("io.netty.allocator.segmentPurgeDelay", 4000));

    /** The fewest slices of a heap segment: one buffer of the largest size class, 132 KiB, takes 3. */
    static final int MIN_HEAP_SEGMENT_SLICES = 3;

    /**
     * {@code io.netty.allocator.heapSegmentSize}: the size of a heap allocator's segments, rounded down to whole
     * slices, from {@link #MIN_HEAP_SEGMENT_SLICES} to {@link Long#SIZE} of them, which set the page kinds of the
     * size-class chunks (see {@link SizeClassTable#pageKinds}). Default: one slice less than
     * {@link #SEGMENT_SIZE_BYTES}, 4032 KiB: under G1, whose regions are powers of two, its {@code byte[]} with the
     * header fits in whole regions with a slice to spare, and in half a region from 8 MiB regions up (a humongous
     * object takes regions of its own: a 4 MiB array would take two 4 MiB regions, or a whole 8 MiB one).
     */
    static final int HEAP_SEGMENT_SIZE_BYTES = heapSegmentSizeOf(SystemPropertyUtil.getInt(
            "io.netty.allocator.heapSegmentSize", SEGMENT_SIZE_BYTES - SLICE_SIZE_BYTES));

    /** {@code value}, no lower than {@code min} nor higher than {@code max}. */
    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }

    /** {@code value} rounded to the nearest multiple of {@code unit}. */
    private static long round(long value, long unit) {
        return Math.round((double) value / unit) * unit;
    }

    static long purgeDelayMillisOf(long millis) {
        return clamp(millis, 10, 600000);
    }

    /** The largest: one bit per segment in one {@code long}. */
    static int defaultRegionSize(int segmentSize) {
        return Long.SIZE * segmentSize;
    }

    /**
     * 0, or {@code size} rounded to the nearest multiple of {@code segmentSize} from 2 up to {@link Long#SIZE}
     * segments: {@link #SEGMENT_SIZE_BYTES} is always a multiple of {@link #REGION_ALIGNMENT_BYTES}, so the regions
     * this yields always keep it.
     */
    static int regionSizeOf(int size, int segmentSize) {
        if (size == 0) {
            return 0;
        }
        return (int) clamp(round(size, segmentSize), 2L * segmentSize, (long) Long.SIZE * segmentSize);
    }

    /**
     * {@code size} rounded to the nearest multiple of {@link #REGION_ALIGNMENT_BYTES} from
     * {@link #MIN_SEGMENT_SIZE_BYTES} to {@link #MAX_SEGMENT_SIZE_BYTES}. {@link #REGION_ALIGNMENT_BYTES} is itself a
     * multiple of {@link #SLICE_SIZE_BYTES}, so the result is always a valid slice count too.
     */
    static int segmentSizeOf(int size) {
        long min = (MIN_SEGMENT_SIZE_BYTES + REGION_ALIGNMENT_BYTES - 1) / REGION_ALIGNMENT_BYTES
                * (long) REGION_ALIGNMENT_BYTES;
        long max = MAX_SEGMENT_SIZE_BYTES / REGION_ALIGNMENT_BYTES * (long) REGION_ALIGNMENT_BYTES;
        return (int) clamp(round(size, REGION_ALIGNMENT_BYTES), min, max);
    }

    /** {@code size} rounded down to whole slices, from {@link #MIN_HEAP_SEGMENT_SLICES} to {@link Long#SIZE}. */
    static int heapSegmentSizeOf(int size) {
        return (int) clamp(size / SLICE_SIZE_BYTES, MIN_HEAP_SEGMENT_SLICES, Long.SIZE) * SLICE_SIZE_BYTES;
    }

    /** 1 to {@link Long#SIZE} whole slices. */
    final int segmentSize;
    final int sliceSize;
    /** {@code numberOfTrailingZeros(sliceSize)}: a byte offset to a slice index without a division. */
    final int sliceShift;
    /** How long free memory stays before it is purged: see {@link #PURGE_DELAY_MILLIS}. */
    final long purgeDelayNanos;
    /**
     * How often, at most, the store runs a purge pass: a quarter of the delay, so that free memory goes back between
     * one delay and a delay and a quarter after it was freed, while the heaps allocate (see
     * {@link PageStore#purgeIfDue}), unless more is due than passes of {@link PageStore#PURGE_BYTES} give back.
     */
    final long purgeCheckNanos;
    /**
     * 2 to {@link Long#SIZE} whole segments, or {@link #mallocRegionSize} (see {@link #withMallocRegions}); 0: regions
     * of {@link #mallocRegionSize} only.
     */
    final int regionSize;
    /** A power of two, honoured if the region source can; 0: any address. */
    final int regionAlignment;
    /** Where no {@code mmap} region source is had: the size of {@code malloc}'d regions (one block), or 0: none. */
    final int mallocRegionSize;

    /** With regions of {@code regionSize} from the allocator's region source. */
    PageStoreConfig(int segmentSize, int sliceSize, long purgeDelayNanos, int regionSize, int regionAlignment) {
        this(segmentSize, sliceSize, purgeDelayNanos, regionSize, regionAlignment, 0);
    }

    /** With {@code mmap} regions of {@code regionSize}, else {@code malloc}'d ones of {@code mallocRegionSize}. */
    PageStoreConfig(int segmentSize, int sliceSize, long purgeDelayNanos, int regionSize, int regionAlignment,
                    int mallocRegionSize) {
        if (sliceSize <= 0) {
            throw new IllegalArgumentException("sliceSize: " + sliceSize);
        }
        if (segmentSize <= 0 || segmentSize % sliceSize != 0 || segmentSize / sliceSize > Long.SIZE) {
            throw new IllegalArgumentException("segmentSize " + segmentSize + " is not 1 to " + Long.SIZE
                    + " slices of " + sliceSize);
        }
        if (purgeDelayNanos <= 0) {
            throw new IllegalArgumentException("purgeDelayNanos: " + purgeDelayNanos);
        }
        assert (sliceSize & sliceSize - 1) == 0 : "slices of a power of two";
        this.segmentSize = segmentSize;
        this.sliceSize = sliceSize;
        sliceShift = Integer.numberOfTrailingZeros(sliceSize);
        this.purgeDelayNanos = purgeDelayNanos;
        purgeCheckNanos = Math.max(1, purgeDelayNanos >>> 2);
        // A region of one block is a malloc'd one (see withMallocRegions); mmap'd ones hold 2 or more.
        int minBlocks = mallocRegionSize != 0 && regionSize == mallocRegionSize ? 1 : 2;
        if (regionSize != 0 && (regionSize < 0 || regionSize % segmentSize != 0
                || regionSize / segmentSize < minBlocks || regionSize / segmentSize > Long.SIZE)) {
            throw new IllegalArgumentException("regionSize " + regionSize + " is not 0 nor " + minBlocks + " to "
                    + Long.SIZE + " segments of " + segmentSize);
        }
        if (regionAlignment < 0 || (regionAlignment & regionAlignment - 1) != 0) {
            throw new IllegalArgumentException("regionAlignment: " + regionAlignment);
        }
        if (regionSize != 0 && regionAlignment != 0 && segmentSize % regionAlignment != 0) {
            throw new IllegalArgumentException("segments of " + segmentSize + " would not start on "
                    + regionAlignment + " boundaries of their region");
        }
        this.regionSize = regionSize;
        this.regionAlignment = regionAlignment;
        if (mallocRegionSize != 0 && mallocRegionSize != segmentSize) {
            throw new IllegalArgumentException("mallocRegionSize " + mallocRegionSize + " is not 0 nor one segment of "
                    + segmentSize);
        }
        this.mallocRegionSize = mallocRegionSize;
    }

    /** This, with {@code malloc}'d regions of one block each, at libc's alignment. */
    PageStoreConfig withMallocRegions() {
        return new PageStoreConfig(segmentSize, sliceSize, purgeDelayNanos, mallocRegionSize, 0, mallocRegionSize);
    }

    /** Regions of one block, one {@code byte[]} each, from {@link MemorySource#allocate}: nothing to map nor purge. */
    static PageStoreConfig heapDefaults() {
        return new PageStoreConfig(HEAP_SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES,
                TimeUnit.MILLISECONDS.toNanos(PURGE_DELAY_MILLIS), 0, 0, HEAP_SEGMENT_SIZE_BYTES);
    }

    static PageStoreConfig directDefaults() {
        return new PageStoreConfig(SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES,
                TimeUnit.MILLISECONDS.toNanos(PURGE_DELAY_MILLIS), SEGMENT_REGION_SIZE_BYTES, REGION_ALIGNMENT_BYTES,
                SEGMENT_SIZE_BYTES);
    }

    int segmentsPerRegion() {
        return regionSize / segmentSize;
    }

    int slicesPerSegment() {
        return segmentSize / sliceSize;
    }
}
