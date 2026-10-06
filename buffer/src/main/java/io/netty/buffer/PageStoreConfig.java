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
 * The immutable parameters of one allocator's page store, read once from the {@code io.netty.allocator.segment*}
 * properties.
 */
final class PageStoreConfig {
    static final int SLICE_SIZE_BYTES = 64 * 1024;
    static final int MAX_SEGMENT_SIZE_BYTES = Long.SIZE * SLICE_SIZE_BYTES;
    /** Room for the largest size class (3 slices) with some to spare. */
    static final int MIN_SEGMENT_SIZE_BYTES = 1024 * 1024;
    /** {@code io.netty.allocator.segmentSize}, rounded to a multiple of {@link #REGION_ALIGNMENT_BYTES}. */
    static final int SEGMENT_SIZE_BYTES = segmentSizeOf(SystemPropertyUtil.getInt("io.netty.allocator.segmentSize",
            AdaptivePoolingAllocator.IS_LOW_MEM ? 2 * 1024 * 1024 : MAX_SEGMENT_SIZE_BYTES));
    /** A direct allocator's regions start at a multiple of 2 MiB, so a THP-enabled kernel can back them whole. */
    static final int REGION_ALIGNMENT_BYTES = 2 * 1024 * 1024;
    /**
     * {@code io.netty.allocator.segmentRegionSize}: the {@code mmap} regions a direct allocator carves its
     * {@link Segment}s from; 0 falls back to one-segment {@code malloc}'d regions. Address space only: pages commit
     * as they are touched. One-segment regions scatter the allocator's memory across the address space; some
     * workloads run measurably faster on larger, contiguous regions.
     */
    static final int SEGMENT_REGION_SIZE_BYTES = regionSizeOf(SystemPropertyUtil.getInt(
            "io.netty.allocator.segmentRegionSize", defaultRegionSize(SEGMENT_SIZE_BYTES)), SEGMENT_SIZE_BYTES);

    /** {@code io.netty.allocator.segmentPurgeDelay}: milliseconds a free slice stays free before it is purged. */
    static final long PURGE_DELAY_MILLIS = purgeDelayMillisOf(
            SystemPropertyUtil.getLong("io.netty.allocator.segmentPurgeDelay", 4000));

    /** The fewest slices of a heap segment: one buffer of the largest size class takes 3. */
    static final int MIN_HEAP_SEGMENT_SLICES = 3;

    /** {@code io.netty.allocator.heapSegmentSize}, which sets the page kinds (see {@link SizeClassTable#pageKinds}). */
    static final int HEAP_SEGMENT_SIZE_BYTES = heapSegmentSizeOf(SystemPropertyUtil.getInt(
            "io.netty.allocator.heapSegmentSize", SEGMENT_SIZE_BYTES - SLICE_SIZE_BYTES));

    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }

    private static long round(long value, long unit) {
        return Math.round((double) value / unit) * unit;
    }

    static long purgeDelayMillisOf(long millis) {
        return clamp(millis, 10, 600000);
    }

    static int defaultRegionSize(int segmentSize) {
        return Long.SIZE * segmentSize;
    }

    static int regionSizeOf(int size, int segmentSize) {
        if (size == 0) {
            return 0;
        }
        return (int) clamp(round(size, segmentSize), 2L * segmentSize, (long) Long.SIZE * segmentSize);
    }

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

    final int segmentSize;
    final int sliceSize;
    /** {@code numberOfTrailingZeros(sliceSize)}: a byte offset to a slice index without a division. */
    final int sliceShift;
    final long purgeDelayNanos;
    final long purgeCheckNanos;
    /** 2 to {@link Long#SIZE} whole segments, or {@link #mallocRegionSize}; 0: regions of {@link #mallocRegionSize}. */
    final int regionSize;
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
