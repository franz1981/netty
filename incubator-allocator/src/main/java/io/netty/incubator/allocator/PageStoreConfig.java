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
package io.netty.incubator.allocator;

import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * The immutable geometry of one allocator's {@link PageStore}: the slice, the block ("segment" in the property
 * names), the region, and the purge delay. Read once from the {@code io.netty.allocator.pagestore.segment*}
 * properties, then shared by every thread.
 */
final class PageStoreConfig {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStoreConfig.class);
    static final int SLICE_SIZE_BYTES = 64 * 1024;
    /** One bit per slice in a block's bitmap. */
    private static final int MAX_SEGMENT_SIZE_BYTES = Long.SIZE * SLICE_SIZE_BYTES;
    /** Room for the largest size class (3 slices) with some to spare. */
    private static final int MIN_SEGMENT_SIZE_BYTES = 1024 * 1024;
    /** {@code io.netty.allocator.pagestore.segmentSize}: the block size, a multiple of the region alignment. */
    static final int SEGMENT_SIZE_BYTES = clampSegmentSize(SystemPropertyUtil.getInt(
            "io.netty.allocator.pagestore.segmentSize",
            AdaptivePoolingAllocator.IS_LOW_MEM ? 2 * 1024 * 1024 : MAX_SEGMENT_SIZE_BYTES));
    /** A direct allocator's regions start at a multiple of 2 MiB, so a THP-enabled kernel can back them whole. */
    static final int REGION_ALIGNMENT_BYTES = 2 * 1024 * 1024;
    /**
     * {@code io.netty.allocator.pagestore.mmap}: whether a direct allocator cuts its blocks from {@code mmap(2)}
     * regions, whose idle slices it gives back with {@code madvise} (Java 23+ on Linux, with native access). Off by
     * default: the direct regions are then {@code malloc}'d blocks.
     */
    static final boolean MMAP = SystemPropertyUtil.getBoolean("io.netty.allocator.pagestore.mmap", false);
    /**
     * {@code io.netty.allocator.pagestore.segmentRegionSize}: the size of the {@code mmap} regions a direct allocator
     * cuts its blocks from; 0 falls back to {@code malloc}'d regions of one block. A mapping is address space only:
     * pages commit as they are touched. Regions of one block scatter the allocator's memory over as many mappings;
     * a larger region keeps its blocks in one.
     */
    static final int SEGMENT_REGION_SIZE_BYTES = clampRegionSize(SystemPropertyUtil.getInt(
            "io.netty.allocator.pagestore.segmentRegionSize", defaultRegionSize(SEGMENT_SIZE_BYTES)),
            SEGMENT_SIZE_BYTES);

    /**
     * {@code io.netty.allocator.pagestore.segmentPurgeDelay}: milliseconds a free slice stays free before a purge,
     * except a released run of {@link PageStore#PURGE_ON_RELEASE_BYTES} or more.
     */
    static final long PURGE_DELAY_MILLIS = clampPurgeDelayMillis(
            SystemPropertyUtil.getLong("io.netty.allocator.pagestore.segmentPurgeDelay", 4000));

    /** The fewest slices of a heap allocator's block: one buffer of the largest size class takes 3. */
    private static final int MIN_HEAP_SEGMENT_SLICES = 3;

    /**
     * {@code io.netty.allocator.pagestore.heapSegmentSize}: the heap allocator's block size, which sets its page
     * kinds (see {@link SizeClassTable#pageKinds}). Such a block is one {@code byte[]}: born young, and copied to
     * the old generation by the first collections. G1 collects young regions by count, so a size that fills its regions
     * whole gets more copied, and more memory resident, than one that leaves part of each region untouched.
     */
    static final int HEAP_SEGMENT_SIZE_BYTES = clampHeapSegmentSize(SystemPropertyUtil.getInt(
            "io.netty.allocator.pagestore.heapSegmentSize", SEGMENT_SIZE_BYTES - SLICE_SIZE_BYTES));

    static {
        logEffective("segmentSize", SEGMENT_SIZE_BYTES);
        logEffective("segmentRegionSize", SEGMENT_REGION_SIZE_BYTES);
        logEffective("heapSegmentSize", HEAP_SEGMENT_SIZE_BYTES);
        logEffective("segmentPurgeDelay", PURGE_DELAY_MILLIS);
    }

    /** The value in use at DEBUG, and a WARN if the property was set and rounded or clamped to another one. */
    private static void logEffective(String name, long effective) {
        String property = "io.netty.allocator.pagestore." + name;
        logger.debug("-D{}: {}", property, effective);
        String set = SystemPropertyUtil.get(property);
        if (set == null) {
            return;
        }
        long asked;
        try {
            asked = Long.parseLong(set.trim());
        } catch (NumberFormatException e) {
            // SystemPropertyUtil said so when the property was read.
            return;
        }
        if (asked != effective) {
            logger.warn("-D{}={} was changed to {}", property, set, effective);
        }
    }

    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }

    private static long round(long value, long unit) {
        return Math.round((double) value / unit) * unit;
    }

    static long clampPurgeDelayMillis(long millis) {
        return clamp(millis, 10, 600000);
    }

    static int defaultRegionSize(int segmentSize) {
        return Long.SIZE * segmentSize;
    }

    static int clampRegionSize(int size, int segmentSize) {
        if (size == 0) {
            return 0;
        }
        return (int) clamp(round(size, segmentSize), 2L * segmentSize, (long) Long.SIZE * segmentSize);
    }

    static int clampSegmentSize(int size) {
        long min = (MIN_SEGMENT_SIZE_BYTES + REGION_ALIGNMENT_BYTES - 1) / REGION_ALIGNMENT_BYTES
                * (long) REGION_ALIGNMENT_BYTES;
        long max = MAX_SEGMENT_SIZE_BYTES / REGION_ALIGNMENT_BYTES * (long) REGION_ALIGNMENT_BYTES;
        return (int) clamp(round(size, REGION_ALIGNMENT_BYTES), min, max);
    }

    /** {@code size} rounded down to whole slices, from {@link #MIN_HEAP_SEGMENT_SLICES} to {@link Long#SIZE}. */
    static int clampHeapSegmentSize(int size) {
        return (int) clamp(size / SLICE_SIZE_BYTES, MIN_HEAP_SEGMENT_SLICES, Long.SIZE) * SLICE_SIZE_BYTES;
    }

    /** The block size. */
    final int segmentSize;
    final int sliceSize;
    /** {@code numberOfTrailingZeros(sliceSize)}: a byte offset to a slice index without a division. */
    final int sliceShift;
    /** How long a slice stays free before the purger may give its memory back. */
    final long purgeDelayNanos;
    /** The least time between two purge passes. */
    final long purgeCheckNanos;
    /**
     * The size of a region: 2 to {@link Long#SIZE} blocks for an {@code mmap} one, or {@link #mallocRegionSize} once
     * mapping was given up ({@link #withMallocRegions}); 0: no regions, the store cannot be built.
     */
    final int regionSize;
    final int regionAlignment;
    /** The size of a {@code malloc}'d region of one block, taken where no {@code mmap} region can be; 0: none. */
    final int mallocRegionSize;

    /** With {@code mmap} regions of {@code regionSize} only: no fallback to {@code malloc}'d ones. */
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

    /** This, with {@code malloc}'d regions of one block each, at libc's alignment: after mapping failed. */
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
