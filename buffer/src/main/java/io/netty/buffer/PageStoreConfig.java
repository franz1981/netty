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
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * The immutable parameters of one allocator's page store, and the direct defaults, read once from the
 * {@code io.netty.allocator.segment*} properties. Read on slow paths only.
 */
final class PageStoreConfig {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStoreConfig.class);

    /** A slice is whole pages, so that no span starts or ends inside a page. */
    static final int PAGE_SIZE_BYTES = PageSize.PAGE_SIZE;
    /** The slice of a direct allocator's {@link Segment}s: 64 KiB. A chunk carved from a segment is whole slices. */
    static final int SLICE_SIZE_BYTES = 64 * 1024;
    /** One bit per slice in one {@code long}: 4 MiB. */
    static final int MAX_SEGMENT_SIZE_BYTES = Long.SIZE * SLICE_SIZE_BYTES;
    /** Room for the largest size-class chunk (9 slices) with some to spare. */
    static final int MIN_SEGMENT_SIZE_BYTES = 1024 * 1024;
    /**
     * {@code io.netty.allocator.segmentSize}: a multiple of {@link #SLICE_SIZE_BYTES} from
     * {@link #MIN_SEGMENT_SIZE_BYTES} to {@link #MAX_SEGMENT_SIZE_BYTES}. Default: 4 MiB, 2 MiB in low-memory mode
     * (where nothing else the allocator holds is above 2 MiB either).
     */
    static final int SEGMENT_SIZE_BYTES = segmentSizeOf(SystemPropertyUtil.getInt("io.netty.allocator.segmentSize",
            AdaptivePoolingAllocator.IS_LOW_MEM ? 2 * 1024 * 1024 : MAX_SEGMENT_SIZE_BYTES));
    /**
     * {@code io.netty.allocator.segmentCacheBytes}: how many bytes of wholly free {@link Segment}s a direct allocator
     * keeps for any of its heaps, at most; see {@link SegmentCache}. 0: none, a wholly free segment is given back at
     * once. Default: 64 MiB, 8 MiB in low-memory mode.
     */
    static final int SEGMENT_CACHE_BYTES = Math.max(0, SystemPropertyUtil.getInt(
            "io.netty.allocator.segmentCacheBytes",
            AdaptivePoolingAllocator.IS_LOW_MEM ? 8 * 1024 * 1024 : 64 * 1024 * 1024));
    /**
     * glibc's dynamic mmap threshold never rises above {@code DEFAULT_MMAP_THRESHOLD_MAX}, 32 MiB on 64-bit, and a
     * chunk above it never moves the threshold: a {@code malloc} above 32 MiB is always an {@code mmap}, its
     * {@code free} always a {@code munmap}, so it leaves no hole in an arena (glibc {@code malloc.c}: the max at 901,
     * the mmap test {@code nb >= mp_.mmap_threshold} at 2351, the ratchet on free, bounded by the max, at 4404-4409).
     */
    static final int MALLOC_MMAP_THRESHOLD_MAX_BYTES = 32 * 1024 * 1024;
    /** A direct allocator's regions start at a multiple of 2 MiB, so that a THP-enabled kernel can back them whole. */
    static final int REGION_ALIGNMENT_BYTES = 2 * 1024 * 1024;
    /**
     * {@code io.netty.allocator.segmentRegionSize}: the size of the regions a direct allocator carves its
     * {@link Segment}s out of (see {@link RegionPool}), a multiple of the segment size above
     * {@link #MALLOC_MMAP_THRESHOLD_MAX_BYTES} and at most {@link Long#SIZE} segments; 0: no regions, one allocation
     * per segment. Default: the smallest valid size (36 MiB with 4 MiB segments), 0 in low-memory mode. Used only where
     * allocating direct memory leaves it untouched ({@code PlatformDependent#directAllocationLeavesMemoryUntouched}):
     * where it is zeroed at allocation, a region would cost its whole size at once.
     */
    static final int SEGMENT_REGION_SIZE_BYTES = regionSizeOf(SystemPropertyUtil.getInt(
            "io.netty.allocator.segmentRegionSize",
            AdaptivePoolingAllocator.IS_LOW_MEM ? 0 : defaultRegionSize(SEGMENT_SIZE_BYTES)), SEGMENT_SIZE_BYTES);

    /** The smallest multiple of {@code segmentSize} above {@link #MALLOC_MMAP_THRESHOLD_MAX_BYTES}. */
    static int defaultRegionSize(int segmentSize) {
        return (MALLOC_MMAP_THRESHOLD_MAX_BYTES / segmentSize + 1) * segmentSize;
    }

    /**
     * {@code size} if it is 0 or a valid region size for {@code segmentSize}; else, with a warning, the default. No
     * regions when segments of {@code segmentSize} would not start on {@link #REGION_ALIGNMENT_BYTES} boundaries.
     */
    static int regionSizeOf(int size, int segmentSize) {
        if (size != 0 && segmentSize % REGION_ALIGNMENT_BYTES != 0) {
            logger.warn("-Dio.netty.allocator.segmentSize={}: not a multiple of the region alignment {}, no regions",
                    segmentSize, REGION_ALIGNMENT_BYTES);
            return 0;
        }
        if (size == 0 || size > MALLOC_MMAP_THRESHOLD_MAX_BYTES && size % segmentSize == 0
                && size / segmentSize <= Long.SIZE) {
            return size;
        }
        int fallback = AdaptivePoolingAllocator.IS_LOW_MEM ? 0 : defaultRegionSize(segmentSize);
        logger.warn("-Dio.netty.allocator.segmentRegionSize={}: not 0 nor a multiple of the segment size {} above {} " +
                "and at most {} segments, using {}",
                size, segmentSize, MALLOC_MMAP_THRESHOLD_MAX_BYTES, Long.SIZE, fallback);
        return fallback;
    }

    private static int segmentSizeOf(int size) {
        if (size % SLICE_SIZE_BYTES != 0 || size < MIN_SEGMENT_SIZE_BYTES || size > MAX_SEGMENT_SIZE_BYTES) {
            int fallback = AdaptivePoolingAllocator.IS_LOW_MEM ? 2 * 1024 * 1024 : MAX_SEGMENT_SIZE_BYTES;
            logger.warn("-Dio.netty.allocator.segmentSize={}: not a multiple of {} from {} to {}, using {}",
                    size, SLICE_SIZE_BYTES, MIN_SEGMENT_SIZE_BYTES, MAX_SEGMENT_SIZE_BYTES, fallback);
            return fallback;
        }
        return size;
    }

    /** 1 to {@link Long#SIZE} whole slices. */
    final int segmentSize;
    final int sliceSize;
    /** 0: no cache, a wholly free segment is freed at once. */
    final int segmentCacheBytes;
    /**
     * The cache ages at most once per interval, driven by the heaps' decays: an interval shorter than theirs ages it
     * no more often than they run.
     */
    final long decayIntervalNanos;
    /** In (0, 1]: the share of the segments cold for a whole interval that one ageing frees, rounded up. */
    final double decayFraction;
    /** 2 to {@link Long#SIZE} whole segments, or 0: one allocation per segment. */
    final int regionSize;
    /** A power of two, honoured if the region source can; 0: any address. */
    final int regionAlignment;

    /** Without regions: one allocation per segment. */
    PageStoreConfig(int segmentSize, int sliceSize, int segmentCacheBytes, long decayIntervalNanos,
                    double decayFraction) {
        this(segmentSize, sliceSize, segmentCacheBytes, decayIntervalNanos, decayFraction, 0, 0);
    }

    PageStoreConfig(int segmentSize, int sliceSize, int segmentCacheBytes, long decayIntervalNanos,
                    double decayFraction, int regionSize, int regionAlignment) {
        if (sliceSize <= 0 || sliceSize % PAGE_SIZE_BYTES != 0) {
            throw new IllegalArgumentException("sliceSize " + sliceSize + " is not whole pages of " + PAGE_SIZE_BYTES);
        }
        if (segmentSize <= 0 || segmentSize % sliceSize != 0 || segmentSize / sliceSize > Long.SIZE) {
            throw new IllegalArgumentException("segmentSize " + segmentSize + " is not 1 to " + Long.SIZE
                    + " slices of " + sliceSize);
        }
        if (segmentCacheBytes < 0) {
            throw new IllegalArgumentException("segmentCacheBytes: " + segmentCacheBytes);
        }
        if (decayIntervalNanos <= 0) {
            throw new IllegalArgumentException("decayIntervalNanos: " + decayIntervalNanos);
        }
        if (!(decayFraction > 0 && decayFraction <= 1)) {
            throw new IllegalArgumentException("decayFraction: " + decayFraction);
        }
        this.segmentSize = segmentSize;
        this.sliceSize = sliceSize;
        this.segmentCacheBytes = segmentCacheBytes;
        this.decayIntervalNanos = decayIntervalNanos;
        if (regionSize != 0 && (regionSize < 0 || regionSize % segmentSize != 0
                || regionSize / segmentSize < 2 || regionSize / segmentSize > Long.SIZE)) {
            throw new IllegalArgumentException("regionSize " + regionSize + " is not 0 nor 2 to " + Long.SIZE
                    + " segments of " + segmentSize);
        }
        if (regionAlignment < 0 || (regionAlignment & regionAlignment - 1) != 0) {
            throw new IllegalArgumentException("regionAlignment: " + regionAlignment);
        }
        if (regionSize != 0 && regionAlignment != 0 && segmentSize % regionAlignment != 0) {
            throw new IllegalArgumentException("segments of " + segmentSize + " would not start on "
                    + regionAlignment + " boundaries of their region");
        }
        this.decayFraction = decayFraction;
        this.regionSize = regionSize;
        this.regionAlignment = regionAlignment;
    }

    /** The cache ages with the heaps' decay interval, by halves. */
    static PageStoreConfig directDefaults() {
        return new PageStoreConfig(SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES, SEGMENT_CACHE_BYTES,
                AdaptivePoolingAllocator.IdleDecay.DECAY_INTERVAL_NANOS, 0.5, SEGMENT_REGION_SIZE_BYTES,
                REGION_ALIGNMENT_BYTES);
    }

    int segmentsPerRegion() {
        return regionSize / segmentSize;
    }

    int slicesPerSegment() {
        return segmentSize / sliceSize;
    }

    /** How many of {@code cold} idle segments one ageing gives back: {@link #decayFraction} of them, rounded up. */
    int toFree(int cold) {
        return cold == 0 ? 0 : (int) Math.min(cold, (long) Math.ceil(cold * decayFraction));
    }
}
