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

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * The immutable parameters of one allocator's page store, and the direct and heap defaults, read once from the
 * {@code io.netty.allocator.segment*} properties. Read on slow paths only.
 */
final class PageStoreConfig {
    /** With regions, a slice is whole pages, so that no span starts or ends inside a page. */
    static final int PAGE_SIZE_BYTES = PageSize.PAGE_SIZE;
    /** The slice of an allocator's {@link Segment}s: 64 KiB. A chunk carved from a segment is whole slices. */
    static final int SLICE_SIZE_BYTES = 64 * 1024;
    /** One bit per slice in one {@code long}: 4 MiB. */
    static final int MAX_SEGMENT_SIZE_BYTES = Long.SIZE * SLICE_SIZE_BYTES;
    /** Room for the largest size-class chunk (9 slices) with some to spare. */
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
     * {@code io.netty.allocator.segmentRegionSize}: the size of the regions a direct allocator carves its
     * {@link Segment}s out of (see {@link PageStore}), rounded to the nearest multiple of the segment size from 2 up to
     * {@link Long#SIZE} segments; 0: no regions, one allocation per segment. Default: {@link Long#SIZE} segments
     * (256 MiB with 4 MiB segments, 128 MiB in low-memory mode). Address space only: a segment's pages are committed
     * as they are touched. Regions need a {@link RegionSource}, {@link MmapRegionSource} for the direct allocator,
     * and pages no larger than {@link #SLICE_SIZE_BYTES}: 0 otherwise.
     */
    static final int SEGMENT_REGION_SIZE_BYTES = regionSizeOf(PAGE_SIZE_BYTES, SystemPropertyUtil.getInt(
            "io.netty.allocator.segmentRegionSize", defaultRegionSize(SEGMENT_SIZE_BYTES)), SEGMENT_SIZE_BYTES);

    /**
     * {@code io.netty.allocator.segmentPurgeDelay}: milliseconds a free region slot, or a free slice of a segment a
     * heap holds, stays free before its memory goes back to the OS, as mimalloc's purge delay times its arena
     * multiplier (1000 ms x 4). From 10 ms to 10 minutes. Default: 4000.
     */
    static final long PURGE_DELAY_MILLIS = purgeDelayMillisOf(
            SystemPropertyUtil.getLong("io.netty.allocator.segmentPurgeDelay", 4000));

    static long purgeDelayMillisOf(long millis) {
        return Math.max(10, Math.min(600000, millis));
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
        long min = 2L * segmentSize;
        long max = (long) Long.SIZE * segmentSize;
        long rounded = Math.round((double) size / segmentSize) * (long) segmentSize;
        return (int) Math.max(min, Math.min(max, rounded));
    }

    /** 0 where a page is larger than a slice, else {@link #regionSizeOf(int, int)}. */
    static int regionSizeOf(int pageSize, int size, int segmentSize) {
        return pageSize > SLICE_SIZE_BYTES ? 0 : regionSizeOf(size, segmentSize);
    }

    /**
     * {@code size} rounded to the nearest multiple of {@link #REGION_ALIGNMENT_BYTES} from
     * {@link #MIN_SEGMENT_SIZE_BYTES} to {@link #MAX_SEGMENT_SIZE_BYTES}. {@link #REGION_ALIGNMENT_BYTES} is itself a
     * multiple of {@link #SLICE_SIZE_BYTES}, so the result is always a valid slice count too.
     */
    static int segmentSizeOf(int size) {
        long rounded = Math.round((double) size / REGION_ALIGNMENT_BYTES) * (long) REGION_ALIGNMENT_BYTES;
        long min = (MIN_SEGMENT_SIZE_BYTES + REGION_ALIGNMENT_BYTES - 1) / REGION_ALIGNMENT_BYTES
                * (long) REGION_ALIGNMENT_BYTES;
        long max = MAX_SEGMENT_SIZE_BYTES / REGION_ALIGNMENT_BYTES * (long) REGION_ALIGNMENT_BYTES;
        return (int) Math.max(min, Math.min(max, rounded));
    }

    /** 1 to {@link Long#SIZE} whole slices. */
    final int segmentSize;
    final int sliceSize;
    /** How long free memory stays before it is purged: see {@link #PURGE_DELAY_MILLIS}. */
    final long purgeDelayNanos;
    /**
     * How often, at most, a heap looks for memory to purge, and the store runs a purge pass: a quarter of the delay,
     * so that free memory goes back between one delay and a delay and a quarter after it was freed, while the heaps
     * allocate (see {@link HeapSegments#purgeTick}).
     */
    final long purgeCheckNanos;
    /** 2 to {@link Long#SIZE} whole segments, or 0: one allocation per segment. */
    final int regionSize;
    /** A power of two, honoured if the region source can; 0: any address. */
    final int regionAlignment;

    /** Without regions: one allocation per segment. */
    PageStoreConfig(int segmentSize, int sliceSize, long purgeDelayNanos) {
        this(segmentSize, sliceSize, purgeDelayNanos, 0, 0);
    }

    PageStoreConfig(int segmentSize, int sliceSize, long purgeDelayNanos, int regionSize, int regionAlignment) {
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
        this.segmentSize = segmentSize;
        this.sliceSize = sliceSize;
        this.purgeDelayNanos = purgeDelayNanos;
        purgeCheckNanos = Math.max(1, purgeDelayNanos >>> 2);
        if (regionSize != 0 && (regionSize < 0 || regionSize % segmentSize != 0
                || regionSize / segmentSize < 2 || regionSize / segmentSize > Long.SIZE)) {
            throw new IllegalArgumentException("regionSize " + regionSize + " is not 0 nor 2 to " + Long.SIZE
                    + " segments of " + segmentSize);
        }
        if (regionSize != 0 && sliceSize % PAGE_SIZE_BYTES != 0) {
            throw new IllegalArgumentException("sliceSize " + sliceSize + " is not whole pages of " + PAGE_SIZE_BYTES
                    + ", as regions need");
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
    }

    /**
     * The direct defaults without regions: a heap segment is one {@code byte[]}, nothing to map nor purge. Under G1
     * the segment is cut to {@link #heapSegmentSizeOf} so that it is never a humongous object.
     */
    static PageStoreConfig heapDefaults() {
        return new PageStoreConfig(G1.HEAP_SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES,
                TimeUnit.MILLISECONDS.toNanos(PURGE_DELAY_MILLIS));
    }

    /**
     * The most a {@code byte[]} of whole slices adds to its length: its header (12 to 24 bytes) rounded up to the
     * object alignment, at most 256 bytes ({@code ObjectAlignmentInBytes}).
     */
    static final int BYTE_ARRAY_OVERHEAD_BYTES = 256;

    /**
     * {@code segmentSize}, or under G1 regions of {@code g1RegionSize} the most whole slices whose {@code byte[]} is
     * not humongous: G1 allocates an object larger than half a region in regions of its own
     * ({@code G1CollectedHeap::is_humongous}). 1 MiB regions: 7 slices of 64 KiB; 2 MiB: 15; 4 MiB: 31; 8 MiB: 63;
     * 16 MiB and up: no cut. A {@code g1RegionSize} of 0 or less: not G1, nothing to cut.
     */
    static int heapSegmentSizeOf(int segmentSize, int sliceSize, long g1RegionSize) {
        if (g1RegionSize <= 0) {
            return segmentSize;
        }
        long slices = Math.min(segmentSize / sliceSize, (g1RegionSize / 2 - BYTE_ARRAY_OVERHEAD_BYTES) / sliceSize);
        return (int) Math.max(1, slices) * sliceSize;
    }

    /** Read on the first heap allocator only: the management beans cost nothing to a direct-only process. */
    private static final class G1 {
        private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStoreConfig.class);
        /** G1's minimum region, assumed when the VM does not publish the one it chose (JDK 8). */
        private static final long MIN_REGION_SIZE_BYTES = 1024 * 1024;
        static final int HEAP_SEGMENT_SIZE_BYTES;

        static {
            long regionSize = regionSize();
            HEAP_SEGMENT_SIZE_BYTES = heapSegmentSizeOf(SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES, regionSize);
            logger.debug("Heap segments: {} bytes (G1 region size: {})", HEAP_SEGMENT_SIZE_BYTES,
                    regionSize > 0 ? regionSize : "not G1, or unknown");
        }

        private G1() {
        }

        /**
         * The G1 region size in bytes, 0 when the VM does not use G1 or that cannot be told ({@code java.management}
         * or {@code com.sun.management} absent, or denied). Reflective, so that this class loads where they are not.
         */
        private static long regionSize() {
            try {
                Class<?> factory = Class.forName("java.lang.management.ManagementFactory");
                Class<?> beanType = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
                Class<?> optionType = Class.forName("com.sun.management.VMOption");
                Object bean = factory.getMethod("getPlatformMXBean", Class.class).invoke(null, beanType);
                if (bean == null) {
                    return 0;
                }
                Method getVMOption = beanType.getMethod("getVMOption", String.class);
                Method getValue = optionType.getMethod("getValue");
                if (!Boolean.parseBoolean((String) getValue.invoke(getVMOption.invoke(bean, "UseG1GC")))) {
                    return 0;
                }
                Object option = getVMOption.invoke(bean, "G1HeapRegionSize");
                long regionSize = Long.parseLong((String) getValue.invoke(option));
                // JDK 8 leaves the flag at 0 when it picks the size itself (heapRegion.cpp, setup_heap_region_size).
                return regionSize > 0 ? regionSize : MIN_REGION_SIZE_BYTES;
            } catch (Throwable t) {
                logger.debug("Could not read the G1 VM options", t);
                return 0;
            }
        }
    }

    static PageStoreConfig directDefaults() {
        return new PageStoreConfig(SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES,
                TimeUnit.MILLISECONDS.toNanos(PURGE_DELAY_MILLIS), SEGMENT_REGION_SIZE_BYTES, REGION_ALIGNMENT_BYTES);
    }

    int segmentsPerRegion() {
        return regionSize / segmentSize;
    }

    int slicesPerSegment() {
        return segmentSize / sliceSize;
    }
}
