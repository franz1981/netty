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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link PageStoreConfig}: its validation (whole slices, whole segments per region, aligned segments), and the
 * direct defaults with their property fallbacks.
 */
final class PageStoreConfigTest {
    /** Bad parameters are rejected: too many slices, a partial slice, no interval. */
    @Test
    void pageStoreConfigRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(65 * SLICE_SIZE_BYTES, SLICE_SIZE_BYTES, INTERVAL, 0, 0), "65 slices");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE + 4096, SLICE_SIZE_BYTES, INTERVAL, 0, 0), "partial slice");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, 0, INTERVAL, 0, 0), "no slice");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, 0, 0, 0), "no interval");
        PageStoreConfig ok = new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, 0, 0, SEGMENT_SIZE)
                .withMallocRegions();
        assertEquals(64, ok.slicesPerSegment());
        assertEquals(SEGMENT_SIZE, ok.regionSize);
    }

    /** The direct defaults: 64 KiB slices, the property-driven segment size, the heaps' decay interval. */
    @Test
    void directDefaults() {
        PageStoreConfig direct = PageStoreConfig.directDefaults();
        assertEquals(SLICE_SIZE_BYTES, direct.sliceSize);
        assertEquals(64 * 1024, direct.sliceSize);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, direct.segmentSize);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(PageStoreConfig.PURGE_DELAY_MILLIS), direct.purgeDelayNanos);
        assertEquals(direct.purgeDelayNanos / 4, direct.purgeCheckNanos);
        if (System.getProperty("io.netty.allocator.segmentPurgeDelay") == null) {
            assertEquals(4000, PageStoreConfig.PURGE_DELAY_MILLIS, "mimalloc's 1000 ms purge delay x 4");
        }
    }

    /** The heap segment property is rounded down to whole slices, from 3 (one 132 KiB buffer) to 64. */
    @Test
    void heapSegmentSizeIsWholeSlices() {
        assertEquals(3 * SLICE_SIZE_BYTES, PageStoreConfig.clampHeapSegmentSize(100 * 1024));
        assertEquals(7 * SLICE_SIZE_BYTES, PageStoreConfig.clampHeapSegmentSize(8 * SLICE_SIZE_BYTES - 1));
        assertEquals(64 * SLICE_SIZE_BYTES, PageStoreConfig.clampHeapSegmentSize(10 * 1024 * 1024));
        assertEquals(PageStoreConfig.HEAP_SEGMENT_SIZE_BYTES, PageStoreConfig.heapDefaults().segmentSize);
    }

    /** The purge delay property is clamped to 10 ms .. 10 minutes. */
    @Test
    void purgeDelayIsClamped() {
        assertEquals(10, PageStoreConfig.clampPurgeDelayMillis(0));
        assertEquals(10, PageStoreConfig.clampPurgeDelayMillis(-5));
        assertEquals(4000, PageStoreConfig.clampPurgeDelayMillis(4000));
        assertEquals(600000, PageStoreConfig.clampPurgeDelayMillis(Long.MAX_VALUE));
    }

    /**
     * The property: 0 stays off; any other value is rounded to the nearest multiple of the segment size, clamped
     * from 2 up to 64 segments, rather than rejected. The default is the largest.
     */
    @Test
    void badRegionSizesAreClampedToTheNearestValid() {
        assertEquals(256 * MIB, PageStoreConfig.defaultRegionSize(SEGMENT_SIZE));
        assertEquals(128 * MIB, PageStoreConfig.defaultRegionSize(2 * MIB));
        assertEquals(0, PageStoreConfig.clampRegionSize(0, SEGMENT_SIZE));
        assertEquals(36 * MIB, PageStoreConfig.clampRegionSize(36 * MIB, SEGMENT_SIZE));
        assertEquals(256 * MIB, PageStoreConfig.clampRegionSize(256 * MIB, SEGMENT_SIZE));
        // Below the floor: clamped up to it.
        assertEquals(8 * MIB, PageStoreConfig.clampRegionSize(-1, SEGMENT_SIZE));
        assertEquals(8 * MIB, PageStoreConfig.clampRegionSize(4 * MIB, SEGMENT_SIZE));
        // Not a multiple of the segment size: rounded to the nearest one.
        assertEquals(36 * MIB, PageStoreConfig.clampRegionSize(34 * MIB, SEGMENT_SIZE));
        assertEquals(40 * MIB, PageStoreConfig.clampRegionSize(38 * MIB, SEGMENT_SIZE));
        // Above the ceiling: clamped down to it.
        assertEquals(256 * MIB, PageStoreConfig.clampRegionSize(260 * MIB, SEGMENT_SIZE));
        assertEquals(256 * MIB, PageStoreConfig.clampRegionSize(Integer.MAX_VALUE, SEGMENT_SIZE));
        // The config itself: 2 to 64 whole segments, a power-of-two alignment.
        assertRejected(SEGMENT_SIZE, SEGMENT_SIZE, REGION_ALIGNMENT);
        assertRejected(SEGMENT_SIZE, 65 * SEGMENT_SIZE, REGION_ALIGNMENT);
        assertRejected(SEGMENT_SIZE, REGION_SIZE + MIB, REGION_ALIGNMENT);
        assertRejected(SEGMENT_SIZE, REGION_SIZE, 3 * MIB);
        assertRejected(SEGMENT_SIZE, -REGION_SIZE, REGION_ALIGNMENT);
        // Segments of 3 MiB in a 2 MiB-aligned region would not start on 2 MiB boundaries: the constructor still
        // rejects this combination when it is built directly, for programmatic misuse.
        assertRejected(3 * MIB, 36 * MIB, REGION_ALIGNMENT);
        new PageStoreConfig(3 * MIB, SLICE_SIZE_BYTES, INTERVAL, 36 * MIB, MIB);
        new PageStoreConfig(3 * MIB, SLICE_SIZE_BYTES, INTERVAL, 36 * MIB, 0);
    }

    /**
     * The property: rounded to the nearest multiple of the region alignment, clamped from 1 to 4 MiB, so that
     * regions stay on rather than turning off for a segment size that would misalign them.
     */
    @Test
    void badSegmentSizesAreClampedToTheNearestValid() {
        assertEquals(2 * MIB, PageStoreConfig.clampSegmentSize(0));
        assertEquals(2 * MIB, PageStoreConfig.clampSegmentSize(-1));
        assertEquals(2 * MIB, PageStoreConfig.clampSegmentSize(MIB));
        assertEquals(2 * MIB, PageStoreConfig.clampSegmentSize(2 * MIB));
        // Exactly halfway between 2 and 4 MiB: rounds up.
        assertEquals(4 * MIB, PageStoreConfig.clampSegmentSize(3 * MIB));
        assertEquals(4 * MIB, PageStoreConfig.clampSegmentSize(4 * MIB));
        assertEquals(4 * MIB, PageStoreConfig.clampSegmentSize(5 * MIB));
        assertEquals(4 * MIB, PageStoreConfig.clampSegmentSize(Integer.MAX_VALUE));
        for (int size = MIB; size <= 4 * MIB; size += 64 * 1024) {
            int rounded = PageStoreConfig.clampSegmentSize(size);
            assertEquals(0, rounded % PageStoreConfig.REGION_ALIGNMENT_BYTES, "" + size);
            assertEquals(0, rounded % SLICE_SIZE_BYTES, "" + size);
        }
    }

    private static void assertRejected(final int segmentSize, final int regionSize, final int alignment) {
        assertThrows(IllegalArgumentException.class, () -> new PageStoreConfig(
                segmentSize, SLICE_SIZE_BYTES, INTERVAL, regionSize, alignment));
    }
}
