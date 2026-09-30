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

import io.netty.buffer.AdaptivePoolingAllocator.IdleDecay;
import org.junit.jupiter.api.Test;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link PageStoreConfig}: its validation (whole slices and pages, whole segments per region, aligned segments), and
 * the direct defaults with their property fallbacks.
 */
final class PageStoreConfigTest {
    /** Bad parameters are rejected: too many slices, a partial slice or page, no interval. */
    @Test
    void pageStoreConfigRejectsBadParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(65 * SLICE_SIZE_BYTES, SLICE_SIZE_BYTES, INTERVAL), "65 slices");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE + 4096, SLICE_SIZE_BYTES, INTERVAL), "partial slice");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, 0, INTERVAL), "no slice");
        assertThrows(IllegalArgumentException.class, () -> new PageStoreConfig(SEGMENT_SIZE,
                PageStoreConfig.PAGE_SIZE_BYTES / 2, INTERVAL), "a slice is whole pages");
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, 0), "no interval");
        PageStoreConfig ok = new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL);
        assertEquals(64, ok.slicesPerSegment());
    }

    /** The direct defaults: 64 KiB slices, the property-driven segment size, the heaps' decay interval. */
    @Test
    void directDefaults() {
        PageStoreConfig direct = PageStoreConfig.directDefaults();
        assertEquals(SLICE_SIZE_BYTES, direct.sliceSize);
        assertEquals(64 * 1024, direct.sliceSize);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, direct.segmentSize);
        assertEquals(IdleDecay.DECAY_INTERVAL_NANOS, direct.decayIntervalNanos);
    }

    /**
     * The property: 0 stays off; any other value is rounded to the nearest multiple of the segment size, clamped
     * from 2 up to 64 segments, rather than rejected. The default is the largest.
     */
    @Test
    void badRegionSizesAreClampedToTheNearestValid() {
        assertEquals(256 * MIB, PageStoreConfig.defaultRegionSize(SEGMENT_SIZE));
        assertEquals(128 * MIB, PageStoreConfig.defaultRegionSize(2 * MIB));
        assertEquals(0, PageStoreConfig.regionSizeOf(0, SEGMENT_SIZE));
        assertEquals(36 * MIB, PageStoreConfig.regionSizeOf(36 * MIB, SEGMENT_SIZE));
        assertEquals(256 * MIB, PageStoreConfig.regionSizeOf(256 * MIB, SEGMENT_SIZE));
        // Below the floor: clamped up to it.
        assertEquals(8 * MIB, PageStoreConfig.regionSizeOf(-1, SEGMENT_SIZE));
        assertEquals(8 * MIB, PageStoreConfig.regionSizeOf(4 * MIB, SEGMENT_SIZE));
        // Not a multiple of the segment size: rounded to the nearest one.
        assertEquals(36 * MIB, PageStoreConfig.regionSizeOf(34 * MIB, SEGMENT_SIZE));
        assertEquals(40 * MIB, PageStoreConfig.regionSizeOf(38 * MIB, SEGMENT_SIZE));
        // Above the ceiling: clamped down to it.
        assertEquals(256 * MIB, PageStoreConfig.regionSizeOf(260 * MIB, SEGMENT_SIZE));
        assertEquals(256 * MIB, PageStoreConfig.regionSizeOf(Integer.MAX_VALUE, SEGMENT_SIZE));
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
        assertEquals(2 * MIB, PageStoreConfig.segmentSizeOf(0));
        assertEquals(2 * MIB, PageStoreConfig.segmentSizeOf(-1));
        assertEquals(2 * MIB, PageStoreConfig.segmentSizeOf(MIB));
        assertEquals(2 * MIB, PageStoreConfig.segmentSizeOf(2 * MIB));
        // Exactly halfway between 2 and 4 MiB: rounds up.
        assertEquals(4 * MIB, PageStoreConfig.segmentSizeOf(3 * MIB));
        assertEquals(4 * MIB, PageStoreConfig.segmentSizeOf(4 * MIB));
        assertEquals(4 * MIB, PageStoreConfig.segmentSizeOf(5 * MIB));
        assertEquals(4 * MIB, PageStoreConfig.segmentSizeOf(Integer.MAX_VALUE));
        for (int size = MIB; size <= 4 * MIB; size += 64 * 1024) {
            int rounded = PageStoreConfig.segmentSizeOf(size);
            assertEquals(0, rounded % PageStoreConfig.REGION_ALIGNMENT_BYTES, "" + size);
            assertEquals(0, rounded % SLICE_SIZE_BYTES, "" + size);
        }
    }

    private static void assertRejected(final int segmentSize, final int regionSize, final int alignment) {
        assertThrows(IllegalArgumentException.class, () -> new PageStoreConfig(
                segmentSize, SLICE_SIZE_BYTES, INTERVAL, regionSize, alignment));
    }
}
