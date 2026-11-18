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

import io.netty.util.concurrent.Ticker;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.netty.incubator.allocator.PageStoreConfig.SLICE_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PageStoreConfig}: its validation (whole slices, whole segments per region, aligned segments), and how the
 * properties are clamped.
 */
final class PageStoreConfigTest {
    /** The largest block: one bit per slice of a 64-bit bitmap. */
    private static final int SEGMENT_SIZE = 64 * SLICE_SIZE_BYTES;
    private static final int REGION_ALIGNMENT = PageStoreConfig.REGION_ALIGNMENT_BYTES;
    private static final int REGION_SIZE = 9 * SEGMENT_SIZE;
    private static final long INTERVAL = TimeUnit.SECONDS.toNanos(10);

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

    /** The heap segment property is rounded down to whole slices, from 3 (one 132 KiB buffer) to 64. */
    @Test
    void heapSegmentSizeIsWholeSlices() {
        assertEquals(3 * SLICE_SIZE_BYTES, PageStoreConfig.clampHeapSegmentSize(100 * 1024));
        assertEquals(7 * SLICE_SIZE_BYTES, PageStoreConfig.clampHeapSegmentSize(8 * SLICE_SIZE_BYTES - 1));
        assertEquals(64 * SLICE_SIZE_BYTES, PageStoreConfig.clampHeapSegmentSize(10 * 1024 * 1024));
        assertEquals(PageStoreConfig.HEAP_SEGMENT_SIZE_BYTES, PageStoreConfig.heapDefaults().segmentSize);
    }

    /**
     * The purge delay property is handled, not rejected: a value in range is kept, one below it, zero or negative, and
     * one above it are clamped to a positive, finite delay, the lower one not above the upper one.
     */
    @Test
    void purgeDelayIsKeptOrClamped() {
        long least = PageStoreConfig.clampPurgeDelayMillis(0);
        long most = PageStoreConfig.clampPurgeDelayMillis(Long.MAX_VALUE);
        assertTrue(least > 0);
        assertTrue(least <= most);
        assertTrue(most < Long.MAX_VALUE);
        assertEquals(least, PageStoreConfig.clampPurgeDelayMillis(-5));
        long between = least + (most - least) / 2;
        assertEquals(between, PageStoreConfig.clampPurgeDelayMillis(between));
        assertEquals(least, PageStoreConfig.clampPurgeDelayMillis(least));
        assertEquals(most, PageStoreConfig.clampPurgeDelayMillis(most));
        assertEquals(most, PageStoreConfig.clampPurgeDelayMillis(most + 1));
    }

    /**
     * The property: 0 stays off; any other value is rounded to the nearest multiple of the segment size, clamped
     * from 2 up to 64 segments, rather than rejected. The default is a valid value.
     */
    @Test
    void badRegionSizesAreClampedToTheNearestValid() {
        for (int segment : new int[] {SEGMENT_SIZE, REGION_ALIGNMENT}) {
            int fallback = PageStoreConfig.defaultRegionSize(segment);
            assertEquals(0, fallback % segment, "the default is whole segments");
            assertTrue(fallback >= 2 * segment && fallback <= 64 * segment, "the default is 2 to 64 segments");
            assertEquals(fallback, PageStoreConfig.clampRegionSize(fallback, segment), "the default is valid");
        }
        assertEquals(0, PageStoreConfig.clampRegionSize(0, SEGMENT_SIZE));
        assertEquals(REGION_SIZE, PageStoreConfig.clampRegionSize(REGION_SIZE, SEGMENT_SIZE));
        assertEquals(64 * SEGMENT_SIZE, PageStoreConfig.clampRegionSize(64 * SEGMENT_SIZE, SEGMENT_SIZE));
        // Below the floor: clamped up to it.
        assertEquals(2 * SEGMENT_SIZE, PageStoreConfig.clampRegionSize(-1, SEGMENT_SIZE));
        assertEquals(2 * SEGMENT_SIZE, PageStoreConfig.clampRegionSize(SEGMENT_SIZE, SEGMENT_SIZE));
        // Not a multiple of the segment size: rounded to the nearest one.
        assertEquals(REGION_SIZE, PageStoreConfig.clampRegionSize(REGION_SIZE - SEGMENT_SIZE / 2, SEGMENT_SIZE));
        assertEquals(REGION_SIZE + SEGMENT_SIZE,
                PageStoreConfig.clampRegionSize(REGION_SIZE + SEGMENT_SIZE / 2, SEGMENT_SIZE));
        // Above the ceiling: clamped down to it.
        assertEquals(64 * SEGMENT_SIZE, PageStoreConfig.clampRegionSize(65 * SEGMENT_SIZE, SEGMENT_SIZE));
        assertEquals(64 * SEGMENT_SIZE, PageStoreConfig.clampRegionSize(Integer.MAX_VALUE, SEGMENT_SIZE));
        // The config itself: 2 to 64 whole segments, a power-of-two alignment.
        assertRejected(SEGMENT_SIZE, SEGMENT_SIZE, REGION_ALIGNMENT);
        assertRejected(SEGMENT_SIZE, 65 * SEGMENT_SIZE, REGION_ALIGNMENT);
        assertRejected(SEGMENT_SIZE, REGION_SIZE + REGION_ALIGNMENT, REGION_ALIGNMENT);
        assertRejected(SEGMENT_SIZE, REGION_SIZE, 3 * REGION_ALIGNMENT / 2);
        assertRejected(SEGMENT_SIZE, -REGION_SIZE, REGION_ALIGNMENT);
        // Segments of 1.5 alignments in an aligned region would not start on alignment boundaries: the constructor
        // still rejects this combination when it is built directly, for programmatic misuse.
        int misaligned = 3 * REGION_ALIGNMENT / 2;
        assertRejected(misaligned, 12 * misaligned, REGION_ALIGNMENT);
        new PageStoreConfig(misaligned, SLICE_SIZE_BYTES, INTERVAL, 12 * misaligned, REGION_ALIGNMENT / 2);
        new PageStoreConfig(misaligned, SLICE_SIZE_BYTES, INTERVAL, 12 * misaligned, 0);
    }

    /**
     * The property: rounded to the nearest multiple of the region alignment, clamped from one alignment up to the
     * 64 slices a block's bitmap holds, so that regions stay on rather than turning off for a segment size that would
     * misalign them.
     */
    @Test
    void badSegmentSizesAreClampedToTheNearestValid() {
        int least = PageStoreConfig.REGION_ALIGNMENT_BYTES;
        int most = 64 * SLICE_SIZE_BYTES;
        assertEquals(least, PageStoreConfig.clampSegmentSize(0));
        assertEquals(least, PageStoreConfig.clampSegmentSize(-1));
        assertEquals(least, PageStoreConfig.clampSegmentSize(least / 2));
        assertEquals(least, PageStoreConfig.clampSegmentSize(least));
        // Exactly halfway between the two: rounds up.
        assertEquals(most, PageStoreConfig.clampSegmentSize(least + (most - least) / 2));
        assertEquals(most, PageStoreConfig.clampSegmentSize(most));
        assertEquals(most, PageStoreConfig.clampSegmentSize(most + least / 2));
        assertEquals(most, PageStoreConfig.clampSegmentSize(Integer.MAX_VALUE));
        for (int size = least / 2; size <= most; size += SLICE_SIZE_BYTES) {
            int rounded = PageStoreConfig.clampSegmentSize(size);
            assertEquals(0, rounded % PageStoreConfig.REGION_ALIGNMENT_BYTES, "" + size);
            assertEquals(0, rounded % SLICE_SIZE_BYTES, "" + size);
        }
    }

    private static void assertRejected(final int segmentSize, final int regionSize, final int alignment) {
        assertThrows(IllegalArgumentException.class, () -> new PageStoreConfig(
                segmentSize, SLICE_SIZE_BYTES, INTERVAL, regionSize, alignment));
    }

    private static AdaptiveByteBufAllocator allocator(PageStoreConfig direct, boolean mmap) {
        return new AdaptiveByteBufAllocator(true, true, direct, PageStoreConfig.heapDefaults(),
                mmap ? a -> new MmapRegionSource(a) : null, Ticker.systemTicker());
    }

    /** The allocator rejects a page store whose blocks cannot hold a buffer of its largest size class. */
    @Test
    void blocksMustHoldTheLargestSizeClass() {
        assertThrows(IllegalArgumentException.class, () -> allocator(new PageStoreConfig(2 * SLICE_SIZE_BYTES,
                SLICE_SIZE_BYTES, INTERVAL, 0, 0, 2 * SLICE_SIZE_BYTES), false), "132 KiB takes 3 slices");
    }

    /**
     * No {@code mmap} regions ({@code segmentRegionSize=0}): the store rejects a region source passed anyway; built
     * with none, it falls back to {@code malloc}'d regions of one block and allocates and closes normally.
     */
    @Test
    void regionSizeZeroRejectsAMmapSourceAndFallsBackWithoutOne() {
        PageStoreConfig noMmap = new PageStoreConfig(PageStoreConfig.SEGMENT_SIZE_BYTES, SLICE_SIZE_BYTES, INTERVAL,
                0, 0, PageStoreConfig.SEGMENT_SIZE_BYTES);
        assertThrows(IllegalArgumentException.class, () -> allocator(noMmap, true));
        AdaptiveByteBufAllocator allocator = allocator(noMmap, false);
        try {
            allocator.directBuffer(1024).release();
            assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.usedDirectMemory());
        } finally {
            allocator.close();
        }
        assertEquals(0, allocator.usedDirectMemory());
    }

    /** A page store needs regions: a config without them, or no region source, is rejected. */
    @Test
    void withoutRegionsThereIsNoPageStore() {
        assertThrows(IllegalArgumentException.class,
                () -> allocator(new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, 0, 0), false));
        assertThrows(IllegalArgumentException.class, () -> allocator(
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT), false));
    }
}
