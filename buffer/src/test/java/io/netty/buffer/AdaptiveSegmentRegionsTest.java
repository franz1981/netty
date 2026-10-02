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

import io.netty.buffer.PageStoreTestSupport.CountingRegionSource;
import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Regions inside a direct {@link AdaptivePoolingAllocator}: size-class buffers land in region memory, and the
 * defaults.
 */
public class AdaptiveSegmentRegionsTest {
    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** Buffers of the size classes through the allocator's own heaps, with regions: data lands inside the region. */
    @Test
    void sizeClassBuffersLiveInRegions() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int size : new int[] {32, 1024, 4352, 16896}) { // size classes in low-memory mode too
            ByteBuf buf = allocator.allocate(size, size);
            bufs.add(buf);
            buf.setLong(size - 8, size);
            assertEquals(size, buf.getLong(size - 8));
            ByteBuf adaptive = buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf ? buf : buf.unwrap();
            Segment segment = ((AdaptivePoolingAllocator.SizeClassedChunk)
                    ((AdaptivePoolingAllocator.AdaptiveByteBuf) adaptive).chunk).segment;
            assertNotNull(segment.region);
            long base = segment.region.buffer.memoryAddress();
            assertTrue(buf.memoryAddress() >= base && buf.memoryAddress() + size <= base + REGION_SIZE);
        }
        assertEquals(1, regions.regions.size());
        assertEquals(0, segments.segmentsAllocated());
        assertAccounted(segments, regions, allocator);
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(segments, regions, allocator);
    }

    /**
     * The defaults, in both memory modes: 64-segment regions aligned to 2 MiB, used wherever they can be mapped.
     */
    @Test
    void regionsWhereverTheyCanBeMapped() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                System.getProperty("io.netty.allocator.segmentRegionSize") == null, "set explicitly");
        PageStoreConfig direct = PageStoreConfig.directDefaults();
        assertEquals(Long.SIZE * direct.segmentSize, direct.regionSize);
        assertEquals(REGION_ALIGNMENT, direct.regionAlignment);
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false);
        assertTrue(AdaptiveByteBufAllocatorTest.directRegions(allocator), "mmap'd or malloc'd");
        assertEquals(MmapRegionSource.isAvailable(), AdaptiveByteBufAllocatorTest.direct(allocator).pageStore
                .regionSource instanceof MmapRegionSource);
    }

    /**
     * Below Java 22, or without native access: {@code malloc}'d regions of one block, counted whole from their
     * allocation.
     */
    @Test
    void withoutMmapRegionsAreMalloced() {
        if (PlatformDependent.javaVersion() < 22) {
            assertFalse(MmapRegionSource.isAvailable());
        }
        assumeFalse(MmapRegionSource.isAvailable(), "mmap regions are available here");
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false);
        PageStore store = AdaptiveByteBufAllocatorTest.direct(allocator).pageStore;
        assertTrue(store.regionSource instanceof MallocRegionSource);
        assertFalse(store.purgesSlices);
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        ByteBuf adaptive = buf instanceof AdaptivePoolingAllocator.AdaptiveByteBuf ? buf : buf.unwrap();
        Segment block = ((AdaptivePoolingAllocator.SizeClassedChunk)
                ((AdaptivePoolingAllocator.AdaptiveByteBuf) adaptive).chunk).segment;
        assertNotNull(block.region);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.metric().usedDirectMemory());
        buf.release();
    }

    /**
     * {@code malloc}'d regions in place of {@code mmap} ones, by configuration (a region size of 0): the same shared
     * slices, the region counted whole.
     */
    @Test
    void aConfigWithoutMmapRegionsMallocsThem() {
        AdaptiveByteBufAllocator adaptive = new AdaptiveByteBufAllocator(true, false);
        AdaptivePoolingAllocator direct = AdaptiveByteBufAllocatorTest.direct(adaptive);
        SegmentSource source = direct.pageStore.segmentSource;
        PageStoreConfig noMmap = PageStoreConfig.sharedSlices(PageStoreConfig.SEGMENT_SIZE_BYTES,
                PageStoreConfig.SLICE_SIZE_BYTES, PageStoreTestSupport.INTERVAL, 0, 0,
                PageStoreConfig.SEGMENT_SIZE_BYTES);
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(
                (AdaptivePoolingAllocator.ChunkAllocator) source, true, source, noMmap);
        PageStore store = allocator.pageStore;
        assertTrue(store.regionSource instanceof MallocRegionSource);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, store.config.regionSize);
        ByteBuf buf = allocator.allocate(1024, 1024);
        assertEquals(1, store.regionCount());
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.usedMemory());
        buf.release();
        store.close();
        assertEquals(0, allocator.usedMemory());
    }
}
