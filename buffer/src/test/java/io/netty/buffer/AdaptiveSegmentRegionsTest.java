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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertEquals(MmapRegionSource.isAvailable(), AdaptiveByteBufAllocatorTest.directRegions(allocator));
    }
}
