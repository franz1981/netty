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
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regions inside a direct {@link AdaptivePoolingAllocator}: size-class buffers land in region memory, and the
 * fallback to regions of one block where none can be mapped.
 */
public class AdaptiveSegmentRegionsTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    private final CountingMemorySource segments = new CountingMemorySource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** Buffers of the size classes through the allocator's own heaps, with regions: data lands inside the region. */
    @Test
    void sizeClassBuffersLiveInRegions() {
        assumeTrue(MmapRegionSource.isAvailable(), "regions of many blocks need mmap");
        AdaptivePoolingAllocator allocator = closer.add(newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT));
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int size : new int[] {32, 1024, 4352, 16896}) { // size classes in low-memory mode too
            ByteBuf buf = allocator.allocate(size, size);
            bufs.add(buf);
            buf.setLong(size - 8, size);
            assertEquals(size, buf.getLong(size - 8));
            ByteBuf adaptive = buf instanceof AdaptiveByteBuf ? buf : buf.unwrap();
            Segment segment = ((AdaptivePoolingAllocator.SizeClassedChunk)
                    ((AdaptiveByteBuf) adaptive).chunk).segment;
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
     * Below Java 22, or without native access: {@code malloc}'d regions of one block, counted whole from their
     * allocation.
     */
    @Test
    void withoutMmapRegionsAreMalloced() {
        if (PlatformDependent.javaVersion() < 22) {
            assertFalse(MmapRegionSource.isAvailable());
        }
        assumeFalse(MmapRegionSource.isAvailable(), "mmap regions are available here");
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(true, false));
        PageStore store = AdaptiveByteBufAllocatorTest.direct(allocator).pageStore;
        assertNull(store.mmap);
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        ByteBuf adaptive = buf instanceof AdaptiveByteBuf ? buf : buf.unwrap();
        Segment block = ((AdaptivePoolingAllocator.SizeClassedChunk)
                ((AdaptiveByteBuf) adaptive).chunk).segment;
        assertNotNull(block.region);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.metric().usedDirectMemory());
        buf.release();
    }

    /**
     * {@code io.netty.allocator.segmentRegionSize=0} (no {@code mmap} regions): the store rejects a region source
     * passed anyway (ab4b464f77 had {@code AdaptiveByteBufAllocator} do exactly that, and no test noticed, since the
     * property is read once at class load and nothing here can flip it); built the way
     * {@code AdaptiveByteBufAllocator} must, with no source, it falls back to {@code malloc}'d one-block regions and
     * allocates and releases normally.
     */
    @Test
    void regionSizeZeroRejectsAMmapSourceAndFallsBackWithoutOne() {
        PageStoreConfig noMmap = new PageStoreConfig(PageStoreConfig.SEGMENT_SIZE_BYTES,
                PageStoreConfig.SLICE_SIZE_BYTES, PageStoreTestSupport.INTERVAL, 0, 0,
                PageStoreConfig.SEGMENT_SIZE_BYTES);
        CountingMemorySource source = new CountingMemorySource();
        assertThrows(IllegalArgumentException.class, () -> new AdaptivePoolingAllocator(source, true,
                new MmapRegionSource(UnpooledByteBufAllocator.DEFAULT), noMmap));
        AdaptivePoolingAllocator allocator = closer.add(new AdaptivePoolingAllocator(source, true, null, noMmap));
        assertNull(allocator.pageStore.mmap);
        ByteBuf buf = allocator.allocate(1024, 1024);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.usedMemory());
        buf.release();
        allocator.pageStore.close();
        assertEquals(0, allocator.usedMemory());
    }
}
