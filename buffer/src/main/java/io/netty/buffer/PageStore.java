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

/**
 * The part of an allocator's page store that all its heaps share: where a heap's segments come from and where they
 * go once wholly free. It has no mutable state of its own; the {@link RegionPool} guards its own. Slow paths only:
 * once per segment taken or freed.
 * <p>
 * Every segment or region allocated here is reported to {@link AdaptivePoolingAllocator#chunkBufferAllocated} and
 * {@link AdaptivePoolingAllocator#chunkBufferFreed}, so that the allocator's used memory and its chunk events count
 * the units this store holds from its sources.
 */
final class PageStore {
    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    final SegmentSource segmentSource;
    /** {@code null} when every segment is an allocation of its own. */
    final RegionPool regionPool;

    /** Without {@code regionSource}, or without regions in {@code config}, every segment is allocated on its own. */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, SegmentSource segmentSource,
              RegionSource regionSource) {
        this.allocator = allocator;
        this.config = config;
        this.segmentSource = segmentSource;
        regionPool = regionSource != null && config.regionSize > 0 ? new RegionPool(this, regionSource) : null;
    }

    /**
     * A segment for {@code heap}, which owns it from now on: a free slot of the fullest region, else a new allocation
     * (a new region, with regions).
     */
    Segment take(HeapSegments heap) {
        Segment segment;
        if (regionPool != null) {
            segment = regionPool.take(heap.isThreadLocal());
        } else {
            segment = new Segment(segmentSource.allocateSegment(config.segmentSize), config.sliceSize);
            allocator.chunkBufferAllocated(segment, true, heap.isThreadLocal());
        }
        segment.owner = heap;
        return segment;
    }

    /**
     * {@code segment}, wholly free and owned by no heap, goes back to its region (freed if that was its last segment
     * out), or to its source. Either may return memory to the OS.
     */
    void free(Segment segment) {
        assert segment.isWhollyFree() && segment.owner == null;
        if (segment.region != null) {
            regionPool.giveBack(segment);
            return;
        }
        allocator.chunkBufferFreed(segment, true);
        segment.buffer.release();
    }
}
