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

import java.util.Arrays;

/**
 * The regions of an allocator, shared by all its heaps: where a segment comes from when the
 * {@link SegmentCache} has none, and where it goes back when it leaves the cache (evicted by the cache's bound, or
 * aged out by its decay). A segment is taken from the fullest region that has a free slot (the oldest of equally
 * full ones), lowest slot first, and a new region is allocated only when none has one: the emptiest regions are
 * left to drain. A region whose segments are all back is freed at once.
 * <p>
 * No delay is added before a region is freed: a segment reaches its region only after the cache kept it through at
 * least one whole interval (the ageing frees only segments that stayed there that long), or when the cache was
 * full, where without regions the segment would have been freed at once just the same. A region therefore goes
 * back no earlier than its last segment would have without regions.
 * <p>
 * Why regions: with glibc, a {@code malloc} above {@link PageStoreConfig#MALLOC_MMAP_THRESHOLD_MAX} is always an
 * {@code mmap} and its {@code free} a {@code munmap}; 4 MiB segments allocated one by one fall under the dynamic
 * threshold once it rose, and their frees leave holes in the arenas. The cost: a region stays allocated while any of
 * its segments is out, which the fullest-first rule is there to keep rare.
 * <p>
 * Guarded by its monitor, taken on slow paths only: a segment taken from or given back to a region, and the
 * dump. A region is allocated outside the lock.
 */
final class RegionPool {
    private final AdaptivePoolingAllocator allocator;
    private final RegionSource source;
    private final PageStoreConfig config;
    // Visible for dumps and tests: the regions with a segment out, oldest first; guarded by this.
    Region[] regions = new Region[4];
    int count;
    // Counters, for dumps and tests; guarded by this.
    long allocated;
    long freed;

    RegionPool(AdaptivePoolingAllocator allocator, RegionSource source, PageStoreConfig config) {
        assert config.regionSize > 0;
        this.allocator = allocator;
        this.source = source;
        this.config = config;
    }

    /** A segment of the fullest region with a free slot, else of a new region, which is announced. */
    Segment take(boolean threadLocal) {
        synchronized (this) {
            Segment segment = takeFromFullest();
            if (segment != null) {
                return segment;
            }
        }
        AbstractByteBuf buffer = source.allocateRegion(config.regionSize, config.regionAlignment);
        assert buffer.capacity() == config.regionSize;
        Region region = new Region(buffer, source.allocatedBytes(buffer), config.segmentsPerRegion());
        allocator.chunkBufferAllocated(region, true, threadLocal);
        synchronized (this) {
            if (count == regions.length) {
                regions = Arrays.copyOf(regions, count << 1);
            }
            regions[count++] = region;
            allocated++;
            // Another thread may have given a segment back meanwhile: the fullest rule still applies.
            return takeFromFullest();
        }
    }

    private Segment takeFromFullest() {
        Region best = null;
        int bestFree = Integer.MAX_VALUE;
        for (int i = 0; i < count; i++) {
            Region region = regions[i];
            int free = region.freeSlotCount();
            if (free != 0 && free < bestFree) {
                best = region;
                bestFree = free;
                if (free == 1) {
                    break;
                }
            }
        }
        return best == null ? null : best.takeSlot(allocator.segmentSource, config);
    }

    /** {@code segment}, wholly free and owned by no heap, is back; its region is freed if it was the last out. */
    void giveBack(Segment segment) {
        Region region = segment.region;
        synchronized (this) {
            assert (region.freeSlots & 1L << segment.slot) == 0 : "slot " + segment.slot + " is already free";
            region.freeSlots |= 1L << segment.slot;
            if (region.freeSlots != region.allFree) {
                return;
            }
            remove(region);
            freed++;
        }
        allocator.chunkBufferFreed(region, true);
        region.buffer.release();
    }

    private void remove(Region region) {
        for (int i = 0; i < count; i++) {
            if (regions[i] == region) {
                System.arraycopy(regions, i + 1, regions, i, count - i - 1);
                regions[--count] = null;
                return;
            }
        }
        throw new IllegalStateException(region + " is not in the pool");
    }

    synchronized int regionCount() {
        return count;
    }
}
