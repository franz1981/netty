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
 * The regions of one allocator that have a segment out, oldest first. A segment comes from the fullest region with a
 * free slot (the oldest on a tie), its lowest slot; a region is allocated only when none has one, and freed as soon
 * as its last segment is back. Segments come back only from the {@link SegmentCache}, so a region is never freed
 * earlier than its last segment would have been without regions.
 * <p>
 * Why: with glibc, a {@code malloc} above {@link PageStoreConfig#MALLOC_MMAP_THRESHOLD_MAX_BYTES} is always an
 * {@code mmap} and its {@code free} a {@code munmap}; 4 MiB segments fall under the dynamic threshold once it rose,
 * and their frees leave holes in the arenas. Cost: a region stays allocated while any of its segments is out.
 * <p>
 * Guarded by its monitor, slow paths only. Regions are allocated and freed outside it.
 */
final class RegionPool {
    private final PageStore store;
    private final RegionSource source;
    private final PageStoreConfig config;
    // Read by tests and dumps; guarded by this.
    Region[] regions = new Region[4];
    int count;
    long allocated;
    long freed;

    RegionPool(PageStore store, RegionSource source) {
        assert store.config.regionSize > 0;
        this.store = store;
        this.source = source;
        config = store.config;
    }

    /** A new region is accounted as allocated here. */
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
        store.allocator.chunkBufferAllocated(region, true, threadLocal);
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
        return best == null ? null : best.takeSlot(store.segmentSource, config);
    }

    /** {@code segment} is wholly free and owned by no heap. Frees and accounts its region if it was the last out. */
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
        store.allocator.chunkBufferFreed(region, true);
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
