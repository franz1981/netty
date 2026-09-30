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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Where an allocator's heaps take their segments from and give them back to: free slots of its {@link Region}s, or,
 * without a {@link RegionSource}, one allocation per segment. Slow paths only: once per segment taken or given back.
 * <p>
 * Regions: a slot is taken and given back by a CAS on its region's bitmap, without a lock; a new region is mapped
 * under this store's monitor, the only lock, when no region has a free slot. Regions are only added, never removed,
 * until {@link #close}.
 * <p>
 * Used memory, reported to {@link AdaptivePoolingAllocator#chunkBufferAllocated} and
 * {@link AdaptivePoolingAllocator#chunkBufferFreed} per segment: without regions, a segment from its allocation to
 * its free. With regions, the committed segments: a slot counts from the first time it is taken to the close, whether
 * a heap holds it or it is free again; a mapped slot never taken does not count.
 */
final class PageStore {
    private static final AtomicLongFieldUpdater<PageStore> SEGMENTS_COMMITTED =
            AtomicLongFieldUpdater.newUpdater(PageStore.class, "segmentsCommitted");
    private static final Region[] NO_REGIONS = new Region[0];

    final AdaptivePoolingAllocator allocator;
    final PageStoreConfig config;
    final SegmentSource segmentSource;
    /** {@code null} when every segment is an allocation of its own. */
    final RegionSource regionSource;
    /** Replaced, one longer, under this store's monitor; read without it. */
    volatile Region[] regions = NO_REGIONS;
    private boolean closed;
    // Read by tests and dumps.
    /** Slots taken while no memory backed them: each such take costs page faults as the segment is touched. */
    volatile long segmentsCommitted;

    /** Without {@code regionSource}, or without regions in {@code config}, every segment is allocated on its own. */
    PageStore(AdaptivePoolingAllocator allocator, PageStoreConfig config, SegmentSource segmentSource,
              RegionSource regionSource) {
        this.allocator = allocator;
        this.config = config;
        this.segmentSource = segmentSource;
        this.regionSource = config.regionSize > 0 ? regionSource : null;
    }

    /**
     * A segment for {@code heap}, which owns it from now on: the lowest free slot of the fullest region (the first
     * from the heap's offset on a tie), else of a new region; without regions, a new allocation.
     */
    Segment take(HeapSegments heap) {
        Segment segment;
        if (regionSource != null) {
            segment = takeFromRegions(heap);
        } else {
            segment = new Segment(segmentSource.allocateSegment(config.segmentSize), config.sliceSize);
            allocator.chunkBufferAllocated(segment, true, heap.isThreadLocal());
        }
        segment.owner = heap;
        return segment;
    }

    private Segment takeFromRegions(HeapSegments heap) {
        for (;;) {
            Region[] regions = this.regions;
            int n = regions.length;
            Region fullest = null;
            int fullestFree = Integer.MAX_VALUE;
            int i = n == 0 ? 0 : heap.regionOffset % n;
            for (int k = 0; k < n; k++, i++) {
                if (i == n) {
                    i = 0;
                }
                Region region = regions[i];
                int free = Long.bitCount(region.free);
                if (free != 0 && free < fullestFree) {
                    fullest = region;
                    fullestFree = free;
                    if (free == 1) {
                        break;
                    }
                }
            }
            if (fullest == null) {
                addRegion(regions);
                continue;
            }
            int slot = fullest.takeSlot();
            if (slot < 0) {
                continue; // taken meanwhile: look again
            }
            Segment segment = fullest.segment(slot, segmentSource, config);
            if (!fullest.committed[slot]) {
                fullest.committed[slot] = true;
                SEGMENTS_COMMITTED.incrementAndGet(this);
                allocator.chunkBufferAllocated(segment, true, heap.isThreadLocal());
            }
            return segment;
        }
    }

    /** Maps a new region, unless one was added since {@code seen} was read. Three system calls. */
    private synchronized void addRegion(Region[] seen) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        if (regions != seen) {
            return;
        }
        AbstractByteBuf buffer = regionSource.allocateRegion(config.regionSize, config.regionAlignment);
        assert buffer.capacity() == config.regionSize;
        Region[] grown = Arrays.copyOf(seen, seen.length + 1);
        grown[seen.length] = new Region(buffer, config.segmentsPerRegion());
        regions = grown;
    }

    /**
     * {@code segment}, wholly free and owned by no heap, goes back to its region's free slots, or to its source,
     * which may return memory to the OS.
     */
    void free(Segment segment) {
        assert segment.isWhollyFree() && segment.owner == null;
        if (segment.region != null) {
            segment.region.giveBack(segment.slot);
            return;
        }
        allocator.chunkBufferFreed(segment, true);
        segment.buffer.release();
    }

    /**
     * Unmaps every region, with whatever segments are still in heaps: only when nothing can touch them any more
     * (the allocator is unreachable). Takes nothing from then on.
     */
    synchronized void close() {
        closed = true;
        Region[] regions = this.regions;
        this.regions = NO_REGIONS;
        for (Region region : regions) {
            for (int slot = 0; slot < region.slots; slot++) {
                if (region.committed[slot]) {
                    region.committed[slot] = false;
                    allocator.chunkBufferFreed(region.segmentOrNull(slot), true);
                }
            }
            region.buffer.release();
        }
    }

    int regionCount() {
        return regions.length;
    }
}
