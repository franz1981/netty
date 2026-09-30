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

import io.netty.buffer.AdaptivePoolingAllocator.HeapSegments;
import io.netty.buffer.AdaptivePoolingAllocator.IdleDecay;
import io.netty.buffer.AdaptivePoolingAllocator.PageStoreConfig;
import io.netty.buffer.AdaptivePoolingAllocator.Region;
import io.netty.buffer.AdaptivePoolingAllocator.RegionPool;
import io.netty.buffer.AdaptivePoolingAllocator.RegionSource;
import io.netty.buffer.AdaptivePoolingAllocator.Segment;
import io.netty.buffer.AdaptiveSegmentsTest.CountingSegmentSource;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.AdaptivePoolingAllocator.SLICE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Segments carved out of regions shared by all the heaps of an allocator ({@link RegionPool}): where a segment comes
 * from, where it goes back, when a region is freed, how the memory is accounted, and the region parameters.
 */
public class AdaptiveSegmentRegionsTest {
    private static final int MIB = 1024 * 1024;
    private static final int SEGMENT = 4 * MIB;
    private static final int REGION = 36 * MIB;
    private static final int ALIGNMENT = 2 * MIB;
    private static final long INTERVAL = IdleDecay.DECAY_INTERVAL_NANOS;

    /** Regions from the direct allocator's malloc source, counted. */
    static final class CountingRegionSource implements RegionSource {
        private final RegionSource delegate =
                new AdaptiveByteBufAllocator.MallocRegionSource(UnpooledByteBufAllocator.DEFAULT);
        final List<AbstractByteBuf> regions = new ArrayList<AbstractByteBuf>();
        final List<Integer> allocatedBytes = new ArrayList<Integer>();

        @Override
        public synchronized AbstractByteBuf allocateRegion(int size, int alignment) {
            AbstractByteBuf region = delegate.allocateRegion(size, alignment);
            regions.add(region);
            allocatedBytes.add(delegate.allocatedBytes(region));
            return region;
        }

        @Override
        public int allocatedBytes(AbstractByteBuf region) {
            return delegate.allocatedBytes(region);
        }

        synchronized int live() {
            int live = 0;
            for (AbstractByteBuf region : regions) {
                live += region.refCnt() > 0 ? 1 : 0;
            }
            return live;
        }

        synchronized long unreleasedBytes() {
            long bytes = 0;
            for (int i = 0; i < regions.size(); i++) {
                bytes += regions.get(i).refCnt() > 0 ? allocatedBytes.get(i) : 0;
            }
            return bytes;
        }
    }

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    private AdaptivePoolingAllocator newAllocator(int cacheBytes, int regionSize, int alignment) {
        return new AdaptivePoolingAllocator(segments, true, segments, regions,
                new PageStoreConfig(SEGMENT, SLICE_SIZE, cacheBytes, INTERVAL, 0.5, regionSize, alignment));
    }

    private void assertAccounted(AdaptivePoolingAllocator allocator) {
        assertEquals(segments.unreleasedBytes() + regions.unreleasedBytes(), allocator.usedMemory(),
                "usedMemory() and what the sources handed out disagree");
    }

    private static boolean addressesReadable() {
        return PlatformDependent.hasUnsafe() || PlatformDependent.hasAlignDirectByteBuffer();
    }

    /**
     * The first segments come from one region, slot after slot, and are views of it: no segment is allocated on its
     * own. The used memory is the region, for what was allocated for it. A tenth segment needs a second region.
     */
    @Test
    void segmentsComeFromRegions() {
        AdaptivePoolingAllocator allocator = newAllocator(64 * MIB, REGION, ALIGNMENT);
        RegionPool pool = allocator.regionPool;
        assertNotNull(pool);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < 9; i++) {
            Segment segment = heap.claim(63);
            taken.add(segment);
            assertNotNull(segment.region);
            assertEquals(i, segment.slot);
            assertSame(taken.get(0).region, segment.region);
            assertEquals(1, pool.regionCount());
        }
        assertEquals(0, segments.segmentsAllocated(), "no segment of its own");
        assertEquals(1, regions.regions.size());
        Region region = taken.get(0).region;
        assertEquals(regions.allocatedBytes.get(0).intValue(), region.capacity());
        assertTrue(region.capacity() == REGION || region.capacity() == REGION + ALIGNMENT, "" + region.capacity());
        assertEquals(region.capacity(), allocator.usedMemory());
        assertEquals(0, region.freeSlots);
        long base = region.buffer.memoryAddress();
        for (Segment segment : taken) {
            assertEquals(base + (long) segment.slot * SEGMENT, segment.memoryAddress());
            segment.buffer.setLong(SEGMENT - 8, segment.slot);
        }
        for (Segment segment : taken) {
            assertEquals(segment.slot, region.buffer.getLong(segment.slot * SEGMENT + SEGMENT - 8));
        }
        Segment tenth = heap.claim(63);
        assertNotSame(region, tenth.region);
        assertEquals(0, tenth.slot);
        assertEquals(2, pool.regionCount());
        assertEquals(2, regions.regions.size());
        assertAccounted(allocator);
    }

    /** With alignment, a region starts at a multiple of 2 MiB, and so does each of its (4 MiB) segments. */
    @Test
    void regionsAreAligned() {
        org.junit.jupiter.api.Assumptions.assumeTrue(addressesReadable());
        AdaptivePoolingAllocator allocator = newAllocator(0, REGION, ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        for (int i = 0; i < 3; i++) {
            Segment segment = heap.claim(63);
            assertEquals(0, segment.memoryAddress() & (ALIGNMENT - 1), "segment " + i);
        }
        assertEquals(0, allocator.regionPool.regions[0].buffer.memoryAddress() & (ALIGNMENT - 1));
        assertAccounted(allocator);
    }

    /**
     * A segment comes from the fullest region with a free slot, its lowest free slot; the oldest of equally full
     * regions wins. Without a cache, a segment that empties goes straight back to its region, and a region whose
     * segments are all back is freed at once.
     */
    @Test
    void fullestRegionFirstAndFreedWhenAllBack() {
        AdaptivePoolingAllocator allocator = newAllocator(0, REGION, ALIGNMENT);
        RegionPool pool = allocator.regionPool;
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < 18; i++) {
            taken.add(heap.claim(63));
        }
        assertEquals(2, pool.regionCount());
        Region a = taken.get(0).region;
        Region b = taken.get(9).region;
        assertNotSame(a, b);
        // a: slots 2, 4, 6 back (3 free); b: slots 1, 3, 5, 7, 8 back (5 free).
        for (int slot : new int[] {2, 4, 6}) {
            heap.release(taken.get(slot), 0, 63);
        }
        for (int slot : new int[] {1, 3, 5, 7, 8}) {
            heap.release(taken.get(9 + slot), 0, 63);
        }
        assertEquals(3, a.freeSlotCount());
        assertEquals(5, b.freeSlotCount());
        Segment next = heap.claim(63);
        assertSame(a, next.region, "a is fuller");
        assertEquals(2, next.slot, "its lowest free slot");
        heap.claim(63); // a: slot 4, 1 free left
        assertEquals(1, a.freeSlotCount());
        // Four more of a back: a and b both have 5 free slots.
        heap.release(taken.get(0), 0, 63);
        heap.release(taken.get(1), 0, 63);
        heap.release(taken.get(3), 0, 63);
        heap.release(taken.get(5), 0, 63);
        assertEquals(5, a.freeSlotCount());
        assertEquals(5, b.freeSlotCount());
        Segment tie = heap.claim(63);
        assertSame(a, tie.region, "equally full: the older region");
        assertEquals(0, tie.slot);
        heap.release(tie, 0, 63);
        assertEquals(2, regions.live());

        // b drains: once its last segment is back, it is freed, and accounted as such.
        long before = allocator.usedMemory();
        for (int slot : new int[] {0, 2, 4, 6}) {
            heap.release(taken.get(9 + slot), 0, 63);
        }
        assertEquals(1, pool.regionCount());
        assertEquals(1, regions.live());
        assertEquals(0, b.buffer.refCnt());
        assertEquals(before - b.capacity(), allocator.usedMemory());
        assertEquals(1, pool.freed);
        assertAccounted(allocator);
    }

    /**
     * With a cache, segments that empty wait there and the region stays; the cache's ageing gives them back to the
     * region by halves, and the region is freed with the last of them. Decays are forced, one interval apart.
     */
    @Test
    void regionFreedOnceTheCacheAgedAllItsSegmentsOut() {
        AdaptivePoolingAllocator allocator = newAllocator(64 * MIB, REGION, ALIGNMENT);
        RegionPool pool = allocator.regionPool;
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < 3; i++) {
            taken.add(heap.claim(63));
        }
        Region region = taken.get(0).region;
        for (Segment segment : taken) {
            heap.release(segment, 0, 63);
        }
        assertEquals(3, allocator.segmentCache.size());
        assertEquals(1, pool.regionCount(), "the cache holds its segments: not back yet");
        assertEquals(region.capacity(), allocator.usedMemory());
        assertEquals(6, region.freeSlotCount());
        long now = System.nanoTime() + INTERVAL;
        heap.decay(now); // starts the interval in which the three are cold
        assertEquals(3, allocator.segmentCache.size());
        heap.decay(now += INTERVAL);
        assertEquals(1, allocator.segmentCache.size(), "two of three back");
        assertEquals(8, region.freeSlotCount());
        assertEquals(1, pool.regionCount());
        assertEquals(1, region.buffer.refCnt());
        assertAccounted(allocator);
        heap.decay(now += INTERVAL);
        assertEquals(0, allocator.segmentCache.size());
        assertEquals(0, pool.regionCount(), "the last segment back frees the region");
        assertEquals(0, region.buffer.refCnt());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(allocator);
    }

    /** A cache of 0 bytes: a segment that empties goes back at once, and so does its region if it was the last. */
    @Test
    void noCacheFreesSegmentAndRegionAtOnce() {
        AdaptivePoolingAllocator allocator = newAllocator(0, REGION, ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        Segment a = heap.claim(10);
        Segment b = heap.claim(60);
        assertSame(a.region, b.region);
        heap.release(a, 0, 10);
        assertEquals(1, regions.live(), "b is still out");
        assertEquals(8, a.region.freeSlotCount());
        heap.release(b, 0, 60);
        assertEquals(0, regions.live());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(allocator);
    }

    /** A segment taken again from its region is the same one, wholly free, with its span buffers. */
    @Test
    void aSegmentBackInItsRegionIsReused() {
        AdaptivePoolingAllocator allocator = newAllocator(0, REGION, ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        Segment keep = heap.claim(63); // keeps the region
        Segment a = heap.claim(63);
        heap.release(a, 0, 63);
        assertNull(a.owner);
        Segment again = heap.claim(63);
        assertSame(a, again);
        assertSame(heap, again.owner);
        heap.release(again, 0, 63);
        heap.release(keep, 0, 63);
        assertEquals(0, regions.live());
        assertAccounted(allocator);
    }

    /** Buffers of the size classes through the allocator's own heaps, with regions: data lands inside the region. */
    @Test
    void sizeClassBuffersLiveInRegions() {
        AdaptivePoolingAllocator allocator = newAllocator(64 * MIB, REGION, ALIGNMENT);
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
            assertTrue(buf.memoryAddress() >= base && buf.memoryAddress() + size <= base + REGION);
        }
        assertEquals(1, regions.regions.size());
        assertEquals(0, segments.segmentsAllocated());
        assertAccounted(allocator);
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(allocator);
    }

    /** Regions off: a config without regions, or no region source, allocates one buffer per segment as before. */
    @Test
    void regionsOffAllocatesSegmentsOneByOne() {
        AdaptivePoolingAllocator noRegions = newAllocator(0, 0, 0);
        assertNull(noRegions.regionPool);
        AdaptivePoolingAllocator noSource = new AdaptivePoolingAllocator(segments, true, segments, null,
                new PageStoreConfig(SEGMENT, SLICE_SIZE, 0, INTERVAL, 0.5, REGION, ALIGNMENT));
        assertNull(noSource.regionPool);
        for (AdaptivePoolingAllocator allocator : new AdaptivePoolingAllocator[] {noRegions, noSource}) {
            HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
            Segment segment = heap.claim(10);
            assertNull(segment.region);
            assertEquals(SEGMENT, allocator.usedMemory());
            heap.release(segment, 0, 10);
            assertEquals(0, allocator.usedMemory());
        }
        assertEquals(2, segments.segmentsAllocated());
        assertEquals(0, regions.regions.size());
    }

    /** The property: 0, or a multiple of the segment size above 32 MiB and at most 64 segments; else the default. */
    @Test
    void badRegionSizesFallBackToTheDefault() {
        int fallback = AdaptivePoolingAllocator.defaultRegionSize(SEGMENT);
        assertEquals(36 * MIB, fallback);
        assertEquals(34 * MIB, AdaptivePoolingAllocator.defaultRegionSize(2 * MIB));
        assertEquals(0, AdaptivePoolingAllocator.regionSizeOf(0, SEGMENT));
        assertEquals(36 * MIB, AdaptivePoolingAllocator.regionSizeOf(36 * MIB, SEGMENT));
        assertEquals(40 * MIB, AdaptivePoolingAllocator.regionSizeOf(40 * MIB, SEGMENT));
        assertEquals(256 * MIB, AdaptivePoolingAllocator.regionSizeOf(256 * MIB, SEGMENT));
        boolean lowMemory = AdaptivePoolingAllocator.SEGMENT_REGION_SIZE == 0
                && System.getProperty("io.netty.allocator.segmentRegionSize") == null;
        int expected = lowMemory ? 0 : fallback;
        for (int bad : new int[] {-1, 4 * MIB, 32 * MIB, 34 * MIB, 260 * MIB}) {
            assertEquals(expected, AdaptivePoolingAllocator.regionSizeOf(bad, SEGMENT), "" + bad);
        }
        // The config itself: 2 to 64 whole segments, a power-of-two alignment.
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT, SLICE_SIZE, 0, INTERVAL, 0.5, SEGMENT, ALIGNMENT));
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT, SLICE_SIZE, 0, INTERVAL, 0.5, 65 * SEGMENT, ALIGNMENT));
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT, SLICE_SIZE, 0, INTERVAL, 0.5, REGION + MIB, ALIGNMENT));
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT, SLICE_SIZE, 0, INTERVAL, 0.5, REGION, 3 * MIB));
        assertThrows(IllegalArgumentException.class,
                () -> new PageStoreConfig(SEGMENT, SLICE_SIZE, 0, INTERVAL, 0.5, -REGION, ALIGNMENT));
    }

    /**
     * Low-memory mode has no regions by default; otherwise 36 MiB regions aligned to 2 MiB, used where allocating
     * leaves the memory untouched.
     */
    @Test
    void lowMemoryModeHasNoRegions() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                System.getProperty("io.netty.allocator.segmentRegionSize") == null, "set explicitly");
        java.lang.reflect.Field f = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        f.setAccessible(true);
        boolean lowMemory = f.getBoolean(null);
        PageStoreConfig direct = PageStoreConfig.directDefaults();
        assertEquals(lowMemory ? 0 : REGION, direct.regionSize);
        assertEquals(ALIGNMENT, direct.regionAlignment);
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true, false);
        assertEquals(!lowMemory && PlatformDependent.directAllocationLeavesMemoryUntouched(),
                AdaptiveByteBufAllocatorTest.directRegions(allocator));
    }

    /**
     * A segment of a freed heap emptied by another thread's release: with no cache it goes straight to its region
     * from that thread, which frees the region when it was the last one out.
     */
    @Test
    void foreignReleaseGivesTheSegmentBackToItsRegion() throws Exception {
        final AdaptivePoolingAllocator allocator = newAllocator(0, REGION, ALIGNMENT);
        final HeapSegments heap = new HeapSegments(allocator, null, Thread.currentThread());
        final Segment segment = heap.claim(10);
        heap.markFreed();
        heap.afterFree();
        Thread releaser = new Thread(() -> heap.release(segment, 0, 10));
        releaser.start();
        releaser.join();
        assertEquals(0, regions.live());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(allocator);
    }
}
