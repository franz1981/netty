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
import io.netty.buffer.PageStoreTestSupport.CountingRegionSource;
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import io.netty.util.internal.PlatformDependent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.MIB;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Real memory sources that count what they hand out, and allocators built on them: the page store's accounting goes
 * through its allocator, so every page store test has one.
 */
final class PageStoreTestSupport {
    static final int MIB = 1024 * 1024;
    static final int SEGMENT_SIZE = 4 * MIB;
    static final int REGION_SIZE = 36 * MIB;
    static final int REGION_ALIGNMENT = 2 * MIB;
    static final long INTERVAL = IdleDecay.DECAY_INTERVAL_NANOS;

    private PageStoreTestSupport() {
    }

    /**
     * Direct or heap blocks counted as they are allocated and freed, as the regions of one block of
     * {@link #mallocRegionSource()} unless {@link #fallback} is set; also the chunk allocator of the allocator under
     * test, for the chunks that are not carved from blocks. The heap one does what the heap allocator's does: a
     * {@code byte[]} per block or chunk, and a view is the block itself.
     */
    static final class CountingMemorySource implements MemorySource {
        final boolean heap;
        final List<AbstractByteBuf> segments = new ArrayList<AbstractByteBuf>();
        final List<AbstractByteBuf> chunks = new ArrayList<AbstractByteBuf>();
        /** What {@link #mallocRegionSource()} returns: by default, this source's blocks, one per region. */
        RegionSource fallback = new RegionSource() {
            @Override
            public AbstractByteBuf allocateRegion(int size, int alignment) {
                return allocateSegment(size);
            }

            @Override
            public boolean canPurgeSlices() {
                return false;
            }

            @Override
            public void purge(AbstractByteBuf region, int offset, int length) {
                throw new UnsupportedOperationException();
            }
        };

        CountingMemorySource() {
            this(false);
        }

        CountingMemorySource(boolean heap) {
            this.heap = heap;
        }

        private AbstractByteBuf newBuffer(int initialCapacity, int maxCapacity) {
            ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;
            if (!heap) {
                return UnsafeByteBufUtil.newDirectByteBuf(alloc, initialCapacity, maxCapacity);
            }
            return PlatformDependent.hasUnsafe() ? new UnpooledUnsafeHeapByteBuf(alloc, initialCapacity, maxCapacity) :
                    new UnpooledHeapByteBuf(alloc, initialCapacity, maxCapacity);
        }

        synchronized AbstractByteBuf allocateSegment(int size) {
            AbstractByteBuf buf = newBuffer(size, size);
            segments.add(buf);
            return buf;
        }

        @Override
        public AbstractByteBuf view(AbstractByteBuf block, int offset, int length) {
            return heap ? block :
                    AdaptiveByteBufAllocator.directSpan(UnpooledByteBufAllocator.DEFAULT, block, offset, length);
        }

        @Override
        public RegionSource mallocRegionSource() {
            return fallback;
        }

        @Override
        public synchronized AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            AbstractByteBuf buf = newBuffer(initialCapacity, maxCapacity);
            chunks.add(buf);
            return buf;
        }

        synchronized int segmentsAllocated() {
            return segments.size();
        }

        synchronized int segmentsLive() {
            int live = 0;
            for (AbstractByteBuf buf : segments) {
                if (buf.refCnt() > 0) {
                    live++;
                }
            }
            return live;
        }

        /** The chunk buffers handed out and not released. */
        synchronized long unreleasedChunkBytes() {
            long bytes = 0;
            for (AbstractByteBuf buf : chunks) {
                bytes += buf.refCnt() > 0 ? buf.capacity() : 0;
            }
            return bytes;
        }

        /** What the allocator holds, seen from outside: every buffer handed out and not released. */
        synchronized long unreleasedBytes() {
            long bytes = 0;
            for (AbstractByteBuf buf : segments) {
                bytes += buf.refCnt() > 0 ? buf.capacity() : 0;
            }
            for (AbstractByteBuf buf : chunks) {
                bytes += buf.refCnt() > 0 ? buf.capacity() : 0;
            }
            return bytes;
        }
    }

    /**
     * Regions counted as they are mapped, and purges as they are called: {@code mmap} and {@code madvise} where
     * {@link MmapRegionSource} is available, else plain direct buffers (untouched {@code malloc} memory, unaligned)
     * whose purged ranges are zeroed.
     */
    static final class CountingRegionSource implements RegionSource {
        final MmapRegionSource mmap;
        /** Whether this stands for {@link MallocRegionSource}: whole regions only, charged by their allocation. */
        final boolean malloc;
        final boolean heap;
        final List<AbstractByteBuf> regions = new ArrayList<AbstractByteBuf>();
        /** Regions given back, in call order. */
        final List<AbstractByteBuf> released = new ArrayList<AbstractByteBuf>();
        /** Runs inside each release, before it releases, when set. */
        volatile Consumer<AbstractByteBuf> onRelease;

        CountingRegionSource() {
            this(false);
        }

        CountingRegionSource(boolean malloc) {
            this(malloc, false);
        }

        /** {@code heap}: whole regions only, each a {@code byte[]}, as the heap allocator's. */
        CountingRegionSource(boolean malloc, boolean heap) {
            assert malloc || !heap;
            this.malloc = malloc;
            this.heap = heap;
            mmap = !malloc && MmapRegionSource.isAvailable() ? new MmapRegionSource(UnpooledByteBufAllocator.DEFAULT) :
                    null;
        }

        @Override
        public boolean canPurgeSlices() {
            return !malloc;
        }

        @Override
        public void releaseRegion(AbstractByteBuf region) {
            Consumer<AbstractByteBuf> hook = onRelease;
            if (hook != null) {
                hook.accept(region);
            }
            synchronized (this) {
                released.add(region);
            }
            region.release();
        }
        /** {offset, length} of each purge call, in call order. */
        final List<int[]> purges = new ArrayList<int[]>();
        /** Runs inside each purge call, before it purges, when set. */
        volatile Runnable onPurge;

        @Override
        public void purge(AbstractByteBuf region, int offset, int length) {
            if (malloc) {
                throw new UnsupportedOperationException("malloc: no purge");
            }
            Runnable hook = onPurge;
            if (hook != null) {
                hook.run();
            }
            synchronized (this) {
                purges.add(new int[] {offset, length});
            }
            if (mmap != null) {
                mmap.purge(region, offset, length);
            } else {
                region.setZero(offset, length);
            }
        }

        synchronized int purgeCalls() {
            return purges.size();
        }

        @Override
        public synchronized AbstractByteBuf allocateRegion(int size, int alignment) {
            AbstractByteBuf region = mmap != null ? mmap.allocateRegion(size, alignment) :
                    heap ? new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, size, size) :
                    UnsafeByteBufUtil.newDirectByteBuf(UnpooledByteBufAllocator.DEFAULT, size, size);
            regions.add(region);
            return region;
        }

        synchronized int live() {
            int live = 0;
            for (AbstractByteBuf region : regions) {
                live += region.refCnt() > 0 ? 1 : 0;
            }
            return live;
        }
    }

    /**
     * Where {@code buf}'s memory starts in {@code segment}, which holds it: by address for direct memory, by array
     * offset for heap memory, which must be the segment's own array.
     */
    static long offsetIn(ByteBuf buf, Segment segment) {
        if (segment.buffer.hasArray()) {
            assertSame(segment.buffer.array(), buf.array(), "not the segment's array");
            return buf.arrayOffset() - segment.buffer.arrayOffset();
        }
        return buf.memoryAddress() - segment.buffer.memoryAddress();
    }

    /**
     * Purge passes from {@code now} on, a cadence floor apart, as long as one is due: all that a pass, which stops at
     * {@link PageStore#PURGE_BYTES}, leaves to the next ones.
     */
    static void purgeUntilDone(PageStore store, long now) {
        for (int i = 0; i < 10000; i++) {
            long passes = store.purges;
            store.purgeIfDue(now);
            if (store.purges == passes) {
                return;
            }
            now += store.config.purgeCheckNanos;
        }
        throw new AssertionError("the purge never stops");
    }

    /** With regions of one block of {@code segmentSize}, from {@code source}: its segments are those blocks. */
    static AdaptivePoolingAllocator newAllocator(CountingMemorySource source, int segmentSize) {
        return new AdaptivePoolingAllocator(source, true, null,
                new PageStoreConfig(segmentSize, SLICE_SIZE_BYTES, INTERVAL, 0, 0, segmentSize));
    }

    static AdaptivePoolingAllocator newAllocator(CountingMemorySource segments, CountingRegionSource regions,
                                                 int regionSize, int alignment) {
        return new AdaptivePoolingAllocator(segments, true, regions,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, regionSize, alignment));
    }

    /** With regions of {@code regionSize} from {@code regions}: every chunk is a run of their shared slices. */
    static AdaptivePoolingAllocator newSharedAllocator(CountingMemorySource segments, RegionSource regions,
                                                       int regionSize, long purgeDelayNanos) {
        // A region of one block is a malloc'd one's size.
        PageStoreConfig config = regionSize == SEGMENT_SIZE ?
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, purgeDelayNanos, 0, 0, SEGMENT_SIZE)
                        .withMallocRegions() :
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, purgeDelayNanos, regionSize, REGION_ALIGNMENT);
        return new AdaptivePoolingAllocator(segments, true, regions, config);
    }

    /**
     * The used memory is the chunk buffers allocated on their own, plus the committed slices of the regions that purge
     * them, plus the other regions whole, from their allocation to their release.
     */
    static void assertSharedAccounted(CountingMemorySource segments, AdaptivePoolingAllocator allocator) {
        PageStore store = allocator.pageStore;
        long stored = 0;
        for (Region region : store.regions) {
            if (!region.released) {
                stored += region.purgesSlices ? committedSlices(region) * (long) SLICE_SIZE_BYTES : region.length;
            }
        }
        assertEquals(segments.unreleasedChunkBytes() + stored, allocator.usedMemory(),
                "usedMemory() and the committed slices disagree");
    }

    /** The slices of {@code region} with memory behind them; a claimed one always has. */
    private static int committedSlices(Region region) {
        int committed = 0;
        for (int slot = 0; slot < region.slots; slot++) {
            Segment block = region.blocks[slot];
            long behind = block.committed;
            assertEquals(0, ~block.free & ~behind & block.allFree, "a claimed slice without memory behind it");
            committed += Long.bitCount(behind);
        }
        return committed;
    }

    static void assertAccounted(CountingMemorySource source, AdaptivePoolingAllocator allocator) {
        assertEquals(source.unreleasedBytes(), allocator.usedMemory(), "usedMemory() and the segment source disagree");
    }

    /** As {@link #assertSharedAccounted}: {@code regions} are counted through the store. */
    static void assertAccounted(CountingMemorySource segments, CountingRegionSource regions,
                                AdaptivePoolingAllocator allocator) {
        assertSharedAccounted(segments, allocator);
    }
}
