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
import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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
     * Direct or heap segments counted as they are allocated and freed; also the chunk allocator of the allocator under
     * test, for the chunks that are not carved from segments. The heap one does what the heap allocator's does: a
     * {@code byte[]} per segment or chunk, and a span is the segment itself.
     */
    static final class CountingSegmentSource
            implements SegmentSource, AdaptivePoolingAllocator.ChunkAllocator {
        final boolean heap;
        final List<AbstractByteBuf> segments = new ArrayList<AbstractByteBuf>();
        final List<AbstractByteBuf> chunks = new ArrayList<AbstractByteBuf>();

        CountingSegmentSource() {
            this(false);
        }

        CountingSegmentSource(boolean heap) {
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

        @Override
        public synchronized AbstractByteBuf allocateSegment(int size) {
            AbstractByteBuf buf = newBuffer(size, size);
            segments.add(buf);
            return buf;
        }

        @Override
        public AbstractByteBuf span(AbstractByteBuf segment, int offset, int length) {
            return heap ? segment :
                    AdaptiveByteBufAllocator.directSpan(UnpooledByteBufAllocator.DEFAULT, segment, offset, length);
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
        final List<AbstractByteBuf> regions = new ArrayList<AbstractByteBuf>();
        /** Regions given back, in call order. */
        final List<AbstractByteBuf> released = new ArrayList<AbstractByteBuf>();
        /** Runs inside each release, before it releases, when set. */
        volatile Consumer<AbstractByteBuf> onRelease;

        CountingRegionSource() {
            this(false);
        }

        CountingRegionSource(boolean malloc) {
            this.malloc = malloc;
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
        return buf.memoryAddress() - segment.memoryAddress();
    }

    static AdaptivePoolingAllocator newAllocator(CountingSegmentSource source, int segmentSize) {
        return new AdaptivePoolingAllocator(source, true, source,
                new PageStoreConfig(segmentSize, SLICE_SIZE_BYTES, INTERVAL));
    }

    static AdaptivePoolingAllocator newAllocator(CountingSegmentSource segments, CountingRegionSource regions,
                                                 int regionSize, int alignment) {
        return new AdaptivePoolingAllocator(segments, true, segments, regions,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, regionSize, alignment));
    }

    /** With shared slices: every chunk is a run of the regions' shared slices (see {@link PageStore}). */
    static AdaptivePoolingAllocator newSharedAllocator(CountingSegmentSource segments, RegionSource regions,
                                                       int regionSize, long purgeDelayNanos) {
        // A region of one block is a malloc'd one's size.
        PageStoreConfig config = regionSize == SEGMENT_SIZE ? PageStoreConfig.sharedSlices(SEGMENT_SIZE,
                SLICE_SIZE_BYTES, purgeDelayNanos, 0, 0, SEGMENT_SIZE).withMallocRegions() :
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, purgeDelayNanos, regionSize, REGION_ALIGNMENT, true);
        return new AdaptivePoolingAllocator(segments, true, segments, regions, config);
    }

    /** Shared slices: the used memory is the segments allocated on their own plus the committed slices. */
    static void assertSharedAccounted(CountingSegmentSource segments, AdaptivePoolingAllocator allocator) {
        PageStore store = allocator.pageStore;
        long stored = 0;
        if (store.purgesSlices) {
            stored = committedSlices(store) * (long) SLICE_SIZE_BYTES;
        } else {
            // malloc'd regions count whole, from their allocation to their release.
            for (Region region : store.regions) {
                stored += region.released ? 0 : store.config.regionSize;
            }
        }
        assertEquals(segments.unreleasedBytes() + stored, allocator.usedMemory(),
                "usedMemory() and the committed slices disagree");
    }

    static int committedSlices(PageStore store) {
        int[] counts = store.sliceCounts();
        int committed = counts[0] + counts[1];
        // A claimed slice always has memory behind it.
        int behind = 0;
        for (Region region : store.regions) {
            if (region.released) {
                continue;
            }
            for (int slot = 0; slot < region.slots; slot++) {
                for (long freedAt : region.block(slot).freedAt) {
                    behind += freedAt != Region.UNCOMMITTED ? 1 : 0;
                }
            }
        }
        assertEquals(committed, behind, "a claimed slice without memory behind it");
        return committed;
    }

    /** Gives {@code segment}'s one span back, then its heap's only reserved one with two decays: back to the store. */
    static void giveBack(HeapSegments heap, Segment segment, int start, int slices) {
        heap.release(segment, start, slices);
        heap.decay(0);
        heap.decay(0);
        assertEquals(0, heap.reserved);
        assertNull(segment.owner);
    }

    static void assertAccounted(CountingSegmentSource source, AdaptivePoolingAllocator allocator) {
        assertEquals(source.unreleasedBytes(), allocator.usedMemory(), "usedMemory() and the segment source disagree");
    }

    /**
     * The used memory is the segments allocated on their own and not freed, plus the committed slots of the regions:
     * see {@link PageStore}.
     */
    static void assertAccounted(CountingSegmentSource segments, CountingRegionSource regions,
                                AdaptivePoolingAllocator allocator) {
        assertEquals(segments.unreleasedBytes() + committedSlots(allocator.pageStore) * (long) SEGMENT_SIZE,
                allocator.usedMemory(), "usedMemory() and what the sources handed out disagree");
    }

    static int committedSlots(PageStore store) {
        int committed = 0;
        for (Region region : store.regions) {
            for (long freedAt : region.freedAt) {
                committed += freedAt != Region.UNCOMMITTED ? 1 : 0;
            }
        }
        return committed;
    }
}
