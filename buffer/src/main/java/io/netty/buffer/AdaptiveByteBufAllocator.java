/*
 * Copyright 2024 The Netty Project
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

import io.netty.util.internal.CleanableDirectBuffer;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;

/**
 * An auto-tuning pooling {@link ByteBufAllocator}, that follows an anti-generational hypothesis.
 * <p>
 * <strong>Note:</strong> this allocator is <strong>experimental</strong>. It is recommended to roll out usage slowly,
 * and to carefully monitor application performance in the process.
 * <p>
 * See the {@link AdaptivePoolingAllocator} class documentation for implementation details.
 */
public final class AdaptiveByteBufAllocator extends AbstractByteBufAllocator
        implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AdaptiveByteBufAllocator.class);
    private static final boolean DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS;

    static {
        DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS = SystemPropertyUtil.getBoolean(
                "io.netty.allocator.useCachedMagazinesForNonEventLoopThreads", false);
        logger.debug("-Dio.netty.allocator.useCachedMagazinesForNonEventLoopThreads: {}",
                     DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
    }

    private final AdaptivePoolingAllocator direct;
    private final AdaptivePoolingAllocator heap;

    public AdaptiveByteBufAllocator() {
        this(!PlatformDependent.isExplicitNoPreferDirect());
    }

    public AdaptiveByteBufAllocator(boolean preferDirect) {
        this(preferDirect, DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
    }

    public AdaptiveByteBufAllocator(boolean preferDirect, boolean useCacheForNonEventLoopThreads) {
        super(preferDirect);
        direct = new AdaptivePoolingAllocator(new DirectChunkAllocator(this), useCacheForNonEventLoopThreads);
        heap = new AdaptivePoolingAllocator(new HeapChunkAllocator(this), useCacheForNonEventLoopThreads);
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(heap.allocate(initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(direct.allocate(initialCapacity, maxCapacity));
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    @Override
    public long usedHeapMemory() {
        return heap.usedMemory();
    }

    @Override
    public long usedDirectMemory() {
        return direct.usedMemory();
    }

    @Override
    public ByteBufAllocatorMetric metric() {
        return this;
    }

    private static final class HeapChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private HeapChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            return PlatformDependent.hasUnsafe() ?
                    new UnpooledUnsafeHeapByteBuf(allocator, initialCapacity, maxCapacity) :
                    new UnpooledHeapByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }

    /**
     * Direct chunk buffers, and the segments the size classes carve their chunks out of: both are libc {@code malloc}
     * behind {@link UnsafeByteBufUtil#newDirectByteBuf} (or {@link java.nio.ByteBuffer#allocateDirect} without
     * {@code Unsafe}, which touches every page at once).
     */
    private static final class DirectChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator, SegmentSource {
        private final ByteBufAllocator allocator;

        private DirectChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            return UnsafeByteBufUtil.newDirectByteBuf(allocator, initialCapacity, maxCapacity);
        }

        @Override
        public AbstractByteBuf allocateSegment(int size) {
            return UnsafeByteBufUtil.newDirectByteBuf(allocator, size, size);
        }

        @Override
        public AbstractByteBuf span(AbstractByteBuf segment, int offset, int length) {
            return directSpan(allocator, segment, offset, length);
        }

        /**
         * Regions only where allocating one leaves its memory untouched: where it is zeroed at allocation
         * ({@code ByteBuffer.allocateDirect}), a region would cost its whole size at once, so segments are then
         * allocated one by one.
         */
        @Override
        public RegionSource regionSource() {
            return PlatformDependent.directAllocationLeavesMemoryUntouched() ? new MallocRegionSource(allocator) : null;
        }
    }

    /**
     * Regions from the direct chunk allocator's memory (libc {@code malloc} behind the platform's cleaner), aligned
     * through {@link PlatformDependent#allocateDirectAligned} when the platform can ({@code aligned_alloc} on the libc
     * linker cleaner, an over-allocation otherwise), unaligned when it cannot. Freed whole when released.
     */
    static final class MallocRegionSource implements RegionSource {
        private final ByteBufAllocator allocator;
        /** Set once an aligned allocation was refused: every later region is unaligned. */
        private volatile boolean cannotAlign;

        MallocRegionSource(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocateRegion(int size, int alignment) {
            if (alignment > 0 && !cannotAlign) {
                try {
                    return UnsafeByteBufUtil.newDirectByteBuf(allocator,
                            PlatformDependent.allocateDirectAligned(size, alignment));
                } catch (UnsupportedOperationException e) {
                    cannotAlign = true;
                    logger.debug("Cannot align direct regions, allocating them unaligned", e);
                }
            }
            return UnsafeByteBufUtil.newDirectByteBuf(allocator, size, size);
        }

        @Override
        public int allocatedBytes(AbstractByteBuf region) {
            CleanableDirectBuffer memory = ((UnpooledDirectByteBuf) region).cleanable;
            return memory != null ? memory.allocatedCapacity() : region.capacity();
        }
    }

    /**
     * A buffer over {@code length} bytes of {@code segment}, a buffer of {@link UnsafeByteBufUtil#newDirectByteBuf},
     * from {@code offset}: of the class of {@code segment}, so that the buffers reading a chunk see one class whatever
     * the chunk, and never freeing the memory.
     */
    static AbstractByteBuf directSpan(ByteBufAllocator allocator, AbstractByteBuf segment, int offset, int length) {
        ByteBuffer span = segment.nioBuffer(offset, length);
        if (segment instanceof UnpooledUnsafeNoCleanerDirectByteBuf) {
            return new UnpooledUnsafeNoCleanerDirectByteBuf(allocator, span, length);
        }
        if (segment instanceof UnpooledUnsafeDirectByteBuf) {
            return new UnpooledUnsafeDirectByteBuf(allocator, span, length, false);
        }
        return new UnpooledDirectByteBuf(allocator, span, length, false, false);
    }
}
