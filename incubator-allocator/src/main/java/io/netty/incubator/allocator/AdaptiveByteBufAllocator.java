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
package io.netty.incubator.allocator;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.util.concurrent.Ticker;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.function.Function;

/**
 * A pooling {@link ByteBufAllocator} on a shared page store, with size classes carved out of its pages.
 * <p>
 * <strong>Note:</strong> this allocator is <strong>experimental</strong>, and so is this module's API. It is
 * recommended to roll out usage slowly, and to carefully monitor application performance in the process.
 * <p>
 * See the {@link AdaptivePoolingAllocator} class documentation for implementation details.
 */
public final class AdaptiveByteBufAllocator extends AbstractByteBufAllocator
        implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AdaptiveByteBufAllocator.class);
    private static final boolean DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS;

    static {
        DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS = SystemPropertyUtil.getBoolean(
                "io.netty.allocator.pagestore.useCachedMagazinesForNonEventLoopThreads", false);
        logger.debug("-Dio.netty.allocator.pagestore.useCachedMagazinesForNonEventLoopThreads: {}",
                DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
    }

    private final AdaptivePoolingAllocator direct;
    private final AdaptivePoolingAllocator heap;

    /** An allocator that prefers direct buffers unless {@code -Dio.netty.noPreferDirect=true} says otherwise. */
    public AdaptiveByteBufAllocator() {
        this(!PlatformDependent.isExplicitNoPreferDirect());
    }

    /** An allocator whose buffers are direct if {@code preferDirect}. */
    public AdaptiveByteBufAllocator(boolean preferDirect) {
        this(preferDirect, DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
    }

    /**
     * An allocator whose buffers are direct if {@code preferDirect}; {@code useCacheForNonEventLoopThreads} gives
     * the threads that are not event loop threads a heap of their own too, instead of the shared stripes.
     */
    public AdaptiveByteBufAllocator(boolean preferDirect, boolean useCacheForNonEventLoopThreads) {
        this(preferDirect, useCacheForNonEventLoopThreads, PageStoreConfig.directDefaults(),
                PageStoreConfig.heapDefaults(), defaultMmapSource(PageStoreConfig.directDefaults()),
                Ticker.systemTicker());
    }

    /**
     * @param mmap makes the source of the direct regions from this allocator, whose roots it reports; {@code null}, or
     *             returning {@code null}: regions of one block each, from the memory source
     * @param ticker the clock of both page stores: every stamp of freed memory and every decay or purge due time
     */
    AdaptiveByteBufAllocator(boolean preferDirect, boolean useCacheForNonEventLoopThreads,
                             PageStoreConfig directConfig, PageStoreConfig heapConfig,
                             Function<? super AdaptiveByteBufAllocator, ? extends MmapRegionSource> mmap,
                             Ticker ticker) {
        super(preferDirect);
        direct = new AdaptivePoolingAllocator(new DirectChunkAllocator(this), useCacheForNonEventLoopThreads,
                mmap == null ? null : mmap.apply(this), directConfig, ticker);
        heap = new AdaptivePoolingAllocator(new HeapChunkAllocator(this), useCacheForNonEventLoopThreads, null,
                heapConfig, ticker);
    }

    /**
     * mmap regions where asked for and libc can be bound (Java 23+ on Linux with native access), else
     * one-block regions from the memory source.
     */
    private static Function<AdaptiveByteBufAllocator, MmapRegionSource> defaultMmapSource(
            PageStoreConfig directConfig) {
        if (!PageStoreConfig.MMAP) {
            logger.debug("Direct regions: one block each from malloc, "
                    + "-Dio.netty.allocator.pagestore.mmap is not true");
        } else if (directConfig.regionSize <= 0) {
            logger.debug("Direct regions: one block each from malloc, "
                    + "-Dio.netty.allocator.pagestore.segmentRegionSize is 0");
        } else if (PlatformDependent.usedDirectMemory() < 0) {
            // Mapped slices are charged to Netty's counter only, and the JDK's limit never sees a mapping.
            logger.debug("Direct regions: one block each from malloc, Netty does not count direct memory "
                    + "(-Dio.netty.maxDirectMemory=0 or no limit known)");
        } else if (!MmapRegionSource.isAvailable()) {
            logger.debug("Direct regions: one block each from malloc, mmap(2) is not available: {}",
                    LinuxMmapLinker.unavailabilityCause().toString());
        } else {
            logger.debug("Direct regions: {} bytes each, mmap(2)", directConfig.regionSize);
            MmapRegionSource.warnIfKernelPredates617();
            return new Function<AdaptiveByteBufAllocator, MmapRegionSource>() {
                @Override
                public MmapRegionSource apply(AdaptiveByteBufAllocator allocator) {
                    return new MmapRegionSource(allocator);
                }
            };
        }
        return null;
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

    /** As {@link AdaptivePoolingAllocator#close()}, for both allocators: no live buffer of either. */
    void close() {
        try {
            direct.close();
        } finally {
            heap.close();
        }
    }

    /** Heap chunk buffers, and the blocks the size classes carve their chunks out of: one {@code byte[]} each. */
    private static final class HeapChunkAllocator implements MemorySource {
        private final ByteBufAllocator allocator;

        private HeapChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            return Roots.heap(allocator, initialCapacity, maxCapacity);
        }
    }

    /**
     * Direct chunk buffers, and the blocks the size classes carve their chunks out of: both are
     * {@link PlatformDependent#allocateDirect(int, boolean)}. A one-shot root keeps its buffer's {@code maxCapacity}:
     * only {@code copy()} on the root reads it.
     */
    private static final class DirectChunkAllocator implements MemorySource {
        private final ByteBufAllocator allocator;

        private DirectChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            return Roots.direct(allocator, initialCapacity, maxCapacity);
        }
    }
}
