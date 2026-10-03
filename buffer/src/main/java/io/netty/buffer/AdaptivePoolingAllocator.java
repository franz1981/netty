/*
 * Copyright 2022 The Netty Project
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

import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.NettyRuntime;
import io.netty.util.Recycler;
import io.netty.util.Recycler.EnhancedHandle;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;

/**
 * A pooling allocator that follows an anti-generational hypothesis: buffers are expected to die young, so the memory
 * behind them is kept close to the thread that allocated it and handed out again as soon as it comes back.
 * <p>
 * All memory comes from the allocator's {@link PageStore}, shared by all heaps: runs of its 64 KiB slices
 * ({@code mmap}'d regions, {@code malloc}'d blocks, or {@code byte[]} blocks for heap memory). Which run serves a
 * request depends on its size:
 * <ul>
 *   <li><b>Up to the largest size class</b> ({@link #SIZE_CLASSES}): a {@link SizeClassedChunk}, cut into equal
 *       segments of one size class. Its {@link SizeClassMagazine} allocates from one chunk at a time and keeps the
 *       others in a {@link SizeClassedChunkCache}, which files them by whether they have a free segment.</li>
 *   <li><b>Above it, up to a block</b>: a span of whole slices holding that buffer alone, see
 *       {@link SpanMagazine}.</li>
 *   <li><b>Larger still</b>, or when no span can be had: a one-shot {@link OneShotChunk}, holding that buffer alone and
 *       freed with it.</li>
 * </ul>
 * <p>
 * The magazines are grouped into {@link StripedHeap}s, each guarded by one lock, and a thread picks a stripe by its
 * id; more stripes are used when threads collide on the lock. A {@link FastThreadLocalThread} instead gets a
 * {@link ThreadLocalSizeClassHeap} of its own, which needs no lock at all, for the size classes and for the buffers
 * above them; the stripes serve the other threads.
 * <p>
 * A buffer released by the thread that owns its chunk is returned to it directly. A buffer released by any other
 * thread puts its segment on the chunk's lock-free free list and leaves a note for the owner, which applies it on its
 * next slow path: the chunk's own structures are only ever touched by one thread at a time.
 * <p>
 * A size class keeps a few empty chunks; any other chunk it gives up frees its slices at once, for any heap. A chunk
 * lists its free segments in their own memory, so making one allocates no list.
 */
@UnstableApi
final class AdaptivePoolingAllocator {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AdaptivePoolingAllocator.class);
    private static final int LOW_MEM_THRESHOLD = 512 * 1024 * 1024;
    static final boolean IS_LOW_MEM = SystemPropertyUtil.getBoolean(
            "io.netty.allocator.lowMemory",
            Runtime.getRuntime().maxMemory() <= LOW_MEM_THRESHOLD);

    /**
     * Whether the IS_LOW_MEM setting should disable thread-local magazines.
     * This can have fairly high performance overhead.
     */
    private static final boolean DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM = SystemPropertyUtil.getBoolean(
            "io.netty.allocator.disableThreadLocalMagazinesOnLowMemory", true);

    /**
     * The 128 KiB minimum chunk size is chosen to encourage the system allocator to delegate to mmap for chunk
     * allocations. For instance, glibc will do this.
     * This pushes any fragmentation from chunk size deviations off physical memory, onto virtual memory,
     * which is a much, much larger space. Chunks are also allocated in whole multiples of the minimum
     * chunk size, which itself is a whole multiple of popular page sizes like 4 KiB, 16 KiB, and 64 KiB.
     */
    static final int MIN_CHUNK_SIZE = 128 * 1024;
    /**
     * To amortize activation/deactivation of chunks, a size-classed chunk holds at least this many segments.
     * We choose 32 because it seems neither too small nor too big.
     */
    private static final int MIN_SEGMENTS_PER_CHUNK = 32;
    /**
     * From this segment size up, every size class uses the chunk size of the first one of its family: 512 KiB for
     * 16, 32, 64 and 128 KiB, and 528 KiB for the four that add a header. One such chunk holds 32 segments of 16 KiB
     * down to 4 of 128 KiB: a heap pays the same for its first buffer of any of these classes, and a chunk given up
     * by one of them is reused by the other three.
     */
    private static final int MEDIUM_SEGMENT_SIZE = 16 * 1024;
    private static final AtomicIntegerFieldUpdater<AdaptivePoolingAllocator> STRIPE_SCAN_LENGTH =
            AtomicIntegerFieldUpdater.newUpdater(AdaptivePoolingAllocator.class, "stripeScanLength");
    private static final int EXPANSION_ATTEMPTS = 3;
    private static final int MAX_STRIPES = IS_LOW_MEM ? 1 :
            MathUtil.safeFindNextPositivePowerOfTwo(NettyRuntime.availableProcessors() * 2);
    private static final int INITIAL_MAGAZINES = 1;
    /** Up to this size a buffer goes to its heap directly; above it, through {@link #allocateFallback}. */
    private static final int MAX_POOLED_BUF_SIZE = IS_LOW_MEM ? 256 * 1024 : 1024 * 1024;

    /**
     * {@code io.netty.allocator.chunkPurgeInterval}: how often a size-class magazine gives up idle chunks, counted in
     * chunks' worth of allocations. After this many times the segments of one of its chunks have been allocated,
     * the magazine applies the notes left by other threads and gives up its wholly free chunks, except the ones each
     * size class in use keeps, and does the same for every other size class of its heap, including the idle ones that
     * no longer allocate. Default: 4. Read from
     * {@code io.netty.allocator.chunkPurgePollsThreadLocal} when only that, its former name, is set.
     */
    static final long CHUNK_PURGE_INTERVAL = Math.max(1, SystemPropertyUtil.getLong(
            "io.netty.allocator.chunkPurgeInterval",
            SystemPropertyUtil.getLong("io.netty.allocator.chunkPurgePollsThreadLocal", 4L)));

    /**
     * {@code io.netty.allocator.magazineBufferQueueCapacity}: how many {@link AdaptiveByteBuf} instances a stripe, and
     * the allocations that fall back to unpooled chunks, keep for reuse. This pools the buffer objects only, not
     * their memory, to save garbage. Default: 1024.
     */
    private static final int MAGAZINE_BUFFER_QUEUE_CAPACITY = SystemPropertyUtil.getInt(
            "io.netty.allocator.magazineBufferQueueCapacity", 1024);

    static {
        warnIfSet("io.netty.allocator.chunkPurgePollsThreadLocal",
                "is deprecated, use -Dio.netty.allocator.chunkPurgeInterval instead");
        warnIfSet("io.netty.allocator.chunkPurgePollsShared",
                "has no effect: use -Dio.netty.allocator.chunkPurgeInterval, which covers shared stripes too");
        warnIfSet("io.netty.allocator.chunkPurgeThreshold",
                "has no effect: see -Dio.netty.allocator.chunkPurgeInterval");
        warnIfSet("io.netty.allocator.threadLocalChunkCacheMaxBytes",
                "has no effect: a size class in use keeps up to four empty chunks");
        warnIfSet("io.netty.allocator.threadLocalChunkCacheMinBytes",
                "has no effect: a size class in use keeps up to four empty chunks");
        warnIfSet("io.netty.allocator.chunkReuseQueueCapacity",
                "has no effect: buffers above the size classes are spans of the page store, which keeps no chunks");
    }

    private static void warnIfSet(String property, String what) {
        if (SystemPropertyUtil.contains(property)) {
            logger.warn("-D{} {}", property, what);
        }
    }

    /**
     * The size classes are chosen based on the following observation:
     * <p>
     * Most allocations, particularly ones above 256 bytes, aim to be a power-of-2. However, many use cases, such
     * as framing protocols, are themselves operating or moving power-of-2 sized payloads, to which they add a
     * small amount of overhead, such as headers or checksums.
     * This means we seem to get a lot of mileage out of having both power-of-2 sizes, and power-of-2-plus-a-bit.
     * <p>
     * On the conflicting requirements of both having as few chunks as possible, and having as little wasted
     * memory within each chunk as possible, this seems to strike a surprisingly good balance for the use cases
     * tested so far.
     */
    private static final int[] SIZE_CLASSES = {
            32,
            64,
            128,
            256,
            512,
            640, // 512 + 128
            1024,
            1152, // 1024 + 128
            2048,
            2304, // 2048 + 256
            4096,
            4352, // 4096 + 256
            8192,
            8704, // 8192 + 512
            16384,
            16896, // 16384 + 512
            32768,
            33792, // 32768 + 1024
            65536,
            67584, // 65536 + 2048
            131072,
            135168, // 131072 + 4096
    };

    private static final int SIZE_CLASSES_COUNT = SIZE_CLASSES.length;
    private static final byte[] SIZE_INDEXES = new byte[SIZE_CLASSES[SIZE_CLASSES_COUNT - 1] / 32 + 1];

    static {
        if (MAGAZINE_BUFFER_QUEUE_CAPACITY < 2) {
            throw new IllegalArgumentException("MAGAZINE_BUFFER_QUEUE_CAPACITY: " + MAGAZINE_BUFFER_QUEUE_CAPACITY
                    + " (expected: >= " + 2 + ')');
        }
        int lastIndex = 0;
        for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
            int sizeClass = SIZE_CLASSES[i];
            //noinspection ConstantValue
            assert (sizeClass & 31) == 0 : "Size class must be a multiple of 32";
            int sizeIndex = sizeIndexOf(sizeClass);
            Arrays.fill(SIZE_INDEXES, lastIndex + 1, sizeIndex + 1, (byte) i);
            lastIndex = sizeIndex;
        }
    }

    /** Largest size served by a size class in low-memory mode: the size classes above it are not pooled there. */
    private static final int LOW_MEM_MAX_SIZE_CLASS = 16896;

    /** Number of size classes that are pooled: all of them, except in low-memory mode. */
    private static final int POOLED_SIZE_CLASSES_COUNT =
            IS_LOW_MEM ? sizeClassIndexOf(LOW_MEM_MAX_SIZE_CLASS) + 1 : SIZE_CLASSES_COUNT;

    private final MemorySource memory;
    private final ChunkRegistry chunkRegistry;
    private final StripedHeap[] stripedHeaps;
    private volatile int stripeScanLength;

    private final AdaptiveRecycler fallbackRecycler;
    private final FastThreadLocal<ThreadLocalSizeClassHeap> threadLocalSizeClassHeap;

    /** This allocator's distinct size-class chunk sizes, and each size class's index in it. */
    final int[] chunkSizes;
    final byte[] sizeClassToChunkPool;
    final PageStore pageStore;
    /**
     * Outside low-memory mode: the largest buffer that is a span of the page store's shared slices, a whole block;
     * above it a buffer takes a run of whole blocks. 0 in low-memory mode.
     */
    private final int largeSpanLimit;

    /**
     * @param regionSource where the regions come from, or {@code null} for {@code memory}'s (see
     *                     {@link PageStore#PageStore})
     */
    AdaptivePoolingAllocator(MemorySource memory, boolean useCacheForNonEventLoopThreads,
                             RegionSource regionSource, PageStoreConfig config) {
        checkSizeClassSpansFit(config);
        pageStore = new PageStore(this, config, memory, regionSource);
        largeSpanLimit = IS_LOW_MEM ? 0 : config.segmentSize;
        this.memory = ObjectUtil.checkNotNull(memory, "memory");
        chunkRegistry = new ChunkRegistry();
        int sliceSize = pageStore.config.sliceSize;
        int segmentSize = pageStore.config.segmentSize;
        chunkSizes = distinctChunkSizes(SIZE_CLASSES, sliceSize, segmentSize);
        sizeClassToChunkPool = chunkPools(SIZE_CLASSES, chunkSizes, sliceSize, segmentSize);
        stripedHeaps = new StripedHeap[MAX_STRIPES];
        for (int i = 0; i < MAX_STRIPES; i++) {
            stripedHeaps[i] = new StripedHeap(pageStore);
        }
        stripeScanLength = INITIAL_MAGAZINES;
        fallbackRecycler = AdaptiveRecycler.sharedWith(MAGAZINE_BUFFER_QUEUE_CAPACITY);

        boolean disableThreadLocalGroups = IS_LOW_MEM && DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM;
        threadLocalSizeClassHeap = disableThreadLocalGroups ? null : new FastThreadLocal<ThreadLocalSizeClassHeap>() {
            @Override
            protected ThreadLocalSizeClassHeap initialValue() {
                if (useCacheForNonEventLoopThreads || ThreadExecutorMap.currentExecutor() != null) {
                    return new ThreadLocalSizeClassHeap(AdaptivePoolingAllocator.this);
                }
                return null;
            }

            @Override
            protected void onRemoval(final ThreadLocalSizeClassHeap heap) throws Exception {
                if (heap != null) {
                    heap.free();
                }
            }
        };
    }

    /** A chunk is at most a segment (see {@link #chunkSizeOf(int, int, int)}): it must hold the largest class. */
    private static void checkSizeClassSpansFit(PageStoreConfig config) {
        int largest = SIZE_CLASSES[SIZE_CLASSES_COUNT - 1];
        int slices = (largest + config.sliceSize - 1) / config.sliceSize;
        if (slices * config.sliceSize > config.segmentSize) {
            throw new IllegalArgumentException("segmentSize " + config.segmentSize
                    + " cannot hold a buffer of " + largest + " (slices of " + config.sliceSize + ')');
        }
    }

    ByteBuf allocate(int size, int maxCapacity) {
        return allocate(size, maxCapacity, Thread.currentThread(), null);
    }

    private AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
        AdaptiveByteBuf allocated = null;
        if (size <= MAX_POOLED_BUF_SIZE) {
            final int index = sizeClassIndexOf(size);
            ThreadLocalSizeClassHeap heap = null;
            if (!IS_LOW_MEM && FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
                heap = threadLocalSizeClassHeap.get();
            }
            if (index < POOLED_SIZE_CLASSES_COUNT) {
                if (heap != null) {
                    allocated = heap.allocate(index, size, maxCapacity, buf);
                } else {
                    allocated = allocateShared(index, size, maxCapacity, currentThread, buf);
                }
            } else if (!IS_LOW_MEM) {
                // Above the size classes: a thread with its own heap never takes a stripe lock for these either.
                if (heap != null) {
                    allocated = heap.allocateLarge(size, maxCapacity, buf);
                } else {
                    allocated = allocateShared(index, size, maxCapacity, currentThread, buf);
                }
            }
        }
        if (allocated == null) {
            allocated = allocateFallback(size, maxCapacity, buf);
        }
        return allocated;
    }

    private AdaptiveByteBuf allocateShared(int sizeClassIndex, int size, int maxCapacity,
                                             Thread currentThread, AdaptiveByteBuf buf) {
        boolean reallocate = buf != null;
        int threadIdx = threadIndex(currentThread);
        int expansions = 0;
        int currentScanLen;
        do {
            currentScanLen = stripeScanLength;
            int mask = currentScanLen - 1;
            int start = threadIdx & mask;
            for (int i = 0, m = currentScanLen << 1; i < m; i++) {
                StripedHeap stripe = stripedHeaps[(start + i) & mask];
                AdaptiveByteBuf result = stripe.tryAllocate(
                        sizeClassIndex, size, maxCapacity, buf, reallocate, this);
                if (result != null) {
                    return result;
                }
            }
            expansions++;
        } while (expansions <= EXPANSION_ATTEMPTS && tryExpandStripeScanLength(currentScanLen));

        return null;
    }

    private boolean tryExpandStripeScanLength(int observed) {
        int current = stripeScanLength;
        if (current > observed) {
            return true;
        }
        if (current >= MAX_STRIPES) {
            return false;
        }
        STRIPE_SCAN_LENGTH.compareAndSet(this, current, current << 1);
        return true;
    }

    private static int sizeIndexOf(final int size) {
        // this is aligning the size to the next multiple of 32 and dividing by 32 to get the size index.
        return size + 31 >> 5;
    }

    /**
     * The size of the chunks of a size class: {@link #MIN_CHUNK_SIZE} up to 4 KiB segments,
     * {@link #MIN_SEGMENTS_PER_CHUNK} segments up to 16.9 KiB, and that same chunk size for the rest of the family
     * from {@link #MEDIUM_SEGMENT_SIZE} up, whose chunks hold 16, 8 and 4 segments.
     */
    static int chunkSizeOf(int segmentSize) {
        while (segmentSize >= MEDIUM_SEGMENT_SIZE << 1) {
            segmentSize >>= 1;
        }
        return Math.max(MIN_CHUNK_SIZE, segmentSize * MIN_SEGMENTS_PER_CHUNK);
    }

    /**
     * The size of the chunks of a size class, spans of {@code sliceSize} slices: {@link #chunkSizeOf} rounded up to
     * whole slices, at most {@code maxChunkSize} (a block, whole slices: heap blocks under G1 can be smaller than 9
     * slices). With 64 KiB slices, 128, 256 and 512 KiB stay; the chunks of the classes that add a header grow to
     * 192 KiB (4352), 320 KiB (8704) and 576 KiB (16896 up), and hold as many segments as fit, the tail unused: at
     * most 8.3% of the chunk, for 67584 and 135168.
     */
    static int chunkSizeOf(int segmentSize, int sliceSize, int maxChunkSize) {
        int chunkSize = chunkSizeOf(segmentSize);
        return Math.min((chunkSize + sliceSize - 1) / sliceSize * sliceSize, maxChunkSize);
    }

    /**
     * The distinct chunk sizes of {@code sizeClasses}, spans of slices of {@code sliceSize}, at most
     * {@code maxChunkSize}, in order of first appearance. Nothing is assumed about the order of the chunk sizes. Size
     * classes with the same chunk size need not be adjacent.
     */
    static int[] distinctChunkSizes(int[] sizeClasses, int sliceSize, int maxChunkSize) {
        int[] distinct = new int[sizeClasses.length];
        int count = 0;
        for (int sizeClass : sizeClasses) {
            int chunkSize = chunkSizeOf(sizeClass, sliceSize, maxChunkSize);
            if (indexOf(distinct, count, chunkSize) == -1) {
                distinct[count++] = chunkSize;
            }
        }
        return Arrays.copyOf(distinct, count);
    }

    /**
     * For each of {@code sizeClasses}, the index of its chunk size in {@code chunkSizes}, as
     * {@link #distinctChunkSizes} makes them.
     */
    static byte[] chunkPools(int[] sizeClasses, int[] chunkSizes, int sliceSize, int maxChunkSize) {
        assert chunkSizes.length <= Byte.MAX_VALUE;
        byte[] pools = new byte[sizeClasses.length];
        for (int i = 0; i < pools.length; i++) {
            int pool = indexOf(chunkSizes, chunkSizes.length, chunkSizeOf(sizeClasses[i], sliceSize, maxChunkSize));
            assert pool >= 0;
            pools[i] = (byte) pool;
        }
        return pools;
    }

    private static int indexOf(int[] values, int count, int value) {
        for (int i = 0; i < count; i++) {
            if (values[i] == value) {
                return i;
            }
        }
        return -1;
    }

    static int sizeClassIndexOf(int size) {
        int sizeIndex = sizeIndexOf(size);
        if (sizeIndex < SIZE_INDEXES.length) {
            return SIZE_INDEXES[sizeIndex];
        }
        return SIZE_CLASSES_COUNT;
    }

    static int[] getSizeClasses() {
        return SIZE_CLASSES.clone();
    }

    private AdaptiveByteBuf allocateFallback(int size, int maxCapacity, AdaptiveByteBuf buf) {
        if (size > MAX_POOLED_BUF_SIZE && size <= largeSpanLimit) {
            // Above the pooled sizes, up to a block: a span of shared slices, as smaller large buffers.
            AdaptiveByteBuf spanned = allocateLargeSpan(size, maxCapacity, buf);
            if (spanned != null) {
                return spanned;
            }
        }
        return allocateOneShot(size, maxCapacity, buf);
    }

    /** A one-shot chunk for this buffer alone; never a span, so a span magazine can fall back to it. */
    private AdaptiveByteBuf allocateOneShot(int size, int maxCapacity, AdaptiveByteBuf buf) {
        if (buf == null) {
            buf = newFallbackBuffer();
        }
        // Above a span, a run of the store's blocks if one fits; else an allocation of its own.
        OneShotChunk chunk = largeSpanLimit != 0 && size > largeSpanLimit ? newStoreOneShot(size) : null;
        if (chunk == null) {
            chunk = new OneShotChunk(memory.allocate(size, maxCapacity), this, null, 0, 0);
            chunkBufferAllocated(chunk, false, false);
        }
        boolean initialized = false;
        try {
            buf.init(chunk.delegate, chunk, 0, 0, 0, size, size, maxCapacity);
            initialized = true;
        } finally {
            if (!initialized) {
                chunk.releaseSegment(0, size);
            }
        }
        return buf;
    }

    /** A span of the calling thread's heap, or of a stripe; {@code null} when every stripe is busy. */
    private AdaptiveByteBuf allocateLargeSpan(int size, int maxCapacity, AdaptiveByteBuf buf) {
        Thread current = Thread.currentThread();
        if (FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
            ThreadLocalSizeClassHeap heap = threadLocalSizeClassHeap.get();
            if (heap != null) {
                return heap.allocateLarge(size, maxCapacity, buf);
            }
        }
        return allocateShared(SIZE_CLASSES_COUNT, size, maxCapacity, current, buf);
    }

    /**
     * A one-shot chunk from the page store for a buffer above a block: a run of whole blocks. {@code null} when no run
     * fits: a buffer larger than a new region.
     */
    private OneShotChunk newStoreOneShot(int size) {
        PageStore store = pageStore;
        // The heaps' decays drive the store's purge; these buffers count toward no heap's, so they drive it too.
        store.purgeIfDue(System.nanoTime());
        int segmentSize = store.config.segmentSize;
        int slots = (int) ((size + (long) segmentSize - 1) / segmentSize);
        long run = store.takeRun(slots);
        if (run < 0) {
            return null;
        }
        Region region = store.regions[(int) (run >>> 32)];
        int start = (int) run;
        boolean made = false;
        try {
            AbstractByteBuf view = store.memory.view(region.buffer, start * segmentSize, slots * segmentSize);
            OneShotChunk chunk = new OneShotChunk(view, this, region, start, slots);
            made = true;
            return chunk;
        } finally {
            if (!made) {
                store.freeRun(region, start, slots);
            }
        }
    }

    private AdaptiveByteBuf newFallbackBuffer() {
        AdaptiveByteBuf buf = fallbackRecycler.get();
        buf.resetRefCnt();
        buf.discardMarks();
        return buf;
    }

    /**
     * Allocate into the given buffer. Used by {@link AdaptiveByteBuf#capacity(int)}.
     */
    void reallocate(int size, int maxCapacity, AdaptiveByteBuf into) {
        AdaptiveByteBuf result = allocate(size, maxCapacity, Thread.currentThread(), into);
        assert result == into : "Re-allocation created separate buffer instance";
    }

    /**
     * The bytes this allocator holds: the {@link PageStore} regions' committed slices or whole regions, from their
     * commit or allocation to their purge or release (see {@link PageStore}), and the one-shot chunks of their own
     * allocation, from {@link #chunkBufferAllocated} to {@link #chunkBufferFreed}. These are the only places that fire
     * the {@link AllocateChunkEvent} and the {@link FreeChunkEvent}, so the bytes allocated minus the bytes freed in a
     * JFR recording equal this number. The size-class chunks and spans carved out of the regions fire no event, since
     * their memory never leaves the allocator.
     */
    long usedMemory() {
        return chunkRegistry.totalCapacity();
    }

    /**
     * A chunk buffer was just taken from {@link #memory} for {@code chunk}.
     *
     * @param pooled      whether the chunk serves many buffers, or is a one-shot chunk for a single one
     * @param threadLocal whether the chunk belongs to a thread-local heap
     */
    void chunkBufferAllocated(ChunkInfo chunk, boolean pooled, boolean threadLocal) {
        chunkRegistry.add(chunk.capacity());
        if (PlatformDependent.isJfrEnabled() && AllocateChunkEvent.isEventEnabled()) {
            AllocateChunkEvent event = new AllocateChunkEvent();
            if (event.shouldCommit()) {
                event.fill(chunk, AdaptiveByteBufAllocator.class);
                event.pooled = pooled;
                event.threadLocal = threadLocal;
                event.commit();
            }
        }
    }

    /** The chunk buffer behind {@code chunk} is about to be released back to {@link #memory} by its chunk. */
    void chunkBufferFreed(ChunkInfo chunk, boolean pooled) {
        chunkRegistry.remove(chunk.capacity());
        if (PlatformDependent.isJfrEnabled() && FreeChunkEvent.isEventEnabled()) {
            FreeChunkEvent event = new FreeChunkEvent();
            if (event.shouldCommit()) {
                event.fill(chunk, AdaptiveByteBufAllocator.class);
                event.pooled = pooled;
                event.commit();
            }
        }
    }

    /**
     * As {@link #chunkBufferAllocated}, for {@code bytes} of a region's shared slices from {@code address} that the
     * {@link PageStore} just committed: allocates nothing unless a JFR event is committed.
     */
    void storeBytesCommitted(long address, int bytes, boolean direct, boolean threadLocal) {
        chunkRegistry.add(bytes);
        if (PlatformDependent.isJfrEnabled() && AllocateChunkEvent.isEventEnabled()) {
            AllocateChunkEvent event = new AllocateChunkEvent();
            if (event.shouldCommit()) {
                event.allocatorType = AdaptiveByteBufAllocator.class;
                event.capacity = bytes;
                event.direct = direct;
                event.address = address;
                event.pooled = true;
                event.threadLocal = threadLocal;
                event.commit();
            }
        }
    }

    /** As {@link #chunkBufferFreed}, for {@code bytes} of shared slices whose memory the {@link PageStore} purged. */
    void storeBytesReleased(long address, int bytes, boolean direct) {
        chunkRegistry.remove(bytes);
        if (PlatformDependent.isJfrEnabled() && FreeChunkEvent.isEventEnabled()) {
            FreeChunkEvent event = new FreeChunkEvent();
            if (event.shouldCommit()) {
                event.allocatorType = AdaptiveByteBufAllocator.class;
                event.capacity = bytes;
                event.direct = direct;
                event.address = address;
                event.pooled = true;
                event.commit();
            }
        }
    }

    // Ensure that we release all previous pooled resources when this object is finalized. This is needed as otherwise
    // we might end up with leaks. While these leaks are usually harmless in reality it would still at least be
    // very confusing for users.
    @SuppressWarnings({"FinalizeDeclaration", "deprecation"})
    @Override
    protected void finalize() throws Throwable {
        try {
            free();
        } finally {
            super.finalize();
        }
    }

    /**
     * Frees the calling thread's heap, the striped heaps and the store's regions now, not at finalization, whose
     * {@link #free} then finds them empty. Only with no live buffer, and no other thread holding or using a heap of
     * this allocator, including one still ending: the regions under them are unmapped or freed.
     */
    void close() {
        if (threadLocalSizeClassHeap != null) {
            threadLocalSizeClassHeap.remove();
        }
        free();
    }

    private void free() {
        for (StripedHeap stripe : stripedHeaps) {
            stripe.freeStripe();
        }
        pageStore.close();
    }

    /**
     * When a heap gives back what it keeps idle: the chunks of a size class that made no allocation through a whole
     * interval (see {@link SizeClassMagazine#decayIfIdle}). On a thread-local heap they age with all of the thread's
     * allocations, whatever their size.
     * <p>
     * The only signal is the heap's own allocations, by whichever thread makes them: each purge tick of a size class
     * adds the allocations it counted, and each span of the large-buffer magazine, already under the stripe lock or
     * on the owner thread, adds its size in units of the smallest size class, so that large buffers read the clock
     * about as often per byte as small ones. The count only paces the clock reads, not the aging. Nothing is added to
     * the size classes' allocation fast paths. Every {@link #DECAY_MIN_ALLOCATIONS} of them the clock is read once,
     * and when {@link #DECAY_INTERVAL_NANOS} passed since the last decay, the idle size classes give up their chunks;
     * otherwise nothing happens until the next count.
     * <p>
     * A chunk given up frees its slices at once, and the heap's ticks drive the store's purge of what stayed free for
     * its own delay, shorter than a decay interval (see {@link PageStore#purgeIfDue}).
     * <p>
     * Guarded like the heap: by the stripe lock, or by the owner thread of a thread-local heap.
     */
    static final class IdleDecay {
        /** At most one decay per this interval: 10 s. */
        static final long DECAY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
        /** How many allocations of the heap between two looks at the clock. */
        static final long DECAY_MIN_ALLOCATIONS = 10000;

        /** The allocator's page store, whose purge the heap's ticks drive; see {@link #count}. */
        private final PageStore store;
        /** The heap's size-class magazines, set once the heap has them; see {@link SizeClassMagazine#decayIfIdle}. */
        SizeClassMagazine[] magazines;
        // Visible for testing.
        long allocationsSinceCheck;
        // Visible for testing.
        long lastDecayNanos = System.nanoTime();

        IdleDecay(PageStore store) {
            this.store = store;
        }

        /**
         * The heap made {@code allocations} more allocations: a size class's purge tick, or a span. Every tick also
         * reads the clock for the store's purge (see {@link PageStore#purgeIfDue}), whose delay is shorter than a decay
         * interval: a tick comes once per four chunks' worth of a class's allocations, at least 128.
         */
        void count(long allocations) {
            allocationsSinceCheck += allocations;
            if (allocationsSinceCheck < DECAY_MIN_ALLOCATIONS) {
                store.purgeIfDue(System.nanoTime());
                return;
            }
            // A new count starts whether or not the interval passed: one look at the clock per count, no more. Every
            // count looks: a heap that allocates always holds something a decay may find idle (a size class it
            // stopped using), so there is nothing cheaper to test first.
            allocationsSinceCheck = 0;
            long now = System.nanoTime();
            if (now - lastDecayNanos >= DECAY_INTERVAL_NANOS) {
                decay(now);
            } else {
                store.purgeIfDue(now);
            }
        }

        // Visible for testing.
        void decay(long now) {
            lastDecayNanos = now;
            allocationsSinceCheck = 0;
            SizeClassMagazine[] mags = magazines;
            if (mags != null) {
                for (SizeClassMagazine mag : mags) {
                    if (mag != null) {
                        mag.decayIfIdle();
                    }
                }
            }
            store.purgeIfDue(now);
        }
    }

    // A stripe: the heap of the threads without a thread-local one that pick it. One StampedLock covers all its
    // size-class magazines and its magazine for buffers above the size classes.
    private static final class StripedHeap {
        final StampedLock lock = new StampedLock();
        final IdleDecay idleDecay;
        SizeClassMagazine[] magazines;
        SpanMagazine spanMagazine;
        AdaptiveRecycler recycler;
        /** Where the stripe's claims start in a region: see {@link PageStore#nextHeapSequence}. */
        int seq = -1;

        StripedHeap(PageStore store) {
            idleDecay = new IdleDecay(store);
        }

        SizeClassMagazine getOrCreateMagazine(int sizeClassIndex, AdaptivePoolingAllocator allocator) {
            SizeClassMagazine[] mags = magazines;
            if (mags == null) {
                return createFirstMagazine(sizeClassIndex, allocator);
            }
            SizeClassMagazine mag = mags[sizeClassIndex];
            if (mag == null) {
                mag = createMagazine(sizeClassIndex, allocator);
            }
            return mag;
        }

        private SizeClassMagazine createFirstMagazine(int sizeClassIndex, AdaptivePoolingAllocator allocator) {
            magazines = new SizeClassMagazine[SIZE_CLASSES_COUNT];
            idleDecay.magazines = magazines;
            joinPageStore(allocator);
            return createMagazine(sizeClassIndex, allocator);
        }

        /** Once per stripe, by whichever magazine comes first. */
        private void joinPageStore(AdaptivePoolingAllocator allocator) {
            if (seq < 0) {
                seq = allocator.pageStore.nextHeapSequence();
            }
        }

        private SizeClassMagazine createMagazine(int sizeClassIndex, AdaptivePoolingAllocator allocator) {
            if (recycler == null) {
                recycler = AdaptiveRecycler.sharedExclusiveGet(MAGAZINE_BUFFER_QUEUE_CAPACITY);
            }
            SizeClassMagazine mag = new SizeClassMagazine(allocator, idleDecay,
                    sizeClassIndex, null, recycler, lock, magazines, seq);
            magazines[sizeClassIndex] = mag;
            return mag;
        }

        /** Above the size classes, under the stripe lock: a span. */
        private AdaptiveByteBuf allocateLargeLocked(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate,
                                                    AdaptivePoolingAllocator allocator) {
            SpanMagazine mag = getOrCreateSpanMagazine(allocator);
            if (buf == null) {
                buf = mag.newBuffer();
            }
            boolean allocated = false;
            try {
                mag.allocate(size, maxCapacity, buf);
                allocated = true;
                return buf;
            } finally {
                if (!allocated && !reallocate) {
                    buf.release();
                }
            }
        }

        SpanMagazine getOrCreateSpanMagazine(AdaptivePoolingAllocator allocator) {
            SpanMagazine mag = spanMagazine;
            if (mag == null) {
                if (recycler == null) {
                    recycler = AdaptiveRecycler.sharedExclusiveGet(MAGAZINE_BUFFER_QUEUE_CAPACITY);
                }
                joinPageStore(allocator);
                mag = new SpanMagazine(allocator, recycler, null, idleDecay, seq);
                spanMagazine = mag;
            }
            return mag;
        }

        void freeStripe() {
            final StampedLock l = lock;
            long stamp = l.writeLock();
            try {
                if (magazines != null) {
                    for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
                        SizeClassMagazine mag = magazines[i];
                        if (mag != null) {
                            mag.free();
                            magazines[i] = null;
                        }
                    }
                }
                spanMagazine = null;
            } finally {
                l.unlockWrite(stamp);
            }
        }

        AdaptiveByteBuf tryAllocate(int sizeClassIndex, int size, int maxCapacity,
                                     AdaptiveByteBuf buf, boolean reallocate,
                                     AdaptivePoolingAllocator allocator) {
            final StampedLock l = lock;
            long stamp = l.tryWriteLock();
            if (stamp == 0) {
                return null;
            }
            try {
                if (sizeClassIndex < SIZE_CLASSES_COUNT) {
                    SizeClassMagazine mag = getOrCreateMagazine(sizeClassIndex, allocator);
                    if (buf == null) {
                        buf = mag.newBuffer();
                    }
                    if (mag.allocate(size, maxCapacity, buf)) {
                        mag.tickAllocPurge();
                        return buf;
                    }
                } else {
                    // No purge tick here: the large-buffer magazines count their allocations themselves.
                    return allocateLargeLocked(size, maxCapacity, buf, reallocate, allocator);
                }
                if (!reallocate) {
                    buf.release();
                }
                return null;
            } finally {
                l.unlockWrite(stamp);
            }
        }
    }

    private static final class ThreadLocalSizeClassHeap {
        private final SizeClassMagazine[] magazines = new SizeClassMagazine[SIZE_CLASSES_COUNT];
        /** Where the heap's claims start in a region: see {@link PageStore#nextHeapSequence}. */
        private final int seq;
        /** Buffers above the size classes; {@code null} until the first one. */
        private SpanMagazine spanMagazine;
        // Visible for testing.
        final IdleDecay idleDecay;
        private final AdaptivePoolingAllocator allocator;

        ThreadLocalSizeClassHeap(AdaptivePoolingAllocator allocator) {
            this.allocator = allocator;
            idleDecay = new IdleDecay(allocator.pageStore);
            idleDecay.magazines = magazines;
            seq = allocator.pageStore.nextHeapSequence();
        }

        AdaptiveByteBuf allocate(int sizeClassIndex, int size, int maxCapacity, AdaptiveByteBuf buf) {
            SizeClassMagazine mag = getOrCreateMagazine(sizeClassIndex);
            boolean reallocate = buf != null;
            if (!reallocate) {
                buf = mag.newBuffer();
            }
            boolean success = mag.allocate(size, maxCapacity, buf);
            assert success : "Thread-local allocation must always succeed";
            mag.tickAllocPurge();
            return buf;
        }

        /**
         * A buffer above the size classes: a span of shared slices from this heap's own {@link SpanMagazine}, created
         * on first use; the owner thread needs no lock.
         */
        AdaptiveByteBuf allocateLarge(int size, int maxCapacity, AdaptiveByteBuf buf) {
            SpanMagazine mag = spanMagazine;
            if (mag == null) {
                mag = new SpanMagazine(allocator, null, Thread.currentThread(), idleDecay, seq);
                spanMagazine = mag;
            }
            boolean reallocate = buf != null;
            if (!reallocate) {
                buf = mag.newBuffer();
            }
            boolean allocated = false;
            try {
                mag.allocate(size, maxCapacity, buf);
                allocated = true;
                return buf;
            } finally {
                if (!allocated && !reallocate) {
                    buf.release();
                }
            }
        }

        SizeClassMagazine getOrCreateMagazine(int sizeClassIndex) {
            SizeClassMagazine mag = magazines[sizeClassIndex];
            if (mag == null) {
                mag = createMagazine(sizeClassIndex);
            }
            return mag;
        }

        private SizeClassMagazine createMagazine(int sizeClassIndex) {
            SizeClassMagazine mag = new SizeClassMagazine(allocator, idleDecay,
                                       sizeClassIndex, Thread.currentThread(), null, null, magazines, seq);
            magazines[sizeClassIndex] = mag;
            return mag;
        }

        void free() {
            for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
                SizeClassMagazine mag = magazines[i];
                if (mag != null) {
                    mag.free();
                    magazines[i] = null;
                }
            }
            spanMagazine = null;
        }
    }

    /**
     * An intrusive doubly linked list of chunks, newest first. The links live on the
     * chunk, so removing any chunk is O(1), and so does the chunk's membership: {@code chunk.queue} is the queue it
     * is on, or {@code null}. Not concurrent: the magazine that owns it holds the stripe lock, or is the only thread
     * that touches it.
     */
    static final class ChunkQueue {
        Chunk head;
        int size;

        void pushFront(Chunk chunk) {
            Chunk head = this.head;
            chunk.prevInQueue = null;
            chunk.nextInQueue = head;
            if (head != null) {
                head.prevInQueue = chunk;
            }
            this.head = chunk;
            chunk.queue = this;
            size++;
        }

        void remove(Chunk chunk) {
            Chunk prev = chunk.prevInQueue;
            Chunk next = chunk.nextInQueue;
            if (prev != null) {
                prev.nextInQueue = next;
            } else {
                head = next;
            }
            if (next != null) {
                next.prevInQueue = prev;
            }
            chunk.prevInQueue = null;
            chunk.nextInQueue = null;
            chunk.queue = null;
            size--;
        }
    }

    /**
     * Chunks that a releasing thread asked their owner to look at, because it freed memory in a chunk whose queues
     * it may not touch (see Invariant N in {@link SizeClassedChunkCache}, which both magazines follow). A lock-free
     * (Treiber) stack that any thread pushes to and the owner takes whole. A chunk's {@code pendingNext} is both its
     * link and the claim that it is queued: a chunk is queued at most once, and a push on a queued chunk costs one
     * volatile read.
     */
    static final class PendingChunks {
        private static final AtomicReferenceFieldUpdater<PendingChunks, Chunk> HEAD =
                AtomicReferenceFieldUpdater.newUpdater(PendingChunks.class, Chunk.class, "head");
        private static final AtomicReferenceFieldUpdater<Chunk, Chunk> NEXT =
                AtomicReferenceFieldUpdater.newUpdater(Chunk.class, Chunk.class, "pendingNext");
        /**
         * Ends the stack, so that a {@code null} link keeps its meaning of "not queued". Never a usable chunk.
         */
        private static final Chunk END = new SizeClassedChunk();

        private volatile Chunk head;

        /**
         * Queue {@code chunk}, unless it is queued already. Any thread, no lock.
         */
        void push(Chunk chunk) {
            if (chunk.pendingNext != null) {
                return;
            }
            // Claim: only the thread that moves the link off null owns the push.
            if (!NEXT.compareAndSet(chunk, null, END)) {
                return;
            }
            Chunk head;
            do {
                head = this.head;
                NEXT.lazySet(chunk, head == null ? END : head);
            } while (!HEAD.compareAndSet(this, head, chunk));
        }

        /**
         * Take every queued chunk: the first one, whose successors {@link #rearm} returns, or {@code null}. Owner
         * only. Cheap when nothing is queued: one volatile read, no atomic read-modify-write; the heap-wide drain
         * pays this per size class.
         */
        Chunk takeAll() {
            if (head == null) {
                return null;
            }
            return HEAD.getAndSet(this, null);
        }

        /**
         * Unlink {@code chunk}, taken by {@link #takeAll}, and make it queueable again; return the next taken chunk,
         * or {@code null}. Call it BEFORE processing the chunk: a return that lands while the chunk is processed must
         * be able to queue it again, and re-arming afterwards would lose that note and strand the chunk until some
         * later, unrelated one.
         * <p>
         * The store is a full volatile store on purpose, not a lazySet: it is the store half of a Dekker pair with
         * the releaser, which pushes the segment (the push ends in a CAS on the chunk's external list, a StoreLoad)
         * and only then reads {@code pendingNext}. The processing reads the free counts right after this store;
         * without the StoreLoad here both sides could miss each other and the chunk would be stranded.
         */
        static Chunk rearm(Chunk chunk) {
            Chunk next = chunk.pendingNext;
            NEXT.set(chunk, null);
            return next == END ? null : next;
        }

        /**
         * Drop every queued chunk; for a cache being freed, whose chunks give their spans back or are abandoned.
         */
        void clear() {
            HEAD.lazySet(this, null);
        }

        // Visible for testing: how many chunks are queued.
        int size() {
            int count = 0;
            Chunk cur = head;
            while (cur != null && cur != END) {
                count++;
                cur = cur.pendingNext;
            }
            return count;
        }
    }

    /**
     * Two-list chunk cache: answers "give me a chunk to carve from" and "release what is idle".
     *
     * <p><b>Access.</b> The queues and each chunk's {@code queue} are touched only by the
     * owner thread (thread-local magazines) or under the stripe write lock (shared magazines) —
     * one magazine's caches all share that one lock. The only exception is {@link #pending},
     * which any releasing thread may push to; it is the sole concurrent structure here.
     *
     * <p><b>The two lists.</b>
     * <ul>
     *   <li><b>Reusable</b> — chunks known to have free segments. {@link #pollChunk} takes the
     *       head, O(1). Fully-free chunks at or below the retention floor stay here rather than
     *       being evicted, so a burst does not have to re-allocate immediately after draining; a size class
     *       that stays idle through a whole decay interval gives up those too, see
     *       {@link SizeClassMagazine#decayIfIdle}.</li>
     *   <li><b>Exhausted</b> — chunks with no free segments when they were filed. Primarily an
     *       ownership registry: it keeps chunks reachable for {@link #free()} and gives the
     *       notification drain somewhere to move a chunk out of. It is <em>not</em> the discovery
     *       mechanism, and is never walked.</li>
     * </ul>
     *
     * <p><b>The active chunk.</b> The chunk the magazine allocates from is the cache's {@link #active}
     * chunk, on neither queue; the magazine's {@code current} field is
     * only the fast path's alias of it. {@link #activate} makes a polled or freshly allocated chunk active,
     * and {@link #deactivate} files it by capacity, like {@link #offerChunk}, when the magazine runs it out
     * of segments, is freed, or its size class stayed idle through a whole decay interval. The active chunk is the
     * magazine's, not a retention candidate: no other cache decision touches it (the release paths and the drain
     * act only on chunks filed on a queue, and {@link #tickPurge} walks the lists only), and it is not counted against
     * the retention floor ({@link #atOrBelowFloor}).
     *
     * <p><b>Why the reusable list is trustworthy.</b> A cached chunk other than the active one can
     * only <em>gain</em> capacity: segments are handed out only by {@code readInitInto} on the active
     * chunk, and a chunk becomes active only through {@link #activate}, after the magazine gave up the
     * previous one. So a non-active chunk filed with capacity still has it, and the head of the
     * reusable list is always usable when there is no active chunk, which is the only time
     * {@link #pollChunk} runs.
     *
     * <p><b>Why the exhausted list is not.</b> {@code offerChunk} files a chunk by reading its
     * capacity, and a cross-thread return landing just after that read leaves it filed as exhausted
     * while it actually has capacity. Monotonicity does not help here — it says the reusable list
     * is pure, not that the exhausted list is.
     *
     * <p><b>Three routes move a chunk back to reusable</b>, all driven by the one event that can
     * change a chunk's occupancy, a segment return:
     * <ol>
     *   <li><b>Inline</b>, when the returning thread can synchronise — it is the owner thread, or it
     *       won the stripe lock. Plain field reads and pointer writes, no atomics. Signal A
     *       (exhausted → reusable) and Signal B (fully free → evicted above the floor).</li>
     *   <li><b>Deferred</b>, when it cannot: {@link #notifyHasCapacity} leaves a note and
     *       {@link #drainPending} applies the transition under the lock. See Invariant N.</li>
     *   <li><b>Probed</b>, as a last resort: {@link #probeExhausted()} looks at a bounded number of
     *       exhausted chunks when the reusable list is empty, because a note pushed concurrently
     *       with the drain has not been applied yet. Without it the caller would allocate a fresh
     *       chunk while a usable one sat in the exhausted list.</li>
     * </ol>
     * Routes 2 and 3 overlap deliberately: notifications reach chunks a bounded scan would not, and
     * a bounded scan covers what notifications are late for.
     *
     * <p>There is deliberately no periodic sweep of the exhausted list. A note is never dropped -
     * {@link #drainPending} re-arms a chunk's link before processing it, so a return that lands
     * mid-processing queues the chunk again rather than being swallowed - so a sweep could only ever
     * find a chunk whose notification was lost, which is a bug in this protocol and not something a
     * periodic rescue should paper over.
     *
     * <p><b>Eviction only ever operates on the reusable list</b> — {@link #evictIfAboveFloor} calls
     * {@code reusable.remove} unconditionally, and every caller either walks the reusable list
     * or moves the chunk there first. So an exhausted-list chunk never gives its span up,
     * and {@link #probeExhausted()} cannot encounter one that did.
     *
     * <p>Note that route 3 can hand out a chunk that route 2 would have evicted. That is intended:
     * reusing a fully-free chunk beats evicting it and allocating a fresh one.
     *
     * <p><b>No cap.</b> {@code offerChunk} files every chunk; cache size follows the working set,
     * and idle chunks leave via Signal B rather than a byte threshold.
     */
    static final class SizeClassedChunkCache {
        /** Bound on the last-resort probe of the exhausted list; see {@link #probeExhausted()}. */
        private static final int MAX_EXHAUSTED_PROBE = 8;

        final ChunkQueue exhausted = new ChunkQueue();
        final ChunkQueue reusable = new ChunkQueue();
        /**
         * The chunk the magazine allocates from, or {@code null}. It is on neither queue, and neither the purge nor
         * the drain acts on it: only the magazine allocates from it, until {@link #deactivate} files it by capacity
         * like any other chunk.
         */
        SizeClassedChunk active;
        /** Treiber stack of chunks that a releasing thread asked us to look at. */
        final PendingChunks pending = new PendingChunks();
        /**
         * Chunk objects given up with their spans, for this cache's next chunks (see {@link SizeClassedChunk#reinit}).
         * A chunk object never leaves the cache that made it, so a note left for an earlier incarnation still reaches
         * the right cache. Unbounded: one object per chunk of the cache's peak, holding nothing else.
         */
        final ChunkQueue idle = new ChunkQueue();
        // Visible for testing: the chunk objects this cache made.
        int chunksMade;

        /**
         * The lock guarding this cache's lists, or {@code null} when there is nothing to guard.
         *
         * <p>A magazine owned by one thread reaches its cache only from that thread, so it has no
         * lock; a magazine on a stripe shares the stripe's lock with the other size classes there.
         * Either way the lists are only ever touched by a thread with exclusive access - see
         * {@link #tryLockForRelease()}, which reports "not available" for both cases alike.
         */
        final StampedLock stripeLock;

        SizeClassedChunkCache() {
            this(null);
        }

        SizeClassedChunkCache(StampedLock stripeLock) {
            this.stripeLock = stripeLock;
        }

        /** The queued chunks a size class keeps before it evicts one that empties: see {@link #atOrBelowFloor}. */
        static final int FLOOR = 4;

        /**
         * {@code true} when the two queues hold at most {@link #FLOOR} chunks between them. This is the retention
         * floor: eviction never takes the last chunks of a size class in use besides the active one, so a size class
         * that empties and fills again around them does not give one up and make it again each time, a chunk object
         * of garbage each (only a class idle through a whole decay interval gives them up, see
         * {@link SizeClassMagazine#decayIfIdle}). Every other chunk that empties frees its slices. mimalloc too keeps
         * up to 3 empty pages of a small size
         * class before freeing them: {@code MI_RETIRE_MAX_PAGES} and {@code _mi_page_retire},
         * https://github.com/microsoft/mimalloc/blob/31d034d/src/page.c#L596-L638, as does the
         * Java port's {@code pageRetire} (https://github.com/neoionet/netty-allocator at 397e933,
         * MiMallocByteBufAllocator.java lines 1745-1768).
         */
        private boolean atOrBelowFloor() {
            return exhausted.size + reusable.size <= FLOOR;
        }

        // Signal A (see refile): exhausted → reusable
        void moveToReusable(SizeClassedChunk chunk) {
            exhausted.remove(chunk);
            reusable.pushFront(chunk);
        }

        void evictIfAboveFloor(SizeClassedChunk chunk) {
            // Every caller filters on the reusable queue, which the active chunk is never on.
            assert chunk != active : "the active chunk must never be evicted";
            if (chunk.hasFullCapacity() && !atOrBelowFloor()) {
                evict(chunk);
            }
        }

        /** {@code chunk}, wholly free and on the reusable list, gives its span back to the store and waits idle. */
        private void evict(SizeClassedChunk chunk) {
            reusable.remove(chunk);
            chunk.releaseSpan();
            idle.pushFront(chunk);
        }

        /** A chunk object given up earlier, or {@code null}: see {@link #idle}. */
        SizeClassedChunk takeIdle() {
            SizeClassedChunk chunk = (SizeClassedChunk) idle.head;
            if (chunk != null) {
                idle.remove(chunk);
            }
            return chunk;
        }

        // --- Notification queue: cross-thread segment returns that could not take the lock ---
        //
        // Invariant N (notification completeness): every segment return is either observed by a later
        // cache decision about that chunk, or leaves an outstanding note that is processed after that
        // decision. Nothing scans, so a lost signal means a chunk with capacity sits on the exhausted
        // list forever -- never reusable, and never fully free either, so the purge sweep will not
        // evict it. Four properties carry the invariant, and all four must hold:
        //
        //  1. Offer before notify. releaseSegment pushes the segment on the chunk's external list first, so a
        //     drainer that pops the note is guaranteed to see the segment.
        //  2. Notes are state-independent: "look at this chunk", never "this specific thing changed".
        //     One note therefore covers any number of later returns, and a note left while the chunk
        //     was still active (or on no queue) stays correct once the chunk is filed. Do not optimise the
        //     note to carry state. This is also why a releaser that finds the claim already taken can
        //     simply walk away: the in-flight note covers its return too.
        //  3. Re-arm before processing (see drainPending).
        //  4. Classification and drain cannot interleave. offerChunk's (read capacity, insert) pair
        //     and the drain both run under the same stripe lock, or on the same owner thread. This is
        //     what covers a return landing right after offerChunk read the capacity but before the
        //     insert: the chunk is filed as exhausted while holding capacity, and the note -- which
        //     cannot be consumed in between -- is what fixes it.
        //
        // A drain that finds the active chunk and no-ops is benign, not a lost signal: the chunk is the
        // magazine's, which consumes its own returned segments through nextAvailableSegmentOffset, and
        // when the magazine gives it up, deactivate files it by capacity -- an offerChunk, so property 4
        // covers it: a note consumed before deactivate made its segment visible to deactivate's capacity
        // read, and a note still outstanding is processed after it, against the list it was filed on.
        // That includes a return that lands from another thread while the chunk is being deactivated.
        // A drain that finds a chunk on no queue is benign too: the chunk is gone (evicted, recycled, or its cache
        // freed). A polled chunk is never seen in that state, because pollChunk and activate run back to
        // back under the same lock or on the same owner thread, with no drain in between.

        /**
         * Queue {@code chunk} for the next drain. Called by a releasing thread that holds no lock,
         * <em>after</em> the segment has been pushed on the chunk's external free list, so a drainer
         * that pops the note is guaranteed to also see the segment.
         *
         * <p>This path must never read {@code queue} or any list link: those belong to the
         * owner thread / stripe lock holder. The note only says "look at this chunk". The chunk finds
         * this cache through its {@code final owningCache} field, so no racy reference read is involved.
         *
         * <p>{@link SizeClassedChunk#pendingNext} doubles as the dedup claim, so a return on a chunk
         * that is already queued costs a single volatile read.
         */
        void notifyHasCapacity(SizeClassedChunk chunk) {
            pending.push(chunk);
        }

        /**
         * Apply every queued notification. Caller must hold the stripe lock, or be the owner thread of
         * a thread-local cache.
         */
        void drainPending() {
            Chunk cur = pending.takeAll();
            while (cur != null) {
                // Re-arm BEFORE processing (property 3): see PendingChunks#rearm.
                Chunk next = PendingChunks.rearm(cur);
                refile((SizeClassedChunk) cur);
                cur = next;
            }
        }

        // Visible for testing: how many chunks are queued for the next drain.
        int pendingCount() {
            return pending.size();
        }

        /**
         * Move {@code chunk} to the queue its capacity calls for, after it may have gained capacity: a segment
         * return, by the owner thread or under the lock, or a note of one. An exhausted chunk with a free segment
         * becomes reusable; a fully free reusable chunk is evicted above the retention floor. Caller holds the
         * stripe lock or is the owner thread.
         */
        void refile(SizeClassedChunk chunk) {
            ChunkQueue queue = chunk.queue;
            if (queue == exhausted) {
                // A note may be stale; a return by the owner or under the lock has just pushed the segment.
                if (!chunk.hasRemainingCapacity()) {
                    return;
                }
                moveToReusable(chunk);
            } else if (queue != reusable) {
                // Idle, or on no queue: either given up (idle, or its cache freed), not ours to move - its capacity is
                // never read, because such a chunk gave its span up - or
                // the active chunk, which consumes its own returned segments and is filed by capacity when the
                // magazine gives it up (see deactivate). A polled chunk is activated before any drain can run.
                return;
            }
            evictIfAboveFloor(chunk);
        }

        /**
         * Try to take exclusive access to this cache so a releasing thread can place a segment and
         * apply any resulting list transition. Returns 0 when unavailable: a cache with no lock has
         * no exclusive mode to take, and a contended stripe lock is not waited on.
         *
         * <p>A non-zero result must be passed to {@link #unlockAfterRelease(long)}; a zero result
         * must not be.
         */
        long tryLockForRelease() {
            return stripeLock == null ? 0 : stripeLock.tryWriteLock();
        }

        /**
         * Release the exclusive access taken by {@link #tryLockForRelease()}.
         *
         * @param stamp a non-zero stamp from {@code tryLockForRelease}. Zero is not a stamp - it is
         *              how that method reports failure, and a cache without a lock reports nothing
         *              else - so passing it here is a caller bug, not a no-op.
         */
        void unlockAfterRelease(long stamp) {
            assert stamp != 0 : "unlockAfterRelease(0): tryLockForRelease did not grant the lock";
            stripeLock.unlockWrite(stamp);
        }

        /** Visible for testing: runs a purge tick without waiting for {@link #CHUNK_PURGE_INTERVAL}, then polls. */
        SizeClassedChunk forcePurge() {
            tickPurge();
            return pollChunkInternal();
        }

        SizeClassedChunk pollChunk() {
            // Slow-path only (once per chunk-worth of allocations), which is exactly where a chunk is
            // wanted; never per allocation.
            drainPending();
            return pollChunkInternal();
        }

        /**
         * O(1) and unconditional: every chunk on the reusable list has capacity, and keeps it for as
         * long as it stays cached (nothing allocates out of a cached chunk, so its capacity can only
         * grow). The exhausted list is never searched — a chunk leaves it only when a notification
         * says it gained capacity.
         */
        private SizeClassedChunk pollChunkInternal() {
            // The magazine gives up its active chunk before it asks for another one.
            assert active == null : "poll with an active chunk";
            SizeClassedChunk chunk = (SizeClassedChunk) reusable.head;
            if (chunk != null) {
                reusable.remove(chunk);
                return chunk;
            }
            return probeExhausted();
        }

        /**
         * Last resort before the caller allocates a fresh chunk: look at a bounded number of
         * exhausted chunks in case one regained capacity from a return whose notification has not
         * been drained yet.
         *
         * <p>An empty reusable list means "no usable chunk is <em>known</em>", not "none exists".
         * {@code drainPending} runs immediately before the poll, so it catches every note pushed
         * before its {@code getAndSet} - but a note pushed concurrently with the drain, or by a
         * releaser that has claimed its link and not yet published it, is not seen. Without this
         * probe the caller would allocate a new chunk while a usable one sat in the exhausted list,
         * which is the chunk-count growth this cache exists to avoid.
         *
         * <p>Notifications cover what a scan cannot reach; a bounded scan covers what notifications
         * are late for.
         *
         * <p>Bounded by chunks <em>visited</em>, not by anything found - a bound on work done is
         * the only kind that holds when nothing matches.
         */
        private SizeClassedChunk probeExhausted() {
            SizeClassedChunk cur = (SizeClassedChunk) exhausted.head;
            int visited = 0;
            while (cur != null && visited < MAX_EXHAUSTED_PROBE) {
                SizeClassedChunk next = (SizeClassedChunk) cur.nextInQueue;
                visited++;
                if (cur.hasRemainingCapacity()) {
                    exhausted.remove(cur);
                    return cur;
                }
                cur = next;
            }
            return null;
        }

        void tickPurge() {
            drainPending();
            // Exhausted→reusable is applied by the drain above. All that is left is evicting
            // fully-free reusable chunks above the retention floor.
            SizeClassedChunk cur = (SizeClassedChunk) reusable.head;
            while (cur != null && !atOrBelowFloor()) {
                SizeClassedChunk next = (SizeClassedChunk) cur.nextInQueue;
                if (cur.hasFullCapacity()) {
                    evict(cur);
                }
                cur = next;
            }
        }

        /**
         * Give up every wholly free reusable chunk, the retention floor included: for a size class that stayed idle
         * through a whole decay interval (see {@link SizeClassMagazine#decayIfIdle}).
         */
        void evictWhollyFree() {
            drainPending();
            SizeClassedChunk cur = (SizeClassedChunk) reusable.head;
            while (cur != null) {
                SizeClassedChunk next = (SizeClassedChunk) cur.nextInQueue;
                if (cur.hasFullCapacity()) {
                    evict(cur);
                }
                cur = next;
            }
        }

        void offerChunk(SizeClassedChunk chunk) {
            if (chunk.hasRemainingCapacity()) {
                reusable.pushFront(chunk);
            } else {
                exhausted.pushFront(chunk);
            }
        }

        /**
         * Make {@code chunk}, which is not in this cache, the active chunk: the one the magazine allocates from
         * until {@link #deactivate} files it by capacity like any other chunk. Caller holds the stripe lock or is
         * the owner thread, like every list operation.
         */
        void activate(SizeClassedChunk chunk) {
            assert active == null : "the magazine already has an active chunk";
            assert chunk.queue == null : "the chunk is filed on a queue";
            active = chunk;
        }

        /**
         * The magazine is done allocating from its active chunk (it ran out of segments, or the magazine is
         * being freed): unlink it and file it by capacity, exactly as {@link #offerChunk} files any chunk.
         *
         * <p>Invariant N holds across this step as it does for any {@code offerChunk}. While the chunk was
         * active, a return that could not synchronise left a note, and the drain ignored it
         * ({@code refile} skips chunks on no queue). Such a note was either
         * <ul>
         *   <li>consumed before this call: then the segment was offered before the note was pushed (property
         *       1), the push happens-before the drain that popped it, and that drain ran on this thread or
         *       under this lock before this call. So the segment is visible here: either the magazine already
         *       allocated it again, or the capacity read below sees it and files the chunk reusable; or</li>
         *   <li>still outstanding (or pushed after this call started): the drain that pops it runs after
         *       this call, under the same lock or on the same owner thread (property 4), and finds the
         *       chunk on the list this call filed it on, so an exhausted-but-not-really chunk is moved; or</li>
         *   <li>never pushed: the releaser found {@code pendingNext} already non-null and walked away. If that
         *       link is a note still outstanding, the previous case covers this segment too. If it is a note a
         *       drain already popped, the releaser read the link before that drain's re-arm store: the releaser
         *       pushed first (a CAS on the external list) and read {@code pendingNext} second, and the drain's
         *       full volatile re-arm store precedes every later volatile read of the external list on the
         *       draining side, this call's capacity read included. So the segment is visible here (see
         *       {@link PendingChunks#rearm}).</li>
         * </ul>
         * A return that took the lock or came from the owner thread cannot interleave with this call at all.
         */
        void deactivate(SizeClassedChunk chunk) {
            assert chunk == active : "not the active chunk";
            active = null;
            offerChunk(chunk);
        }

        /**
         * The heap is gone: every chunk gives its span back, or is abandoned to the store until its buffers are back
         * (see {@link SizeClassedChunk#releaseOrAbandon}). Outstanding notes are dropped: nobody drains this cache
         * any more.
         */
        void free() {
            pending.clear();
            // The magazine gives up its active chunk before it frees its cache.
            assert active == null : "free with an active chunk";
            freeAll(exhausted);
            freeAll(reusable);
        }

        private static void freeAll(ChunkQueue queue) {
            Chunk cur;
            while ((cur = queue.head) != null) {
                queue.remove(cur);
                ((SizeClassedChunk) cur).releaseOrAbandon();
            }
        }

        // Visible for testing: no chunk linked on either list.
        boolean isEmpty() {
            return exhausted.size + reusable.size == 0;
        }
    }

    private static final class SizeClassChunkController {

        private static final int COLOUR_SHIFT = 6;
        private static final int MAX_COLOURS = 16;
        private static final int MIN_BUFFERS_TO_DROP_ONE = 32;

        private final int segmentSize;
        private final int chunkSize;
        /** The segments a chunk hands out: those that fit, less one an exact fit gives up for its colours. */
        final int buffers;
        /** How many start offsets, 64 bytes apart, the span chunks of this class rotate through. */
        private final int colours;
        private int nextColour;

        private SizeClassChunkController(int segmentSize, int chunkSize) {
            this.segmentSize = segmentSize;
            this.chunkSize = chunkSize;
            // Slab colouring: each chunk object of a heap's class starts its segments 64 bytes further into its spans
            // than the previous one, round robin over at most 16 offsets, so that segment k of consecutive chunks does
            // not share its offset in a 4 KiB page. The room is the tail the segments leave unused; a
            // class that fits exactly gives up one segment for it when it has 32 or more and the segment makes room for
            // a second colour (not the 32-byte class), else stays uncoloured. See
            // Bonwick, "The Slab Allocator: An Object-Caching Kernel Memory Allocator", USENIX Summer 1994, section
            // 4.3 https://people.eecs.berkeley.edu/~kubitron/courses/cs194-24-S14/hand-outs/bonwick_slab.pdf, Linux's
            // colour_off/colour/colour_next https://github.com/torvalds/linux/blob/v4.19/mm/slab.c#L2684-L2693 and
            // Afek, Dice, Morrison, "Cache Index-Aware Memory Allocation", ISMM 2011
            // https://www.cs.tau.ac.il/~mad/publications/ismm2011-CIF.pdf
            int buffers = chunkSize / segmentSize;
            int room = chunkSize - buffers * segmentSize;
            if (room < 1 << COLOUR_SHIFT && buffers >= MIN_BUFFERS_TO_DROP_ONE
                    && room + segmentSize >= 1 << COLOUR_SHIFT) {
                buffers--;
                room += segmentSize;
            }
            this.buffers = buffers;
            colours = Math.min(MAX_COLOURS, (room >>> COLOUR_SHIFT) + 1);
        }

        /** The start offset of the next chunk object's segments in its spans; single writer, as the magazine. */
        private int nextColourOffset() {
            int colour = nextColour;
            nextColour = colour + 1 == colours ? 0 : colour + 1;
            return colour << COLOUR_SHIFT;
        }

        /**
         * Compute the "fast max capacity" value for the buffer: one segment, or less if the buffer may not grow
         * that far.
         */
        int computeBufferCapacity(int maxCapacity) {
            return Math.min(segmentSize, maxCapacity);
        }

        /**
         * A new chunk for {@code magazine}: a span of shared slices (see {@link PageStore#claimSlices}), served by a
         * chunk object its cache gave up earlier, or by a new one. The store accounts the slices, so nothing is
         * announced here.
         */
        SizeClassedChunk newChunkAllocation(SizeClassMagazine magazine) {
            PageStore store = magazine.allocator.pageStore;
            int slices = chunkSize / store.config.sliceSize;
            long run = store.claimSlices(slices, magazine.seq,
                    magazine.ownerThread != null);
            Segment segment = store.block(run);
            int start = store.start(run);
            try {
                SizeClassedChunkCache cache = magazine.chunkCache;
                SizeClassedChunk chunk = cache.takeIdle();
                if (chunk == null) {
                    chunk = new SizeClassedChunk(magazine, this, nextColourOffset());
                    cache.chunksMade++;
                }
                chunk.reinit(segment, start);
                return chunk;
            } catch (Throwable t) {
                segment.releaseRun(start, slices, System.nanoTime());
                throw t;
            }
        }
    }

    private static int threadIndex(Thread t) {
        int id = (int) t.getId();
        return id ^ (id >>> 16);
    }

    static final class AdaptiveRecycler extends Recycler<AdaptiveByteBuf> {

        private AdaptiveRecycler(boolean unguarded, int interval) {
            // uses fast thread local
            super(unguarded, interval);
        }

        private AdaptiveRecycler(int maxCapacity, boolean unguarded) {
            // doesn't use fast thread local, shared MPMC
            super(maxCapacity, unguarded);
        }

        private AdaptiveRecycler(int maxCapacity, boolean unguarded, boolean exclusiveGet) {
            // doesn't use fast thread local, exclusive-get mode
            super(maxCapacity, unguarded, exclusiveGet);
        }

        @Override
        protected AdaptiveByteBuf newObject(final Handle<AdaptiveByteBuf> handle) {
            return new AdaptiveByteBuf((EnhancedHandle<AdaptiveByteBuf>) handle);
        }

        public static AdaptiveRecycler threadLocal() {
            // Interval 0: pool every recycled buffer object, as the stripes' pools do, instead of the
            // io.netty.recycler.ratio default, which admits one in eight at the cost of a counter and a
            // data-dependent branch per allocation; retention is already bounded by the recycler's
            // capacity.
            return new AdaptiveRecycler(true, 0);
        }

        public static AdaptiveRecycler sharedWith(int maxCapacity) {
            return new AdaptiveRecycler(maxCapacity, true);
        }

        public static AdaptiveRecycler sharedExclusiveGet(int maxCapacity) {
            return new AdaptiveRecycler(maxCapacity, true, true);
        }
    }

    /**
     * The magazine of one size class, on a shared stripe (guarded by the stripe lock) or on a thread-local heap
     * (used by its owner thread only). It carves fixed-size segments out of {@link SizeClassedChunk}s, keeps its
     * chunks in its own {@link SizeClassedChunkCache} (the one it allocates from as the cache's active chunk, which
     * {@link #current} aliases).
     */
    private static final class SizeClassMagazine {
        private static final AdaptiveRecycler EVENT_LOOP_LOCAL_BUFFER_POOL = AdaptiveRecycler.threadLocal();

        private SizeClassedChunk current;
        final AdaptivePoolingAllocator allocator;
        final Thread ownerThread;
        private final SizeClassChunkController chunkController;
        private final SizeClassedChunkCache chunkCache;
        /**
         * Every size-classed magazine of the heap this magazine belongs to, including this one. The whole array is
         * covered by the one lock (shared stripe) or the one owner thread (thread-local heap) that guards this
         * magazine, which is what makes the heap-wide drain legal from here.
         */
        private final SizeClassMagazine[] heapMagazines;
        final int sizeClassIndex;
        private final IdleDecay idleDecay;
        final AdaptiveRecycler bufRecycler; // for ByteBuf wrapper pooling; null → EVENT_LOOP_LOCAL_BUFFER_POOL
        /** The heap's sequence: see {@link PageStore#nextHeapSequence}. */
        final int seq;
        private final int purgeTickThreshold;
        private int allocCount;
        /** Purge ticks so far; with {@link #allocCount} it tells whether the class allocated since the last decay. */
        private int purgeTicks;
        private int purgeTicksAtDecay;
        private int allocCountAtDecay;

        SizeClassMagazine(AdaptivePoolingAllocator allocator, IdleDecay idleDecay, int sizeClassIndex,
                          Thread ownerThread, AdaptiveRecycler bufRecycler, StampedLock stripeLock,
                          SizeClassMagazine[] heapMagazines, int seq) {
            this.idleDecay = idleDecay;
            this.seq = seq;
            this.heapMagazines = heapMagazines;
            this.allocator = allocator;
            this.ownerThread = ownerThread;
            this.sizeClassIndex = sizeClassIndex;
            this.bufRecycler = bufRecycler;
            int segmentSize = SIZE_CLASSES[sizeClassIndex];
            int chunkSize = allocator.chunkSizes[allocator.sizeClassToChunkPool[sizeClassIndex]];
            this.chunkController = new SizeClassChunkController(segmentSize, chunkSize);
            this.chunkCache = new SizeClassedChunkCache(stripeLock);
            this.purgeTickThreshold = (int) Math.min(Integer.MAX_VALUE,
                    CHUNK_PURGE_INTERVAL * (chunkSize / segmentSize));
        }

        /**
         * Count one successful allocation and, when the budget is spent, purge this magazine's cache
         * and those of every other size class on this heap, then count the allocations for the heap's
         * {@link IdleDecay}.
         *
         * <p>Call exactly once per successful {@link #allocate}.
         */
        void tickAllocPurge() {
            if (++allocCount >= purgeTickThreshold) {
                allocCount = 0;
                purgeTicks++;
                chunkCache.tickPurge();
                purgeHeapSiblings();
                idleDecay.count(purgeTickThreshold);
            }
        }

        /**
         * Called by the heap's {@link IdleDecay}: a size class that made no allocation since the previous decay gives
         * up its current chunk and every wholly free chunk it keeps, the one it keeps as its floor included. Its floor
         * is for a class in use; one unused through a whole interval keeps nothing. Chunks with buffers out stay.
         * Reads only counters the allocations already keep.
         */
        void decayIfIdle() {
            int ticks = purgeTicks;
            int allocs = allocCount;
            boolean idle = ticks == purgeTicksAtDecay && allocs == allocCountAtDecay;
            purgeTicksAtDecay = ticks;
            allocCountAtDecay = allocs;
            if (!idle) {
                return;
            }
            SizeClassedChunk curr = current;
            if (curr != null && curr.hasFullCapacity()) {
                current = null;
                curr.releaseFromMagazine();
            }
            chunkCache.evictWhollyFree();
        }

        /**
         * Purge the caches of the other size classes on this heap. A size class that has gone idle
         * stops allocating, so it would never fire its own tick — and those are exactly the caches
         * worth purging, because the spans they give up serve every size class of every heap.
         */
        private void purgeHeapSiblings() {
            SizeClassMagazine[] mags = heapMagazines;
            for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
                SizeClassMagazine sibling = mags[i];
                if (sibling != null && sibling != this) {
                    sibling.chunkCache.tickPurge();
                }
            }
        }

        /**
         * Apply the notifications left by releasers on every size class of this heap, not just this
         * magazine's. A size class that has gone idle stops allocating, so it would never drain its
         * own notes — and those are exactly the chunks worth reclaiming, because the spans they give
         * up serve every size class of every heap.
         *
         * <p>Called on the allocation slow path only, right before {@link SizeClassedChunkCache#pollChunk},
         * which is once per chunk-worth of allocations. One volatile read per magazine when there are no notes.
         *
         * <p>This magazine's own cache is skipped: {@code pollChunk} drains it on the very next
         * line, which is both the last moment before the poll and therefore the freshest - it also
         * catches notes that landed while the other size classes were being drained.
         */
        private void drainHeapPending() {
            SizeClassMagazine[] mags = heapMagazines;
            for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
                SizeClassMagazine mag = mags[i];
                if (mag != null && mag != this) {
                    mag.chunkCache.drainPending();
                }
            }
        }

        boolean allocate(int size, int maxCapacity, AdaptiveByteBuf buf) {
            int startingCapacity = chunkController.computeBufferCapacity(maxCapacity);
            SizeClassedChunk curr = current;
            if (curr != null) {
                boolean success = curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                if (!success || curr.remainingCapacity() == 0) {
                    // Out of segments: give the chunk up. If a segment comes back from another thread after the
                    // count above, deactivate files the chunk as reusable by its capacity, so a later poll can hand
                    // it back. The !success case is defensive: the previous call left remainingCapacity() > 0,
                    // which counts only free segments, and this magazine is the only consumer of its chunk's free
                    // lists, so the read above always finds a segment.
                    current = null;
                    curr.releaseFromMagazine();
                }
                if (success) {
                    return true;
                }
            }
            return allocateSlow(size, maxCapacity, buf, startingCapacity);
        }

        /**
         * The current chunk (if any) had no room. Poll the cache, then fall back to allocating a fresh chunk.
         * Whichever chunk ends up serving the allocation becomes the cache's active chunk, which no cache decision
         * but an idle size class's decay touches, and is aliased by {@link #current} for the fast path.
         */
        private boolean allocateSlow(int size, int maxCapacity, AdaptiveByteBuf buf, int startingCapacity) {
            assert current == null;
            SizeClassedChunk curr;
            boolean polledChunkWithoutSegment = false;

            // Now try to poll from the cache first
            drainHeapPending();
            curr = chunkCache.pollChunk();
            if (curr != null) {
                chunkCache.activate(curr);
                // The size-class cache only hands out chunks with a free segment, and a segment always fits the size,
                // so this never happens; if that invariant ever broke, fall back to a fresh chunk rather than fail.
                if (curr.remainingCapacity() < size) {
                    polledChunkWithoutSegment = true;
                    curr.releaseFromMagazine();
                    curr = null;
                }
            }
            if (curr == null) {
                curr = chunkController.newChunkAllocation(this);
                chunkCache.activate(curr);
            }

            // The active chunk stays the cache's (see SizeClassedChunkCache#active); current is only the fast
            // path's alias of it.
            current = curr;
            // Checked only now, with the fallback chunk active and aliased, so that with assertions enabled the
            // failure leaves the magazine and its cache consistent.
            assert !polledChunkWithoutSegment : "the cache handed out a chunk without a free segment";
            boolean success;
            try {
                int remainingCapacity = curr.remainingCapacity();
                assert remainingCapacity >= size;
                if (remainingCapacity > startingCapacity) {
                    success = curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                    curr = null;
                } else {
                    success = curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
                }
            } finally {
                if (curr != null) {
                    // Release in a finally block so even if readInitInto(...) would throw we would still correctly
                    // release the current chunk before null it out.
                    curr.releaseFromMagazine();
                    current = null;
                }
            }
            return success;
        }

        void free() {
            if (current != null) {
                current.releaseFromMagazine();
                current = null;
            }
            chunkCache.free();
        }

        AdaptiveByteBuf newBuffer() {
            AdaptiveByteBuf buf = bufRecycler != null ? bufRecycler.get() : EVENT_LOOP_LOCAL_BUFFER_POOL.get();
            buf.resetRefCnt();
            buf.discardMarks();
            return buf;
        }
    }

    /**
     * Buffers above the size classes, up to a block, when the allocator has a {@link PageStore}: each one is a span of
     * whole slices of the store's shared slices (see {@link PageStore#claimSlices}), sized to the buffer rounded up to
     * slices, as mimalloc's large pages. No block of a chunk, nothing kept for reuse: a release, from any thread,
     * gives the span back to the store at once (see {@link SharedSpanChunk}).
     * <p>
     * The heap's owner (the stripe lock holder, or the thread of a thread-local heap) claims the spans.
     */
    private static final class SpanMagazine {
        /** An allocation counts toward the heap's {@link IdleDecay} as its size in units of the smallest size class. */
        private static final int COUNT_SHIFT = 5;
        private static final int COLOUR_SHIFT = 6;
        /** 4032 bytes at most, as mimalloc's large colours: https://github.com/microsoft/mimalloc/pull/1339 */
        private static final int MAX_COLOURS = 64;

        final AdaptivePoolingAllocator allocator;
        /** The heap's sequence: see {@link PageStore#nextHeapSequence}. */
        private final int seq;
        private final AdaptiveRecycler bufRecycler;
        private final Thread ownerThread;
        private final IdleDecay idleDecay;
        final int sliceShift;
        private final int segmentSlices;
        /** Round robin over the colours of the spans; single writer, as the magazine. */
        private int nextColour;

        SpanMagazine(AdaptivePoolingAllocator allocator, AdaptiveRecycler bufRecycler, Thread ownerThread,
                     IdleDecay idleDecay, int seq) {
            this.allocator = allocator;
            this.bufRecycler = bufRecycler;
            this.ownerThread = ownerThread;
            this.idleDecay = idleDecay;
            this.seq = seq;
            int sliceSize = allocator.pageStore.config.sliceSize;
            assert (sliceSize & sliceSize - 1) == 0 : "slices of a power of two";
            sliceShift = Integer.numberOfTrailingZeros(sliceSize);
            segmentSlices = allocator.pageStore.config.slicesPerSegment();
        }

        void allocate(int size, int maxCapacity, AdaptiveByteBuf buf) {
            int sliceShift = this.sliceShift;
            int slices = (int) ((size + (1L << sliceShift) - 1) >>> sliceShift);
            if (slices > segmentSlices) {
                // Segments smaller than the buffer (a small configured segment size): a buffer of its own.
                allocator.allocateOneShot(size, maxCapacity, buf);
                return;
            }
            PageStore store = allocator.pageStore;
            long run = store.claimSlices(slices, seq, ownerThread != null);
            Segment segment = store.block(run);
            int start = store.start(run);
            // Colour, as the size classes' spans (see SizeClassChunkController) and as mimalloc does for its large
            // allocations (https://github.com/microsoft/mimalloc/pull/1339, issue #1121): the buffer starts up to
            // 4032 bytes into its span, in 64-byte steps taken round robin, out of the tail the span leaves unused
            // past the buffer, so it costs no memory. A release rounds its capacity up to whole slices again.
            int colours = Math.min(MAX_COLOURS, (int) (((long) slices << sliceShift) - size >>> COLOUR_SHIFT) + 1);
            int colour = colours == 1 ? 0 : (nextColour++ & Integer.MAX_VALUE) % colours << COLOUR_SHIFT;
            // A block has one chunk for the spans of every thread-local heap, and one for every stripe's.
            Chunk chunk = ownerThread != null ? segment.threadLocalSpans : segment.sharedSpans;
            boolean initialized = false;
            try {
                buf.init(segment.buffer, chunk, 0, 0, (start << sliceShift) + colour, size,
                        (slices << sliceShift) - colour, maxCapacity);
                initialized = true;
            } finally {
                if (!initialized) {
                    segment.releaseRun(start, slices, System.nanoTime());
                }
            }
            idleDecay.count(size >>> COUNT_SHIFT);
        }

        AdaptiveByteBuf newBuffer() {
            AdaptiveByteBuf buf = bufRecycler != null ? bufRecycler.get()
                    : SizeClassMagazine.EVENT_LOOP_LOCAL_BUFFER_POOL.get();
            buf.resetRefCnt();
            buf.discardMarks();
            return buf;
        }
    }

    /**
     * The chunk of every large-buffer span of one block of a region's shared slices, whichever heap claimed it (see
     * {@link PageStore#claimSlices}): a release, from any thread, gives the span's slices back to the store at once.
     * Made with its region; nothing is queued, nothing is owned.
     */
    static final class SharedSpanChunk extends Chunk {
        private final PageStore store;
        final Segment block;
        private final int sliceShift;
        private final boolean threadLocal;

        /** @param threadLocal whether the heaps whose spans this chunk holds are thread-local ones, for JFR */
        SharedSpanChunk(Segment block, PageStore store, boolean threadLocal) {
            super(block.buffer, store.allocator, true);
            this.store = store;
            this.block = block;
            this.threadLocal = threadLocal;
            assert (block.sliceSize & block.sliceSize - 1) == 0 : "slices of a power of two";
            sliceShift = Integer.numberOfTrailingZeros(block.sliceSize);
        }

        /** Any thread: the span of {@code length} bytes at {@code offset}, colour included, is free. */
        @Override
        void releaseSegment(int offset, int length) {
            int shift = sliceShift;
            block.releaseRun(offset >>> shift, length + (1 << shift) - 1 >>> shift, System.nanoTime());
        }

        @Override
        boolean inThreadLocalMagazine() {
            return threadLocal;
        }

        @Override
        public String toString() {
            return "SharedSpanChunk[" + block + ']';
        }
    }

    private static final class ChunkRegistry {
        private final LongAdder totalCapacity = new LongAdder();

        public long totalCapacity() {
            return totalCapacity.sum();
        }

        void add(int bytes) {
            totalCapacity.add(bytes);
        }

        void remove(int bytes) {
            totalCapacity.add(-bytes);
        }
    }

    /**
     * What every chunk has in common, pooled or not: the buffer it carves allocations out of and the allocator that
     * owns it. Accounting and JFR events follow the buffer, not the chunk: see {@link #usedMemory()}.
     */
    abstract static class Chunk implements ChunkInfo {
        /**
         * The {@link ChunkQueue} of its magazine's cache this chunk is filed on, or {@code null}: the magazine's
         * active chunk, a chunk just polled, or one that left the cache. The release paths take a cache decision only
         * when it is on a queue, with one null check.
         */
        ChunkQueue queue;
        // Links of the ChunkQueue this chunk is on, if any. nextInQueue also links an abandoned chunk in its
        // store's stack (see PageStore#abandon), which it joins off every queue.
        Chunk prevInQueue;
        Chunk nextInQueue;
        /**
         * Link in its cache's {@link PendingChunks}: {@code null} = not queued for attention, non-null = queued (or in
         * the middle of being queued). This field <em>is</em> the dedup claim: whoever moves it off {@code null} owns
         * the push, so no separate flag is needed.
         */
        volatile Chunk pendingNext;
        /** Final but for a size-class chunk, which has one per incarnation (see {@code SizeClassedChunk#reinit}). */
        protected AbstractByteBuf delegate;
        // We need the top-level allocator so ByteBuf.capacity(int) can call reallocate()
        final AdaptivePoolingAllocator allocator;
        final int capacity;
        private final boolean pooled;

        Chunk() {
            // Constructor only used by the PendingChunks end marker.
            delegate = null;
            allocator = null;
            capacity = 0;
            pooled = false;
        }

        /** @param pooled whether the chunk serves many buffers, or is a one-shot chunk for a single buffer */
        Chunk(AbstractByteBuf delegate, AdaptivePoolingAllocator allocator, boolean pooled) {
            this(delegate, allocator, pooled, delegate.capacity());
        }

        /** @param capacity the bytes of {@code delegate} this chunk hands out: all of them, unless it is a span */
        Chunk(AbstractByteBuf delegate, AdaptivePoolingAllocator allocator, boolean pooled, int capacity) {
            this.delegate = delegate;
            this.pooled = pooled;
            this.capacity = capacity;
            this.allocator = allocator;
        }

        /**
         * Called when a ByteBuf is done using its allocation in this chunk.
         */
        abstract void releaseSegment(int startIndex, int size);

        /**
         * Whether this chunk is attached to a magazine of a thread-local heap right now, for the JFR events.
         */
        boolean inThreadLocalMagazine() {
            return false;
        }

        @Override
        public int capacity() {
            return capacity;
        }

        @Override
        public boolean isDirect() {
            return delegate.isDirect();
        }

        @Override
        public long memoryAddress() {
            return delegate._memoryAddress();
        }
    }

    /**
     * A chunk cut into segments of one size class, a span of its block from slice {@link #spanStart}.
     * <p>
     * <b>Free segments</b> are not listed anywhere outside the block: a free segment holds, in its own first four
     * bytes, the offset of the next free one (mimalloc's block free list). Offsets are the block's, from {@link #base}.
     * The chunk keeps
     * <ul>
     *   <li>{@link #head}: the first segment released by the owner thread, or by a thread holding the stripe lock.
     *       The last segment released is the first handed out again;</li>
     *   <li>{@link #bump}: the first segment never handed out. These are taken in order and need no link, so a new
     *       chunk fills nothing, and whatever the memory holds is ignored;</li>
     *   <li>{@link #localFree}: how many segments those two account for;</li>
     *   <li>{@link #externalFree}: the segments released by any other thread, as one {@code long} holding their
     *       count above the offset of the first. A releaser writes its link and then publishes the new first and the
     *       count with one CAS; the owner takes the whole list, and its count, with one {@code getAndSet} when
     *       {@link #head} and {@link #bump} have run out. Only pushes race with each other and the list is only ever
     *       taken whole, so there is no ABA.</li>
     * </ul>
     * Every question about capacity is arithmetic on the counts: nothing walks a list.
     * <p>
     * <b>A corrupted link.</b> A link is checked when it is read: it must be another segment of this chunk, or the
     * end. Anything else means that a buffer was written after it was released. The allocation fails, the chain is
     * forgotten ({@link #corruptedFreeList}) and its segments, still free, are counted in {@link #lostFree}, so the
     * chunk keeps working and is still given up. A link overwritten with the offset of another segment cannot be
     * detected, and a release that reaches a chunk after it gave its span up writes into memory that is no longer its
     * own: neither can happen without using a buffer after releasing it.
     * <p>
     * <b>Incarnations.</b> A chunk object outlives its span: given up, it waits on its cache's idle list and serves the
     * cache's next chunk on whatever span the store has ({@link #reinit}); its colour stays, so its capacity too.
     * <p>
     * <b>One owner.</b> Every field but {@link #externalFree} and {@code pendingNext} belongs to the chunk's owner: the
     * owner thread, or the holder of the stripe lock, or, once the heap died with buffers out, the store's purger (see
     * {@link #releaseOrAbandon}). Any other releaser writes its segment's link, pushes it with one CAS on
     * {@link #externalFree}, and leaves a note for the owner; it reads nothing else of the chunk. Only the owner
     * decides that every segment is back ({@link #hasFullCapacity}: one volatile read) and gives the span up, so a
     * span goes back once and a late release cannot find it gone: before its CAS a segment is out, so the chunk is
     * not all free. After its CAS a releaser touches only the final {@link #owningCache} and {@code pendingNext},
     * which mean the same for every incarnation: its note may reach a later one, and is harmless, since a note only
     * says "look at this chunk" and the owner files it by what it is now.
     */
    static class SizeClassedChunk extends Chunk {
        static final int FREE_LIST_EMPTY = -1;
        /** No segment and a count of zero: see {@link #externalFree}. */
        private static final long EXTERNAL_EMPTY = 0xFFFFFFFFL;
        private static final AtomicLongFieldUpdater<SizeClassedChunk> EXTERNAL_FREE =
                AtomicLongFieldUpdater.newUpdater(SizeClassedChunk.class, "externalFree");
        private final int segments;
        private final int segmentSize;
        /** How far into each span the segments start: 64-byte steps, one per chunk object. */
        private final int colour;
        // Per incarnation, set by reinit.
        /** Offset of the first segment. */
        private int base;
        /** Offset of the first segment released by the owner thread or under the stripe lock. */
        private int head;
        /** Offset of the first segment never handed out; they are taken in order up to {@link #bumpLimit}. */
        private int bump;
        private int bumpLimit;
        /** Offset of the last segment: the largest link a free segment can hold. */
        private int lastSegmentOffset;
        /** The segments reachable from {@link #head} plus those never handed out. */
        private int localFree;
        /**
         * Free segments that can no longer be reached, because the chain they were on held a corrupted link
         * ({@link #corruptedFreeList}). Never handed out again, but counted as free, so that the chunk empties.
         */
        private int lostFree;
        /** Segments released by other threads: their count in the high half, the first one's offset in the low. */
        private volatile long externalFree;
        /** {@code null} on a stripe, and once abandoned: see {@link #releaseOrAbandon}. */
        private Thread ownerThread;
        /**
         * Snapshot behind {@link #remainingCapacity()}: bytes handed out since the last refresh from the free counts.
         * Segments returned since then are not subtracted, so {@code capacity - allocatedBytes} never counts a
         * segment that is not free.
         */
        private int allocatedBytes;

        final SizeClassedChunkCache owningCache;
        /** The block this chunk is a span of, from slice {@link #spanStart}; {@code null} for the end marker. */
        // Visible for testing.
        Segment segment;
        int spanStart;

        /**
         * Constructor only used by {@link PendingChunks}' end marker.
         */
        SizeClassedChunk() {
            segmentSize = 0;
            segments = 0;
            colour = 0;
            ownerThread = null;
            owningCache = null;
        }

        /** A chunk object of {@code magazine}'s cache, with no span until {@link #reinit}. */
        SizeClassedChunk(SizeClassMagazine magazine, SizeClassChunkController controller, int colour) {
            super(null, magazine.allocator, true, controller.chunkSize - colour);
            segmentSize = controller.segmentSize;
            segments = controller.buffers;
            this.colour = colour;
            ownerThread = magazine.ownerThread;
            owningCache = magazine.chunkCache;
        }

        /**
         * Owner: this object becomes a chunk on the span of {@code segment} from slice {@code spanStart}, every segment
         * free and none handed out; nothing is filled. Every release of an earlier incarnation pushed its segment
         * before that one was given up, so none can push on this one.
         */
        void reinit(Segment segment, int spanStart) {
            this.segment = segment;
            this.spanStart = spanStart;
            // The block's own buffer, as a large-buffer span reads it: no buffer object per chunk.
            delegate = segment.buffer;
            int base = spanStart * segment.sliceSize + colour;
            this.base = base;
            bump = base;
            bumpLimit = base + segments * segmentSize;
            lastSegmentOffset = bumpLimit - segmentSize;
            head = FREE_LIST_EMPTY;
            localFree = segments;
            lostFree = 0;
            allocatedBytes = 0;
            externalFree = EXTERNAL_EMPTY;
        }

        /**
         * Called when a magazine is done using this chunk, probably because it was emptied: it stops being the
         * cache's active chunk and is filed by capacity. {@link #owningCache} is the cache of the one magazine that
         * ever allocates from this chunk.
         */
        void releaseFromMagazine() {
            owningCache.deactivate(this);
        }

        /**
         * Only read from {@link AdaptiveByteBuf#init}, reached from {@link #readInitInto} on the magazine's active
         * chunk, so this chunk is always attached to its magazine here, and that magazine is a thread-local one exactly
         * when this chunk has an owner thread.
         */
        @Override
        boolean inThreadLocalMagazine() {
            return ownerThread != null;
        }

        boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            final int startIndex = nextAvailableSegmentOffset();
            if (startIndex == FREE_LIST_EMPTY) {
                return false;
            }
            allocatedBytes += segmentSize;
            try {
                buf.init(delegate, this, 0, 0, startIndex, size, startingCapacity, maxCapacity);
            } catch (Throwable t) {
                allocatedBytes -= segmentSize;
                releaseSegmentOffsetIntoFreeList(startIndex);
                throw t;
            }
            return true;
        }

        private int nextAvailableSegmentOffset() {
            int head = this.head;
            if (head != FREE_LIST_EMPTY) {
                this.head = nextFreeAfter(head);
                localFree--;
                return head;
            }
            int bump = this.bump;
            if (bump < bumpLimit) {
                this.bump = bump + segmentSize;
                localFree--;
                return bump;
            }
            return takeExternalFree();
        }

        /** Visible for testing: take every free segment without telling the cache, and return how many there were. */
        int takeAllFreeSegments() {
            int taken = 0;
            while (nextAvailableSegmentOffset() != FREE_LIST_EMPTY) {
                taken++;
            }
            return taken;
        }

        /** Take every segment other threads have released, and hand out the first. */
        private int takeExternalFree() {
            if (externalFree == EXTERNAL_EMPTY) {
                return FREE_LIST_EMPTY;
            }
            long taken = EXTERNAL_FREE.getAndSet(this, EXTERNAL_EMPTY);
            int head = (int) taken;
            // Counted before the first link is read, so that a corrupted one forgets exactly these segments.
            localFree += externalCount(taken);
            this.head = nextFreeAfter(head);
            localFree--;
            return head;
        }

        private static int externalCount(long externalFree) {
            return (int) (externalFree >>> 32);
        }

        /** The link a free segment holds: the offset of the next free segment, or {@link #FREE_LIST_EMPTY}. */
        private int nextFreeAfter(int offset) {
            int next = delegate._getIntLE(offset);
            if (isCorruptedLink(offset, next)) {
                throw corruptedFreeList(offset);
            }
            return next;
        }

        // Apart from nextFreeAfter so that each stays within 35 bytes of bytecode, the size C1 and cold C2 call sites
        // inline.
        private boolean isCorruptedLink(int offset, int next) {
            return next != FREE_LIST_EMPTY && (next < base || next > lastSegmentOffset) || next == offset;
        }

        /**
         * A link that is not another segment of this chunk. The chain it belongs to cannot be trusted, so it is
         * forgotten, and the exception to throw is returned: its segments are never handed out again, but they
         * stay counted as free ({@link #lostFree}), so the chunk is still given up once the segments in use are back,
         * and it keeps serving the segments never handed out and those released from now on. The capacity snapshot is
         * dropped with the chain, so that the next query counts again.
         */
        private IllegalStateException corruptedFreeList(int offset) {
            int next = delegate._getIntLE(offset);
            int reachable = (bumpLimit - bump) / segmentSize;
            lostFree += localFree - reachable;
            localFree = reachable;
            head = FREE_LIST_EMPTY;
            allocatedBytes = capacity;
            return new IllegalStateException("free segment at " + offset + " links to " + next
                    + ": a buffer was written after it was released");
        }

        private void pushLocalFree(int offset) {
            delegate._setIntLE(offset, head);
            head = offset;
            localFree++;
        }

        // Package-private for the tests that cut a release from another thread in two: this, then the note.
        void pushExternalFree(int offset) {
            long current;
            long pushed;
            do {
                current = externalFree;
                delegate._setIntLE(offset, (int) current);
                pushed = (long) externalCount(current) + 1 << 32 | offset & 0xFFFFFFFFL;
            } while (!EXTERNAL_FREE.compareAndSet(this, current, pushed));
        }

        /**
         * Whether this chunk has a free segment, as the cache files it (reusable or exhausted) and probes it.
         * Unlike {@link #remainingCapacity()} it never refreshes the snapshot.
         */
        public boolean hasRemainingCapacity() {
            int remaining = capacity - allocatedBytes;
            if (remaining > 0) {
                return true;
            }
            return localFree > 0 || externalFree != EXTERNAL_EMPTY;
        }

        boolean hasFullCapacity() {
            return localFree + lostFree + externalCount(externalFree) == segments;
        }

        /**
         * The free bytes of this chunk as the magazine sees it after each allocation. While the snapshot is above
         * one segment it is returned as is, without reading the free counts; at or below one segment the free
         * segments are counted and the snapshot refreshed. Before the first refresh the snapshot also counts the
         * tail of the chunk that is too small for a segment, when the chunk size is not a multiple of the segment
         * size.
         */
        public int remainingCapacity() {
            int remaining = capacity - allocatedBytes;
            return remaining > segmentSize ? remaining : updateRemainingCapacity(remaining);
        }

        private int updateRemainingCapacity(int snapshotted) {
            int freeSegments = externalCount(externalFree) + localFree;
            int updated = freeSegments * segmentSize;
            if (updated != snapshotted) {
                allocatedBytes = capacity() - updated;
            }
            return updated;
        }

        private void releaseSegmentOffsetIntoFreeList(int startIndex) {
            if (ownerThread != null && Thread.currentThread() == ownerThread) {
                pushLocalFree(startIndex);
            } else {
                pushExternalFree(startIndex);
            }
        }

        @Override
        void releaseSegment(int startIndex, int size) {
            if (ownerThread != null && Thread.currentThread() == ownerThread) {
                pushLocalFree(startIndex);
                // Neither a chunk out of the cache nor the magazine's active chunk is ever moved or evicted
                // by a segment return: the active chunk consumes its own returned segments.
                if (queue != null) {
                    owningCache.refile(this);
                }
                return;
            }
            final SizeClassedChunkCache cache = owningCache;
            final long stamp = cache.tryLockForRelease();
            if (stamp != 0) {
                try {
                    pushLocalFree(startIndex);
                    if (queue != null) {
                        cache.refile(this);
                    }
                } finally {
                    cache.unlockAfterRelease(stamp);
                }
                return;
            }
            pushExternalFree(startIndex);
            // The chunk just gained capacity but we could not take the lock to apply the resulting list
            // transition. Leave a note instead; the next drain applies it.
            cache.notifyHasCapacity(this);
        }

        /** The slices of the span this chunk is; its colour is less than one. */
        int spanSlices() {
            return (capacity + segment.sliceSize - 1) / segment.sliceSize;
        }

        /** Owner, with every segment back: the span goes back to the store's shared slices. */
        void releaseSpan() {
            assert hasFullCapacity();
            segment.releaseRun(spanStart, spanSlices(), System.nanoTime());
        }

        /**
         * Owner, as its heap dies: the span goes back now if every segment is back; else the chunk is abandoned to
         * the store, whose purger becomes its owner and gives the span back at the first pass that finds every
         * segment back ({@link #releaseIfAllFree}). From here on every release, the dying thread's own included,
         * takes the CAS path: the owner thread is forgotten.
         */
        void releaseOrAbandon() {
            if (hasFullCapacity()) {
                releaseSpan();
                return;
            }
            ownerThread = null;
            allocator.pageStore.abandon(this);
        }

        /**
         * Purger, for an abandoned chunk: gives the span back if every segment is back. A release that wins a dead
         * stripe's lock still counts on the owner's side, so the purger takes that lock to look, and looks again at
         * its next pass when the lock is taken.
         */
        boolean releaseIfAllFree() {
            SizeClassedChunkCache cache = owningCache;
            long stamp = cache.tryLockForRelease();
            if (stamp == 0 && cache.stripeLock != null) {
                return false;
            }
            try {
                if (!hasFullCapacity()) {
                    return false;
                }
                releaseSpan();
                return true;
            } finally {
                if (stamp != 0) {
                    cache.unlockAfterRelease(stamp);
                }
            }
        }

        // Visible for testing.
        int externalFreeCount() {
            return externalCount(externalFree);
        }

        // Visible for testing.
        int freeSegmentCount() {
            return localFree + lostFree + externalCount(externalFree);
        }

        // Visible for testing.
        Thread ownerThread() {
            return ownerThread;
        }
    }

    /**
     * One buffer that owns its memory, freed with it by whichever thread releases it: a run of whole blocks of the
     * store for a buffer above a block, else an allocation of its own (a buffer larger than a region, the busy-stripe
     * fallback, low-memory mode).
     */
    private static final class OneShotChunk extends Chunk {
        /** The region whose blocks {@link #runStart} to {@link #runStart} + {@link #runSlots} this is, or null. */
        private final Region region;
        private final int runStart;
        private final int runSlots;

        OneShotChunk(AbstractByteBuf delegate, AdaptivePoolingAllocator allocator, Region region, int runStart,
                     int runSlots) {
            super(delegate, allocator, false);
            this.region = region;
            this.runStart = runStart;
            this.runSlots = runSlots;
        }

        /** Any thread, once: the run goes back to the store, or the allocation is freed. */
        @Override
        void releaseSegment(int startIndex, int size) {
            if (region != null) {
                allocator.pageStore.freeRun(region, runStart, runSlots);
            } else {
                allocator.chunkBufferFreed(this, false);
                delegate.release();
            }
        }

        @Override
        public String toString() {
            return "OneShotChunk[capacity: " + delegate.capacity() + ']';
        }
    }

    static final class AdaptiveByteBuf extends AbstractReferenceCountedByteBuf {

        private final EnhancedHandle<AdaptiveByteBuf> handle;

        // this both act as adjustment and the start index for a free list segment allocation
        private int startIndex;
        private AbstractByteBuf rootParent;
        Chunk chunk;
        private int length;
        private int maxFastCapacity;
        private ByteBuffer tmpNioBuf;
        private boolean hasArray;
        private boolean hasMemoryAddress;

        AdaptiveByteBuf(EnhancedHandle<AdaptiveByteBuf> recyclerHandle) {
            super(0);
            handle = ObjectUtil.checkNotNull(recyclerHandle, "recyclerHandle");
        }

        void init(AbstractByteBuf unwrapped, Chunk wrapped, int readerIndex, int writerIndex,
                  int startIndex, int size, int capacity, int maxCapacity) {
            this.startIndex = startIndex;
            chunk = wrapped;
            length = size;
            maxFastCapacity = capacity;
            maxCapacity(maxCapacity);
            setIndex0(readerIndex, writerIndex);
            hasArray = unwrapped.hasArray();
            hasMemoryAddress = unwrapped.hasMemoryAddress();
            rootParent = unwrapped;
            tmpNioBuf = null;

            if (PlatformDependent.isJfrEnabled() && AllocateBufferEvent.isEventEnabled()) {
                AllocateBufferEvent event = new AllocateBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.chunkPooled = wrapped.pooled;
                    event.chunkThreadLocal = wrapped.inThreadLocalMagazine();
                    event.commit();
                }
            }
        }

        private AbstractByteBuf rootParent() {
            final AbstractByteBuf rootParent = this.rootParent;
            if (rootParent != null) {
                return rootParent;
            }
            throw new IllegalReferenceCountException();
        }

        @Override
        public int capacity() {
            return length;
        }

        @Override
        public int maxFastWritableBytes() {
            return Math.min(maxFastCapacity, maxCapacity()) - writerIndex;
        }

        @Override
        public ByteBuf capacity(int newCapacity) {
            checkNewCapacity(newCapacity);
            if (length <= newCapacity && newCapacity <= maxFastCapacity) {
                length = newCapacity;
                return this;
            }
            if (newCapacity < capacity()) {
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }

            if (PlatformDependent.isJfrEnabled() && ReallocateBufferEvent.isEventEnabled()) {
                ReallocateBufferEvent event = new ReallocateBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.newCapacity = newCapacity;
                    event.commit();
                }
            }

            // Reallocation required.
            Chunk chunk = this.chunk;
            AdaptivePoolingAllocator allocator = chunk.allocator;
            int readerIndex = this.readerIndex;
            int writerIndex = this.writerIndex;
            int baseOldRootIndex = startIndex;
            int oldLength = length;
            int oldCapacity = maxFastCapacity;
            AbstractByteBuf oldRoot = rootParent();
            allocator.reallocate(newCapacity, maxCapacity(), this);
            oldRoot.getBytes(baseOldRootIndex, this, 0, oldLength);
            chunk.releaseSegment(baseOldRootIndex, oldCapacity);
            assert oldCapacity < maxFastCapacity && newCapacity <= maxFastCapacity :
                    "Capacity increase failed";
            this.readerIndex = readerIndex;
            this.writerIndex = writerIndex;
            return this;
        }

        @Override
        public ByteBufAllocator alloc() {
            return rootParent().alloc();
        }

        @SuppressWarnings("deprecation")
        @Override
        public ByteOrder order() {
            return rootParent().order();
        }

        @Override
        public ByteBuf unwrap() {
            return null;
        }

        @Override
        public boolean isDirect() {
            return rootParent().isDirect();
        }

        @Override
        public int arrayOffset() {
            return idx(rootParent().arrayOffset());
        }

        @Override
        public boolean hasMemoryAddress() {
            return hasMemoryAddress;
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return _memoryAddress();
        }

        @Override
        long _memoryAddress() {
            AbstractByteBuf root = rootParent;
            return root != null ? root._memoryAddress() + startIndex : 0L;
        }

        @Override
        boolean _isDirect() {
            AbstractByteBuf root = rootParent;
            return root != null && root.isDirect();
        }

        @Override
        public ByteBuffer nioBuffer(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffer(idx(index), length);
        }

        @Override
        public ByteBuffer internalNioBuffer(int index, int length) {
            checkIndex(index, length);
            return (ByteBuffer) internalNioBuffer().position(index).limit(index + length);
        }

        private ByteBuffer internalNioBuffer() {
            if (tmpNioBuf == null) {
                tmpNioBuf = rootParent().nioBuffer(startIndex, maxFastCapacity);
            }
            return (ByteBuffer) tmpNioBuf.clear();
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffers(idx(index), length);
        }

        @Override
        public boolean hasArray() {
            return hasArray;
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            return rootParent().array();
        }

        @Override
        public ByteBuf copy(int index, int length) {
            checkIndex(index, length);
            return rootParent().copy(idx(index), length);
        }

        @Override
        public int nioBufferCount() {
            return rootParent().nioBufferCount();
        }

        @Override
        protected byte _getByte(int index) {
            return rootParent()._getByte(idx(index));
        }

        @Override
        protected short _getShort(int index) {
            return rootParent()._getShort(idx(index));
        }

        @Override
        protected short _getShortLE(int index) {
            return rootParent()._getShortLE(idx(index));
        }

        @Override
        protected int _getUnsignedMedium(int index) {
            return rootParent()._getUnsignedMedium(idx(index));
        }

        @Override
        protected int _getUnsignedMediumLE(int index) {
            return rootParent()._getUnsignedMediumLE(idx(index));
        }

        @Override
        protected int _getInt(int index) {
            return rootParent()._getInt(idx(index));
        }

        @Override
        protected int _getIntLE(int index) {
            return rootParent()._getIntLE(idx(index));
        }

        @Override
        protected long _getLong(int index) {
            return rootParent()._getLong(idx(index));
        }

        @Override
        protected long _getLongLE(int index) {
            return rootParent()._getLongLE(idx(index));
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuffer dst) {
            checkIndex(index, dst.remaining());
            rootParent().getBytes(idx(index), dst);
            return this;
        }

        @Override
        protected void _setByte(int index, int value) {
            rootParent()._setByte(idx(index), value);
        }

        @Override
        protected void _setShort(int index, int value) {
            rootParent()._setShort(idx(index), value);
        }

        @Override
        protected void _setShortLE(int index, int value) {
            rootParent()._setShortLE(idx(index), value);
        }

        @Override
        protected void _setMedium(int index, int value) {
            rootParent()._setMedium(idx(index), value);
        }

        @Override
        protected void _setMediumLE(int index, int value) {
            rootParent()._setMediumLE(idx(index), value);
        }

        @Override
        protected void _setInt(int index, int value) {
            rootParent()._setInt(idx(index), value);
        }

        @Override
        protected void _setIntLE(int index, int value) {
            rootParent()._setIntLE(idx(index), value);
        }

        @Override
        protected void _setLong(int index, long value) {
            rootParent()._setLong(idx(index), value);
        }

        @Override
        protected void _setLongLE(int index, long value) {
            rootParent()._setLongLE(idx(index), value);
        }

        @Override
        public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
            checkIndex(index, length);
            if (tmpNioBuf == null && PlatformDependent.javaVersion() >= 13) {
                ByteBuffer dstBuffer = rootParent()._internalNioBuffer();
                PlatformDependent.absolutePut(dstBuffer, idx(index), src, srcIndex, length);
            } else {
                ByteBuffer tmp = (ByteBuffer) internalNioBuffer().clear().position(index);
                tmp.put(src, srcIndex, length);
            }
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
            checkIndex(index, length);
            if (src instanceof AdaptiveByteBuf && PlatformDependent.javaVersion() >= 16) {
                AdaptiveByteBuf srcBuf = (AdaptiveByteBuf) src;
                srcBuf.checkIndex(srcIndex, length);
                ByteBuffer dstBuffer = rootParent()._internalNioBuffer();
                ByteBuffer srcBuffer = srcBuf.rootParent()._internalNioBuffer();
                PlatformDependent.absolutePut(dstBuffer, idx(index), srcBuffer, srcBuf.idx(srcIndex), length);
            } else {
                ByteBuffer tmp = internalNioBuffer();
                tmp.position(index);
                tmp.put(src.nioBuffer(srcIndex, length));
            }
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuffer src) {
            int length = src.remaining();
            checkIndex(index, length);
            if (src == tmpNioBuf) {
                src = src.duplicate();
            }
            ByteBuffer tmp = internalNioBuffer();
            if (PlatformDependent.javaVersion() >= 16) {
                int offset = src.position();
                PlatformDependent.absolutePut(tmp, index, src, offset, length);
                src.position(offset + length);
            } else {
                tmp.position(index);
                tmp.put(src);
            }
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, OutputStream out, int length)
                throws IOException {
            checkIndex(index, length);
            if (length != 0) {
                ByteBuffer tmp = internalNioBuffer();
                ByteBufUtil.readBytes(alloc(), tmp.hasArray() ? tmp : tmp.duplicate(), index, length, out);
            }
            return this;
        }

        @Override
        public int getBytes(int index, GatheringByteChannel out, int length)
                throws IOException {
            checkIndex(index, length);
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf);
        }

        @Override
        public int getBytes(int index, FileChannel out, long position, int length)
                throws IOException {
            checkIndex(index, length);
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf, position);
        }

        @Override
        public int setBytes(int index, InputStream in, int length)
                throws IOException {
            checkIndex(index, length);
            final AbstractByteBuf rootParent = rootParent();
            if (rootParent.hasArray()) {
                return rootParent.setBytes(idx(index), in, length);
            }
            byte[] tmp = ByteBufUtil.threadLocalTempArray(length);
            int readBytes = in.read(tmp, 0, length);
            if (readBytes <= 0) {
                return readBytes;
            }
            setBytes(index, tmp, 0, readBytes);
            return readBytes;
        }

        @Override
        public int setBytes(int index, ScatteringByteChannel in, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length));
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setBytes(int index, FileChannel in, long position, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length), position);
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setCharSequence(int index, CharSequence sequence, Charset charset) {
            return setCharSequence0(index, sequence, charset, false);
        }

        private int setCharSequence0(int index, CharSequence sequence, Charset charset, boolean expand) {
            if (charset.equals(CharsetUtil.UTF_8)) {
                int length = ByteBufUtil.utf8MaxBytes(sequence);
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                return ByteBufUtil.writeUtf8(this, index, length, sequence, sequence.length());
            }
            if (charset.equals(CharsetUtil.US_ASCII) || charset.equals(CharsetUtil.ISO_8859_1)) {
                int length = sequence.length();
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                return ByteBufUtil.writeAscii(this, index, sequence, length);
            }
            byte[] bytes = sequence.toString().getBytes(charset);
            if (expand) {
                ensureWritable0(bytes.length);
                // setBytes(...) will take care of checking the indices.
            }
            setBytes(index, bytes);
            return bytes.length;
        }

        @Override
        public int writeCharSequence(CharSequence sequence, Charset charset) {
            int written = setCharSequence0(writerIndex, sequence, charset, true);
            writerIndex += written;
            return written;
        }

        @Override
        public int forEachByte(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByte(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public int forEachByteDesc(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByteDesc(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public ByteBuf setZero(int index, int length) {
            checkIndex(index, length);
            rootParent().setZero(idx(index), length);
            return this;
        }

        @Override
        public ByteBuf writeZero(int length) {
            ensureWritable(length);
            rootParent().setZero(idx(writerIndex), length);
            writerIndex += length;
            return this;
        }

        private int forEachResult(int ret) {
            if (ret < startIndex) {
                return -1;
            }
            return ret - startIndex;
        }

        @Override
        public boolean isContiguous() {
            return rootParent().isContiguous();
        }

        private int idx(int index) {
            return index + startIndex;
        }

        @Override
        protected void deallocate() {
            if (PlatformDependent.isJfrEnabled() && FreeBufferEvent.isEventEnabled()) {
                FreeBufferEvent event = new FreeBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.commit();
                }
            }

            if (chunk != null) {
                chunk.releaseSegment(startIndex, maxFastCapacity);
            }
            tmpNioBuf = null;
            chunk = null;
            rootParent = null;
            handle.unguardedRecycle(this);
        }
    }
}
