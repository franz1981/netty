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

import io.netty.util.NettyRuntime;
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

import java.util.Queue;
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
 * All memory comes from the allocator's {@link PageStore}, shared by all heaps, as spans of its slices (its class
 * javadoc defines the units). Up to the largest size class ({@link SizeClassTable#SIZES}) a span is a
 * {@link SizeClassedChunk}, cut into equal slots of one size class, one {@link SizeClassMagazine} allocating from
 * it at a time (see its state diagram). Above it, up to a block, a span holds one buffer alone
 * ({@link Heap#allocateLarge}); larger still, or when no span can be had, a {@link OneShotChunk} holds that buffer
 * alone and is freed with it.
 * <p>
 * The magazines are grouped into stripe {@link Heap}s, each guarded by one lock, and a thread picks a stripe by its
 * id; more stripes are used when threads collide on the lock. A {@link FastThreadLocalThread} instead gets a
 * {@link Heap} of its own, needing no lock, for the size classes and for the buffers above them.
 * <p>
 * A buffer released by the thread that owns its chunk is returned to it directly; released by any other thread, its
 * slot goes on the chunk's lock-free free list and a note is left for the owner to apply on its next slow path.
 */
@UnstableApi
final class AdaptivePoolingAllocator {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AdaptivePoolingAllocator.class);
    private static final int LOW_MEM_THRESHOLD = 512 * 1024 * 1024;
    static final boolean IS_LOW_MEM = SystemPropertyUtil.getBoolean(
            "io.netty.allocator.lowMemory",
            Runtime.getRuntime().maxMemory() <= LOW_MEM_THRESHOLD);

    private static final boolean DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM = SystemPropertyUtil.getBoolean(
            "io.netty.allocator.disableThreadLocalMagazinesOnLowMemory", true);

    private static final AtomicIntegerFieldUpdater<AdaptivePoolingAllocator> STRIPE_SCAN_LENGTH =
            AtomicIntegerFieldUpdater.newUpdater(AdaptivePoolingAllocator.class, "stripeScanLength");
    private static final int EXPANSION_ATTEMPTS = 3;
    private static final int MAX_STRIPES = IS_LOW_MEM ? 1 :
            MathUtil.safeFindNextPositivePowerOfTwo(NettyRuntime.availableProcessors() * 2);
    private static final int INITIAL_MAGAZINES = 1;
    /** Up to this size a buffer goes to its heap directly; above it, through {@link #allocateFallback}. */
    private static final int MAX_POOLED_BUF_SIZE = IS_LOW_MEM ? 256 * 1024 : 1024 * 1024;

    /**
     * {@code io.netty.allocator.chunkPurgeInterval}: how often, in chunks' worth of allocations, a size-class
     * magazine applies the notes left by other threads and counts its allocations for the heap's decay clock. Read
     * from {@code io.netty.allocator.chunkPurgePollsThreadLocal} when only that, its former name, is set.
     */
    static final long CHUNK_PURGE_INTERVAL = Math.max(1, SystemPropertyUtil.getLong(
            "io.netty.allocator.chunkPurgeInterval",
            SystemPropertyUtil.getLong("io.netty.allocator.chunkPurgePollsThreadLocal", 4L)));

    /**
     * {@code io.netty.allocator.magazineBufferQueueCapacity}: how many {@link AdaptiveByteBuf} instances a stripe
     * keeps for reuse. Pools the buffer objects only, not their memory, to save garbage.
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

    static {
        if (MAGAZINE_BUFFER_QUEUE_CAPACITY < 2) {
            throw new IllegalArgumentException("MAGAZINE_BUFFER_QUEUE_CAPACITY: " + MAGAZINE_BUFFER_QUEUE_CAPACITY
                    + " (expected: >= " + 2 + ')');
        }
    }

    /** The size classes this allocator serves, and the chunk geometry they take: see {@link SizeClassTable}. */
    private static final int SIZE_CLASSES_COUNT = SizeClassTable.SIZES_COUNT;

    /** Largest size served by a size class in low-memory mode: the size classes above it are not pooled there. */
    private static final int LOW_MEM_MAX_SIZE_CLASS = 16896;

    /** Number of size classes that are pooled: all of them, except in low-memory mode. */
    private static final int POOLED_SIZE_CLASSES_COUNT =
            IS_LOW_MEM ? sizeClassIndexOf(LOW_MEM_MAX_SIZE_CLASS) + 1 : SIZE_CLASSES_COUNT;

    private final MemorySource memory;
    private final LongAdder usedMemory = new LongAdder();
    private final Heap[] stripedHeaps;
    private volatile int stripeScanLength;

    private final FastThreadLocal<Heap> threadLocalHeap;

    /** The size classes and the chunk geometry they take under {@link #pageStore}'s config. */
    final SizeClassTable table;
    final PageStore pageStore;
    /**
     * Outside low-memory mode: the largest buffer that is a span of the page store's slices, a whole block; above
     * it a buffer takes consecutive whole blocks ({@link PageStore#claimBlocks}). 0 in low-memory mode.
     */
    private final int largeSpanLimit;

    /**
     * @param mmap where {@code mmap} regions come from, or {@code null} for {@code memory}'s one-block ones (see
     *             {@link PageStore#PageStore})
     */
    AdaptivePoolingAllocator(MemorySource memory, boolean useCacheForNonEventLoopThreads,
                             MmapRegionSource mmap, PageStoreConfig config) {
        table = new SizeClassTable(config);
        checkSizeClassSpansFit(config, table);
        pageStore = new PageStore(this, config, memory, mmap);
        largeSpanLimit = IS_LOW_MEM ? 0 : config.segmentSize;
        this.memory = ObjectUtil.checkNotNull(memory, "memory");
        stripedHeaps = new Heap[MAX_STRIPES];
        for (int i = 0; i < MAX_STRIPES; i++) {
            stripedHeaps[i] = new Heap(this, new StampedLock(), null);
        }
        stripeScanLength = INITIAL_MAGAZINES;

        boolean disableThreadLocalGroups = IS_LOW_MEM && DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM;
        threadLocalHeap = disableThreadLocalGroups ? null : new FastThreadLocal<Heap>() {
            @Override
            protected Heap initialValue() {
                if (useCacheForNonEventLoopThreads || ThreadExecutorMap.currentExecutor() != null) {
                    return new Heap(AdaptivePoolingAllocator.this, null, Thread.currentThread());
                }
                return null;
            }

            @Override
            protected void onRemoval(final Heap heap) throws Exception {
                if (heap != null) {
                    heap.close();
                }
            }
        };
    }

    /**
     * The largest class must fit a block, and no chunk may hold more than {@link SizeClassedChunk#MAX_SLOTS}: the
     * page kinds hold the largest class by construction once it fits the block (see {@link SizeClassTable#pageKinds}).
     */
    private static void checkSizeClassSpansFit(PageStoreConfig config, SizeClassTable table) {
        int largest = SizeClassTable.SIZES[SIZE_CLASSES_COUNT - 1];
        if (largest > config.segmentSize) {
            throw new IllegalArgumentException("segmentSize " + config.segmentSize + " cannot hold a buffer of "
                    + largest + " (slices of " + config.sliceSize + ')');
        }
        for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
            int slotsPerChunk = table.slots[i];
            if (slotsPerChunk > SizeClassedChunk.MAX_SLOTS) {
                throw new IllegalArgumentException("chunks of " + SizeClassTable.SIZES[i] + "-byte buffers would hold "
                        + slotsPerChunk + ", more than " + SizeClassedChunk.MAX_SLOTS + " (slices of "
                        + config.sliceSize + ')');
            }
        }
    }

    ByteBuf allocate(int size, int maxCapacity) {
        return allocate(size, maxCapacity, Thread.currentThread(), null);
    }

    private AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
        AdaptiveByteBuf allocated = null;
        if (size <= MAX_POOLED_BUF_SIZE) {
            final int index = sizeClassIndexOf(size);
            // Above the size classes: a thread with its own heap never takes a stripe lock for these either.
            if (index < POOLED_SIZE_CLASSES_COUNT || !IS_LOW_MEM) {
                Heap heap = null;
                if (!IS_LOW_MEM && FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
                    heap = threadLocalHeap.get();
                }
                allocated = heap != null ? allocateOwned(heap, index, size, maxCapacity, buf)
                        : allocateShared(index, size, maxCapacity, currentThread, buf);
            }
        }
        if (allocated == null) {
            allocated = allocateFallback(size, maxCapacity, buf);
        }
        return allocated;
    }

    /** The owner thread's heap, no lock: the same size-class/large split a stripe makes under its lock. */
    private static AdaptiveByteBuf allocateOwned(Heap heap, int sizeClassIndex, int size, int maxCapacity,
                                                  AdaptiveByteBuf buf) {
        if (sizeClassIndex < SIZE_CLASSES_COUNT) {
            AdaptiveByteBuf result = heap.allocateSizeClass(sizeClassIndex, size, maxCapacity, buf);
            assert result != null : "owner allocation must always succeed";
            return result;
        }
        return heap.allocateLarge(size, maxCapacity, buf, buf != null);
    }

    /**
     * A stripe's size-class slot or span: takes the stripe's lock itself, calling the same {@link Heap} method the
     * owner thread calls without one. A stripe whose lock is busy, or whose allocation fails, is skipped.
     */
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
                Heap stripe = stripedHeaps[(start + i) & mask];
                long stamp = stripe.lock.tryWriteLock();
                if (stamp == 0) {
                    continue;
                }
                try {
                    AdaptiveByteBuf result = sizeClassIndex < SIZE_CLASSES_COUNT
                            ? stripe.allocateSizeClass(sizeClassIndex, size, maxCapacity, buf)
                            : stripe.allocateLarge(size, maxCapacity, buf, reallocate);
                    if (result != null) {
                        return result;
                    }
                } finally {
                    stripe.lock.unlockWrite(stamp);
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

    /** The size class of {@code size}: see {@link SizeClassTable#indexOf}. */
    static int sizeClassIndexOf(int size) {
        return SizeClassTable.indexOf(size);
    }

    private AdaptiveByteBuf allocateFallback(int size, int maxCapacity, AdaptiveByteBuf buf) {
        if (size > MAX_POOLED_BUF_SIZE && size <= largeSpanLimit) {
            // Above the pooled sizes, up to a block: a span of the store's slices, as smaller large buffers.
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
        // Above a block: consecutive whole blocks of the store if a region holds them; else an allocation of its own.
        OneShotChunk chunk = largeSpanLimit != 0 && size > largeSpanLimit ? newStoreOneShot(size) : null;
        if (chunk == null) {
            chunk = new OneShotChunk(memory.allocate(size, maxCapacity), this, null, 0, 0);
            memoryCommitted(chunk.delegate._memoryAddress(), chunk.capacity, chunk.delegate.isDirect(), false, false);
        }
        boolean initialized = false;
        try {
            buf.init(chunk.delegate, chunk, 0, 0, chunk.base, size, size, maxCapacity);
            initialized = true;
        } finally {
            if (!initialized) {
                chunk.releaseSlot(0, size);
            }
        }
        return buf;
    }

    /** A span of the calling thread's heap, or of a stripe; {@code null} when every stripe is busy. */
    private AdaptiveByteBuf allocateLargeSpan(int size, int maxCapacity, AdaptiveByteBuf buf) {
        Thread current = Thread.currentThread();
        if (FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
            Heap heap = threadLocalHeap.get();
            if (heap != null) {
                return heap.allocateLarge(size, maxCapacity, buf, buf != null);
            }
        }
        return allocateShared(SIZE_CLASSES_COUNT, size, maxCapacity, current, buf);
    }

    /** A one-shot chunk of consecutive whole blocks of the page store; {@code null} when no region can hold them. */
    private OneShotChunk newStoreOneShot(int size) {
        PageStore store = pageStore;
        // The purge runs from the heaps' allocation paths; these buffers pass through no heap, so they run it too.
        store.purgeIfDue(System.nanoTime());
        int segmentSize = store.config.segmentSize;
        int slots = (int) ((size + (long) segmentSize - 1) / segmentSize);
        long run = store.claimBlocks(slots);
        if (run < 0) {
            return null;
        }
        Region region = store.region(run);
        int start = store.firstBlock(run);
        boolean made = false;
        try {
            OneShotChunk chunk = new OneShotChunk(region.buffer, this, region, start, slots);
            made = true;
            return chunk;
        } finally {
            if (!made) {
                store.releaseBlocks(region, start, slots);
            }
        }
    }

    /** A buffer object of no heap: not kept for reuse. */
    private static AdaptiveByteBuf newFallbackBuffer() {
        return new AdaptiveByteBuf(null);
    }

    /**
     * Allocate into the given buffer. Used by {@link AdaptiveByteBuf#capacity(int)}.
     */
    void reallocate(int size, int maxCapacity, AdaptiveByteBuf into) {
        AdaptiveByteBuf result = allocate(size, maxCapacity, Thread.currentThread(), into);
        assert result == into : "Re-allocation created separate buffer instance";
    }

    /**
     * The bytes this allocator holds: the {@link PageStore}'s committed slices or whole regions, and the one-shot
     * chunks of their own allocation, from {@link #memoryCommitted} to {@link #memoryReleased}. Size-class chunks
     * and spans carved out of a region fire no such event, since their memory never leaves the allocator.
     */
    long usedMemory() {
        return usedMemory.sum();
    }

    /** {@code bytes} of memory at {@code address} were just committed: a one-shot allocation, or shared slices. */
    void memoryCommitted(long address, int bytes, boolean direct, boolean pooled, boolean threadLocal) {
        usedMemory.add(bytes);
        MemoryEvents.allocated(address, bytes, direct, pooled, threadLocal);
    }

    /** As {@link #memoryCommitted}, for memory about to be released back to the OS or the {@link PageStore}. */
    void memoryReleased(long address, int bytes, boolean direct, boolean pooled) {
        usedMemory.add(-bytes);
        MemoryEvents.freed(address, bytes, direct, pooled);
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

    /** Frees the calling thread's heap, the striped heaps and the store's regions. Only with no live buffer. */
    void close() {
        if (threadLocalHeap != null) {
            threadLocalHeap.remove();
        }
        free();
    }

    private void free() {
        for (Heap stripe : stripedHeaps) {
            stripe.close();
        }
        pageStore.close();
    }

    /**
     * One thread's heap, or one stripe shared by the threads without one: a magazine per size class in use, its
     * chunks, and the buffer objects. It owns chunks and spans, never the blocks they are in: those are the
     * {@link PageStore}'s, shared by every heap. {@code lock} is {@code null} for a thread-local heap
     * ({@code owner} is then non-null) and a {@link StampedLock} for a stripe. The owner thread calls
     * {@link #allocateSizeClass} and {@link #allocateLarge} directly; the router
     * ({@link AdaptivePoolingAllocator#allocateShared}) takes the stripe's {@code tryWriteLock} itself. Every
     * release path reads {@link #lock} and takes it the same way, if it is not {@code null}.
     */
    static final class Heap {
        /** At most one decay per this interval: 10 s. */
        static final long DECAY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
        /** How many allocations of the heap between two looks at the clock. */
        private static final long DECAY_MIN_ALLOCATIONS = 10000;
        /** An allocation counts toward the decay clock as its size in units of the smallest size class. */
        private static final int COUNT_SHIFT = 5;
        /** Steps of one cache line a span's buffer may start into the span: see {@link #allocateSpan}. */
        private static final int MAX_SPAN_COLOURS = 64;

        final StampedLock lock;
        final Thread owner;
        final AdaptivePoolingAllocator allocator;
        final PageStore store;
        private final int segmentSlices;

        private SizeClassMagazine[] magazines;
        /** The notes left for this heap's size classes: see {@link PendingChunks}. */
        final PendingChunks notes = new PendingChunks();
        /** The buffer objects this heap's buffers are made of, kept for reuse. */
        final BufferPool pool;
        /** Round robin over the colours of the spans; single writer, as the heap's owner or lock holder. */
        private int nextSpanColour;
        private long allocationsSinceCheck;
        private long lastDecayNanos = System.nanoTime();

        Heap(AdaptivePoolingAllocator allocator, StampedLock lock, Thread owner) {
            this.allocator = allocator;
            this.store = allocator.pageStore;
            this.lock = lock;
            this.owner = owner;
            segmentSlices = store.config.slicesPerSegment();
            pool = new BufferPool(owner, MAGAZINE_BUFFER_QUEUE_CAPACITY);
        }

        /** The magazine of {@code sizeClassIndex}, made on first use. */
        private SizeClassMagazine magazine(int sizeClassIndex) {
            SizeClassMagazine[] mags = magazines;
            SizeClassMagazine mag;
            if (mags == null || (mag = mags[sizeClassIndex]) == null) {
                mag = newMagazine(sizeClassIndex);
            }
            return mag;
        }

        // Out of line: the allocation fast path must not carry the magazine's construction.
        private SizeClassMagazine newMagazine(int sizeClassIndex) {
            SizeClassMagazine[] mags = magazines;
            if (mags == null) {
                mags = new SizeClassMagazine[SIZE_CLASSES_COUNT];
                magazines = mags;
            }
            SizeClassMagazine mag = new SizeClassMagazine(this, sizeClassIndex);
            mags[sizeClassIndex] = mag;
            return mag;
        }

        /**
         * The size-class slot in {@code sizeClassIndex}'s magazine. The owner thread always succeeds; the router,
         * under the stripe's lock, releases a fresh {@code buf} on failure (unless reallocating) and returns
         * {@code null} so its scan can move to another stripe.
         */
        AdaptiveByteBuf allocateSizeClass(int sizeClassIndex, int size, int maxCapacity, AdaptiveByteBuf buf) {
            SizeClassMagazine mag = magazine(sizeClassIndex);
            boolean reallocate = buf != null;
            if (!reallocate) {
                buf = pool.take();
            }
            if (mag.allocate(size, maxCapacity, buf)) {
                mag.tick();
                if (reallocate) {
                    // Its slot is this heap's now, so is the object: freed in the same owner decision as the slot.
                    buf.pool = pool;
                }
                return buf;
            }
            if (!reallocate) {
                buf.release();
            }
            return null;
        }

        /**
         * Above the size classes, up to a block: a span of the store's slices ({@link PageStore#claimSlices}), as
         * many as the buffer rounds up to. No chunk object, nothing kept for reuse: a release, from any thread,
         * gives the span back to the store at once (see {@link SharedSpanChunk}).
         */
        AdaptiveByteBuf allocateLarge(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
            if (buf == null) {
                buf = pool.take();
            } else {
                buf.pool = pool;
            }
            boolean allocated = false;
            try {
                allocateSpan(size, maxCapacity, buf);
                allocated = true;
                return buf;
            } finally {
                if (!allocated && !reallocate) {
                    buf.release();
                }
            }
        }

        private void allocateSpan(int size, int maxCapacity, AdaptiveByteBuf buf) {
            int shift = store.config.sliceShift;
            int slices = (int) ((size + (1L << shift) - 1) >>> shift);
            if (slices > segmentSlices) {
                // Slices smaller than the buffer (a small configured segment size): a buffer of its own.
                allocator.allocateOneShot(size, maxCapacity, buf);
                return;
            }
            long run = store.claimSlices(slices, owner != null);
            Segment segment = store.block(run);
            int start = store.start(run);
            // Colour, as the size classes' chunks (see colourOffset): the buffer starts a round-robin number of
            // cache lines into its span, out of the tail the span leaves unused past the buffer, so it costs no
            // memory. A release rounds its capacity up to whole slices again.
            int room = (int) (((long) slices << shift) - size);
            int colour = SizeClassTable.colourOffset(nextSpanColour++, room, MAX_SPAN_COLOURS);
            // A block has one chunk object for the spans of every thread-local heap, and one for every stripe's.
            Chunk chunk = owner != null ? segment.threadLocalSpans : segment.sharedSpans;
            boolean initialized = false;
            try {
                buf.init(segment.buffer, chunk, 0, 0, segment.base + (start << shift) + colour, size,
                        (slices << shift) - colour, maxCapacity);
                initialized = true;
            } finally {
                if (!initialized) {
                    segment.releaseRun(start, slices, System.nanoTime());
                }
            }
            countAllocations(size >>> COUNT_SHIFT);
        }

        /** The heap made {@code allocations} more allocations: a size class's purge tick, or a span. */
        void countAllocations(long allocations) {
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
                releaseIdle(now);
            } else {
                store.purgeIfDue(now);
            }
        }

        /**
         * Apply every queued note, each by the magazine of its chunk. Caller holds the stripe lock, or is the owner
         * thread.
         */
        void applyNotes() {
            Chunk cur = notes.takeAll();
            while (cur != null) {
                // Re-arm BEFORE processing: see PendingChunks#rearm.
                Chunk next = PendingChunks.rearm(cur);
                SizeClassedChunk chunk = (SizeClassedChunk) cur;
                chunk.magazine.slotReturned(chunk);
                cur = next;
            }
        }

        /** The idle size classes give up their chunks: see {@link SizeClassMagazine#dropIfIdle}. */
        void releaseIdle(long now) {
            lastDecayNanos = now;
            allocationsSinceCheck = 0;
            SizeClassMagazine[] mags = magazines;
            if (mags != null) {
                for (SizeClassMagazine mag : mags) {
                    if (mag != null) {
                        mag.dropIfIdle();
                    }
                }
            }
            store.purgeIfDue(now);
        }

        /** The heap is gone: every magazine's chunks give their spans back, or are abandoned. */
        void close() {
            final StampedLock l = lock;
            long stamp = l != null ? l.writeLock() : 0;
            try {
                if (magazines != null) {
                    for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
                        SizeClassMagazine mag = magazines[i];
                        if (mag != null) {
                            mag.close();
                            magazines[i] = null;
                        }
                    }
                }
                // Nobody drains the notes any more: see SizeClassMagazine#close.
                notes.clear();
            } finally {
                if (l != null) {
                    l.unlockWrite(stamp);
                }
            }
        }
    }

    /**
     * An intrusive doubly linked list of chunks: the links live on the chunk, so removing any chunk is O(1), and so
     * is checking its membership ({@code chunk.queue}). Not concurrent: the owning magazine holds the stripe lock,
     * or is the only thread that touches it.
     */
    static final class ChunkQueue {
        Chunk head;
        private Chunk tail;
        int size;

        /** Remove and return the head, or {@code null} when empty. */
        Chunk pollFront() {
            Chunk chunk = head;
            if (chunk != null) {
                remove(chunk);
            }
            return chunk;
        }

        void pushFront(Chunk chunk) {
            Chunk head = this.head;
            chunk.prevInQueue = null;
            chunk.nextInQueue = head;
            if (head != null) {
                head.prevInQueue = chunk;
            } else {
                tail = chunk;
            }
            this.head = chunk;
            chunk.queue = this;
            size++;
        }

        void pushBack(Chunk chunk) {
            Chunk tail = this.tail;
            chunk.nextInQueue = null;
            chunk.prevInQueue = tail;
            if (tail != null) {
                tail.nextInQueue = chunk;
            } else {
                head = chunk;
            }
            this.tail = chunk;
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
            } else {
                tail = prev;
            }
            chunk.prevInQueue = null;
            chunk.nextInQueue = null;
            chunk.queue = null;
            size--;
        }
    }

    /**
     * Chunks that a releasing thread asked their owner to look at, because it freed memory in a chunk whose queues
     * it may not touch. One per heap, shared by every size class. A lock-free (Treiber) stack that any thread
     * pushes to and the heap's owner takes whole; a chunk's {@code pendingNext} is both its link and the claim that
     * it is queued, so a push on an already-queued chunk costs one volatile read.
     */
    static final class PendingChunks {
        private static final AtomicReferenceFieldUpdater<PendingChunks, Chunk> HEAD =
                AtomicReferenceFieldUpdater.newUpdater(PendingChunks.class, Chunk.class, "head");
        private static final AtomicReferenceFieldUpdater<Chunk, Chunk> NEXT =
                AtomicReferenceFieldUpdater.newUpdater(Chunk.class, Chunk.class, "pendingNext");
        /** Ends the stack, so a {@code null} link keeps its meaning of "not queued". Never a usable chunk. */
        private static final Chunk END = new Chunk(null, null, false, 0) {
            @Override
            void releaseSlot(int offset, int size) {
                throw new IllegalStateException();
            }
        };

        private volatile Chunk head;

        /** Queue {@code chunk}, unless it is queued already. Any thread, no lock. */
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

        /** Take every queued chunk: the first one, whose successors {@link #rearm} returns, or {@code null}. Owner
         *  only; cheap when nothing is queued. */
        Chunk takeAll() {
            if (head == null) {
                return null;
            }
            return HEAD.getAndSet(this, null);
        }

        /**
         * Unlink {@code chunk}, taken by {@link #takeAll}, and make it queueable again; returns the next taken
         * chunk, or {@code null}. Call it BEFORE processing the chunk, so a return landing mid-process can queue it
         * again instead of stranding the note. A full volatile store, not a lazySet: the store half of a Dekker
         * pair with the releaser's CAS-then-read of {@code pendingNext}, or both sides could miss each other.
         */
        static Chunk rearm(Chunk chunk) {
            Chunk next = chunk.pendingNext;
            NEXT.set(chunk, null);
            return next == END ? null : next;
        }

        /** Drop every queued chunk, for a heap being freed. */
        private void clear() {
            HEAD.lazySet(this, null);
        }
    }

    private static int threadIndex(Thread t) {
        int id = (int) t.getId();
        return id ^ (id >>> 16);
    }

    /**
     * The buffer objects one heap keeps for reuse: a stack its owner (the owner thread, or a stripe's lock holder)
     * pushes and pops, and a queue any other thread returns into, which only the owner takes from.
     */
    static final class BufferPool {
        /** The heap's owner thread; {@code null} for a stripe, whose owner is its lock holder. */
        private final Thread owner;
        private final AdaptiveByteBuf[] stack;
        private int top;
        private final Queue<AdaptiveByteBuf> returned;

        BufferPool(Thread owner, int capacity) {
            this.owner = owner;
            stack = new AdaptiveByteBuf[capacity];
            returned = PlatformDependent.newFixedMpscQueue(capacity);
        }

        /** Owner: a buffer object, reset and ready. */
        AdaptiveByteBuf take() {
            AdaptiveByteBuf buf;
            int t = top;
            if (t != 0) {
                top = --t;
                buf = stack[t];
                stack[t] = null;
            } else {
                buf = takeReturnedOrNew();
            }
            buf.resetRefCnt();
            buf.discardMarks();
            return buf;
        }

        // Out of line: once warm, the stack serves.
        private AdaptiveByteBuf takeReturnedOrNew() {
            AdaptiveByteBuf buf = returned.poll();
            return buf != null ? buf : new AdaptiveByteBuf(this);
        }

        /** Owner: keeps {@code buf}, if any and if there is room. */
        void keep(AdaptiveByteBuf buf) {
            int t = top;
            if (buf != null && t < stack.length) {
                stack[t] = buf;
                top = t + 1;
            }
        }

        /** Any other thread: {@code buf}, if any, goes to the owner's queue, if there is room. */
        void returnRemotely(AdaptiveByteBuf buf) {
            if (buf != null) {
                returned.offer(buf);
            }
        }

        /** Any thread, with no slot decision to share: the owner thread keeps {@code buf}, any other queues it. */
        void recycle(AdaptiveByteBuf buf) {
            if (Thread.currentThread() == owner) {
                keep(buf);
            } else {
                returnRemotely(buf);
            }
        }
    }

    /**
     * One size class of a heap: carves fixed-size slots out of {@link SizeClassedChunk}s, allocating from
     * {@link #current} and keeping the others on {@link #reusable} (a free slot), {@link #full} (none when filed)
     * or {@link #spare} (no span, see {@link SizeClassedChunk}'s state diagram). {@link #full} is an ownership
     * registry only, walked just by the bounded probe in {@link #allocateSlow}: a cross-thread return that cannot
     * synchronise leaves a note (see {@link PendingChunks}) instead of moving the chunk there and then.
     */
    static final class SizeClassMagazine {
        /** Bound on the last-resort probe of {@link #full}; see {@link #allocateSlow}. */
        private static final int MAX_FULL_PROBE = 8;
        /** The chunks a size class keeps queued before it gives one up that empties: see {@link #atFloor}. */
        static final int FLOOR = 4;

        final Heap heap;

        private final int slotSize;
        final int chunkSize;
        /** {@link #chunkSize} in slices: the span length this class claims and gives back, and its bin in the store. */
        final int slices;
        final int slots;
        /** The chunk size's tail its slots leave unused: the room {@code colourOffset} rotates a chunk's start in. */
        private final int room;
        private int nextColour;

        /** The chunk this magazine allocates from, or {@code null} once closed. */
        SizeClassedChunk current;
        final ChunkQueue reusable = new ChunkQueue();
        final ChunkQueue full = new ChunkQueue();
        final ChunkQueue spare = new ChunkQueue();
        /** Whether an empty chunk may sit on {@link #reusable}, kept because the class is at or below floor. */
        private boolean freeChunkKept;

        private final int tickThreshold;
        private int allocCount;
        /** Ticks so far; with {@link #allocCount} it tells whether the class allocated since the last decay. */
        private int ticks;
        private int ticksAtDecay;
        private int allocCountAtDecay;

        SizeClassMagazine(Heap heap, int sizeClassIndex) {
            this.heap = heap;
            SizeClassTable table = heap.allocator.table;
            slotSize = SizeClassTable.SIZES[sizeClassIndex];
            slices = table.chunkSlices[sizeClassIndex];
            chunkSize = slices * heap.store.config.sliceSize;
            slots = table.slots[sizeClassIndex];
            room = table.room[sizeClassIndex];
            tickThreshold = (int) Math.min(Integer.MAX_VALUE, CHUNK_PURGE_INTERVAL * (chunkSize / slotSize));
        }

        /**
         * Count one successful allocation and, when the budget is spent, apply the heap's notes and count it for
         * the heap's decay clock: a size class that keeps allocating from its current chunk takes no slow path, so
         * this is what applies notes and reads the clock for it. Call exactly once per successful {@link #allocate}.
         */
        void tick() {
            if (++allocCount >= tickThreshold) {
                allocCount = 0;
                ticks++;
                heap.applyNotes();
                heap.countAllocations(tickThreshold);
            }
        }

        /**
         * Called by {@link Heap#releaseIdle}: a size class that made no allocation since the previous decay gives
         * up its current chunk and every empty chunk it keeps, floor included; one unused through a whole interval
         * keeps nothing. Chunks with buffers out stay.
         */
        private void dropIfIdle() {
            int t = ticks;
            int allocs = allocCount;
            boolean idle = t == ticksAtDecay && allocs == allocCountAtDecay;
            ticksAtDecay = t;
            allocCountAtDecay = allocs;
            if (!idle) {
                return;
            }
            if (current != null && current.allFree()) {
                retireCurrent();
            }
            heap.applyNotes();
            returnFreeSpans(false);
        }

        boolean allocate(int size, int maxCapacity, AdaptiveByteBuf buf) {
            int startingCapacity = Math.min(slotSize, maxCapacity);
            SizeClassedChunk curr = current;
            if (curr != null) {
                boolean success = curr.takeSlot(buf, size, startingCapacity, maxCapacity);
                if (!success || curr.freeBytes() == 0) {
                    // Out of slots (or, defensively, takeSlot failed): a slot that comes back from another thread
                    // after this files it as reusable by its capacity, so a later probe can hand it back.
                    retireCurrent();
                }
                if (success) {
                    return true;
                }
            }
            return allocateSlow(size, maxCapacity, buf, startingCapacity);
        }

        /**
         * The current chunk (if any) had no room: take one from {@link #reusable}, probe {@link #full} for one that
         * regained a slot, or claim a fresh span. Whichever chunk ends up serving the allocation becomes
         * {@link #current}, on no list.
         */
        private boolean allocateSlow(int size, int maxCapacity, AdaptiveByteBuf buf, int startingCapacity) {
            assert current == null;
            heap.applyNotes();
            SizeClassedChunk curr = (SizeClassedChunk) reusable.pollFront();
            if (curr == null) {
                SizeClassedChunk probed = (SizeClassedChunk) full.head;
                for (int visited = 0; curr == null && probed != null && visited < MAX_FULL_PROBE; visited++) {
                    SizeClassedChunk next = (SizeClassedChunk) probed.nextInQueue;
                    if (probed.hasFreeSlot()) {
                        full.remove(probed);
                        curr = probed;
                    }
                    probed = next;
                }
            }
            if (curr == null) {
                // A span of the store's slices, served by a chunk object the magazine gave up earlier (spare) or a
                // new one, coloured round robin so that slot k of consecutive chunks does not share its offset in a
                // page (Bonwick, "The Slab Allocator", USENIX Summer 1994, section 4.3).
                PageStore store = heap.store;
                long run = store.claimSlices(slices, heap.owner != null);
                Segment segment = store.block(run);
                int start = store.start(run);
                try {
                    curr = (SizeClassedChunk) spare.pollFront();
                    if (curr == null) {
                        int colour = SizeClassTable.colourOffset(nextColour++, room, SizeClassTable.MAX_CHUNK_COLOURS);
                        curr = new SizeClassedChunk(this, colour);
                    }
                    curr.takeSpan(segment, start);
                } catch (Throwable t) {
                    segment.releaseRun(start, slices, System.nanoTime());
                    throw t;
                }
            }

            current = curr;
            boolean success;
            try {
                int freeBytes = curr.freeBytes();
                assert freeBytes >= size : "the chunk handed out has no free slot";
                success = curr.takeSlot(buf, size, startingCapacity, maxCapacity);
                if (freeBytes > startingCapacity) {
                    curr = null;
                }
            } finally {
                if (curr != null) {
                    // Release in a finally block so even if takeSlot(...) would throw we would still correctly
                    // retire the current chunk before null it out.
                    retireCurrent();
                }
            }
            return success;
        }

        /**
         * The current chunk is out of slots, or the magazine is stopping: file it by capacity, exactly as a
         * release files any chunk ({@link #slotReturned}). Above the floor no empty chunk stays.
         */
        private void retireCurrent() {
            SizeClassedChunk chunk = current;
            current = null;
            boolean hasCapacity = chunk.hasFreeSlot();
            if (hasCapacity) {
                reusable.pushFront(chunk);
            } else {
                full.pushFront(chunk);
            }
            if (atFloor()) {
                if (hasCapacity && chunk.allFree()) {
                    freeChunkKept = true;
                }
            } else if (freeChunkKept) {
                returnFreeSpans(true);
            } else if (hasCapacity && chunk.allFree()) {
                returnSpan(chunk);
            }
        }

        /**
         * A release or an applied note gave {@code chunk} a slot: FULL → REUSABLE, or its span back if all free
         * above the floor. Caller holds the stripe lock or is the owner thread.
         */
        void slotReturned(SizeClassedChunk chunk) {
            ChunkQueue queue = chunk.queue;
            if (queue == full) {
                // A note may be stale; a return by the owner or under the lock has just pushed the slot.
                if (!chunk.hasFreeSlot()) {
                    return;
                }
                full.remove(chunk);
                reusable.pushBack(chunk);
            } else if (queue != reusable) {
                // Spare, or on no queue: not ours to move, or the current chunk, filed by capacity when it retires.
                return;
            }
            if (chunk.allFree()) {
                if (atFloor()) {
                    freeChunkKept = true;
                } else {
                    returnSpan(chunk);
                }
            }
        }

        /** {@code chunk}, empty and on {@link #reusable}, gives its span back to the store and waits spare. */
        private void returnSpan(SizeClassedChunk chunk) {
            reusable.remove(chunk);
            chunk.releaseSpan();
            spare.pushFront(chunk);
        }

        /**
         * Every empty chunk on {@link #reusable} gives its span back: {@code keepFloor} stops once at or below the
         * floor; {@code false} gives up everything (a class idle through a whole decay interval).
         */
        void returnFreeSpans(boolean keepFloor) {
            SizeClassedChunk cur = (SizeClassedChunk) reusable.head;
            while (cur != null) {
                if (keepFloor && atFloor()) {
                    return;
                }
                SizeClassedChunk next = (SizeClassedChunk) cur.nextInQueue;
                if (cur.allFree()) {
                    returnSpan(cur);
                }
                cur = next;
            }
            freeChunkKept = false;
        }

        /**
         * {@code true} when {@link #reusable} and {@link #full} hold at most {@link #FLOOR} chunks between them, so
         * a size class that empties and fills again around them does not give one up and make it again each time.
         */
        private boolean atFloor() {
            return full.size + reusable.size <= FLOOR;
        }

        /**
         * The heap is gone: the current chunk and every queued one give their spans back, or are abandoned to the
         * store until their buffers are back (see {@link SizeClassedChunk#releaseOrAbandon}).
         */
        private void close() {
            if (current != null) {
                retireCurrent();
            }
            releaseAll(full);
            releaseAll(reusable);
            freeChunkKept = false;
        }

        private static void releaseAll(ChunkQueue queue) {
            Chunk cur;
            while ((cur = queue.pollFront()) != null) {
                ((SizeClassedChunk) cur).releaseOrAbandon();
            }
        }
    }

    /**
     * The chunk every span of a buffer above the size classes in one block belongs to, whichever heap claimed it:
     * one object per block (two: one for the thread-local heaps' spans, one for the stripes'), so such a buffer
     * costs no object of its own. It has no state; a release, from any thread, gives the span back to the block by
     * CAS at once. Nothing is queued, nothing is owned.
     */
    static final class SharedSpanChunk extends Chunk {
        final Segment block;
        private final boolean threadLocal;

        /** @param threadLocal whether the heaps whose spans this chunk holds are thread-local ones, for JFR */
        SharedSpanChunk(Segment block, PageStore store, boolean threadLocal) {
            super(block.buffer, store.allocator, true, store.config.segmentSize);
            this.block = block;
            this.threadLocal = threadLocal;
        }

        /** Any thread: the span of {@code length} bytes at {@code offset}, colour included, goes back to the block. */
        @Override
        void releaseSlot(int offset, int length) {
            int shift = block.region.store.config.sliceShift;
            int relative = offset - block.base;
            block.releaseRun(relative >>> shift, length + (1 << shift) - 1 >>> shift, System.nanoTime());
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

    /** What every chunk has in common: the buffer it carves allocations out of and the allocator that owns it. */
    abstract static class Chunk {
        /** The {@link ChunkQueue} this chunk is filed on, or {@code null}: the current chunk, or one just polled. */
        ChunkQueue queue;
        // Links of the ChunkQueue this chunk is on, if any.
        Chunk prevInQueue;
        Chunk nextInQueue;
        /**
         * Link in its magazine's {@link PendingChunks}; {@code null} means not queued. This field <em>is</em> the
         * dedup claim: whoever moves it off {@code null} owns the push, so no separate flag is needed.
         */
        volatile Chunk pendingNext;
        /** Final but for a size-class chunk, which has one per incarnation (see {@code SizeClassedChunk#takeSpan}). */
        protected AbstractByteBuf delegate;
        // We need the top-level allocator so ByteBuf.capacity(int) can call reallocate()
        final AdaptivePoolingAllocator allocator;
        final int capacity;
        /** Whether this chunk serves many buffers, or is a one-shot chunk for a single one: for the JFR events. */
        final boolean pooled;

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
        abstract void releaseSlot(int startIndex, int size);

        /** {@link #releaseSlot}, and {@code buf} goes back to its pool. */
        void release(int startIndex, int size, AdaptiveByteBuf buf) {
            releaseSlot(startIndex, size);
            BufferPool pool = buf.pool;
            if (pool != null) {
                pool.recycle(buf);
            }
        }

        /**
         * Whether this chunk is attached to a magazine of a thread-local heap right now, for the JFR events.
         */
        boolean inThreadLocalMagazine() {
            return false;
        }
    }

    /**
     * A chunk cut into fixed slots of one size class, a span of its block from slice {@link #spanStart}.
     * <p>
     * <b>Free slots</b> are linked by index through {@link #next}, one entry per slot, never through the slots'
     * own memory: see {@link #next}. Slot {@code i} is at offset {@code base + i * slotSize} of the block. The chunk
     * keeps
     * <ul>
     *   <li>{@link #localHead}: the first slot released by the owner thread, or by a thread holding the stripe lock.
     *       The last slot released is the first handed out again;</li>
     *   <li>{@link #bump}: the first slot never handed out. These are taken in order and need no link, so a new
     *       chunk fills nothing, and {@link #next} is never cleared;</li>
     *   <li>{@link #localFree}: how many slots those two account for;</li>
     *   <li>{@link #remoteFree}: the slots released by any other thread, as one {@code long} holding their count
     *       above the index of the first. A releaser writes its link and then publishes the new first and the
     *       count with one CAS; the owner takes the whole list, and its count, with one {@code getAndSet} when
     *       {@link #localHead} and {@link #bump} have run out. Only pushes race with each other and the list is only
     *       ever taken whole, so there is no ABA.</li>
     * </ul>
     * Every question about capacity is arithmetic on the counts: nothing walks a list.
     * <p>
     * <b>Incarnations.</b> A chunk object outlives its span, served again by {@link #takeSpan} with the same colour.
     * <p>
     * <b>One owner.</b> Only the owner thread, the stripe lock holder, or, once abandoned, the purger touches a
     * field other than {@link #remoteFree} and {@code pendingNext}, which any releaser may push onto or note instead.
     * <p>
     * <b>States.</b> The queue this chunk is on is the only truth; {@code queue == null} is CURRENT (or, with
     * {@code ownerThread == null} and no span, ABANDONED). NOTED ({@code pendingNext != null}) is orthogonal, owned
     * by {@link PendingChunks} alone.
     * <pre>
     *             claimChunk (takeSpan)        takeSlot() fails: retireCurrent()
     *   SPARE ───────────────────────► CURRENT ──────────────────────────► FULL ──┐
     *  (no span)  ▲                       ▲   ▲                            │      │ slotReturned(): a release
     *             │ returnSpan(): all     │   │ pollFront()                │      │ (owner / applied note) gave
     *             │ free above the floor  │   │ probe (hasFreeSlot)        ▼      │ a slot back
     *             └────────────────────── │ ──┴───────────────────────── REUSABLE ◄┘
     *                                     │ retireCurrent() with a free slot   │
     *   heap dies (magazine.close): CURRENT/REUSABLE/FULL ──┬─ allFree ─► span back, object dropped
     *                                                       └─ else ─► ABANDONED (store.abandon; ownerThread = null)
     *   ABANDONED ── purger: returnSpanIfAllFree ─► span back, object dropped
     * </pre>
     */
    static class SizeClassedChunk extends Chunk {
        /** Ends a list in {@link #next}; fits a {@code short} like every slot index. */
        private static final int FREE_LIST_EMPTY = -1;
        /** The cost of {@link #next}'s {@code short} entries: no chunk has more slots than a {@code short} indexes. */
        static final int MAX_SLOTS = Short.MAX_VALUE + 1;
        /** No slot and a count of zero: see {@link #remoteFree}. */
        private static final long REMOTE_EMPTY = 0xFFFFFFFFL;
        private static final AtomicLongFieldUpdater<SizeClassedChunk> REMOTE_FREE =
                AtomicLongFieldUpdater.newUpdater(SizeClassedChunk.class, "remoteFree");
        private final int slots;
        private final int slotSize;
        /** How far into each span the slots start: 64-byte steps, one per chunk object. */
        private final int colour;
        /**
         * Per slot, the index of the next free slot on its list ({@link #localHead}'s or {@link #remoteFree}'s), or
         * {@link #FREE_LIST_EMPTY}; meaningful for free slots only, never cleared. A side array rather than a link
         * stored in each free slot's own memory (as mimalloc does): taking or freeing a slot then never reads or
         * writes the slot's memory, so a buffer written after its release cannot break a list, a free slot's memory
         * is not touched for the sake of the list, and the links survive the span being given back. One
         * {@code short} per slot, owned by the chunk object, which {@link #takeSpan} reuses span after span: nothing
         * is allocated per span. The cost is {@link #MAX_SLOTS}.
         */
        private final short[] next;
        /** {@link #indexReciprocal} of {@link #slotSize}: an offset becomes an index without a division. */
        private final long indexRecip;
        // Per incarnation, set by takeSpan.
        /** Offset of the first slot. */
        private int base;
        /** Index of the first slot released by the owner thread or under the stripe lock. */
        private int localHead;
        /** Index of the first slot never handed out; they are taken in order up to {@link #slots}. */
        private int bump;
        /** The slots reachable from {@link #localHead} plus those never handed out. */
        private int localFree;
        /** Slots released by other threads: their count in the high half, the first one's index in the low. */
        private volatile long remoteFree;
        /** {@code null} on a stripe, and once abandoned: see {@link #releaseOrAbandon}. */
        private Thread ownerThread;
        /** Link of the store's abandoned-chunk stack, written only there (see {@code PageStore#abandon}). */
        SizeClassedChunk nextAbandoned;
        /**
         * Snapshot behind {@link #freeBytes()}: bytes handed out since the last refresh from the free counts.
         * Slots returned since then are not subtracted, so {@code capacity - allocatedBytes} never counts a slot
         * that is not free.
         */
        private int allocatedBytes;

        final SizeClassMagazine magazine;
        /** The block this chunk is a span of, from slice {@link #spanStart}; {@code null} for the end marker. */
        Segment segment;
        int spanStart;

        /** A chunk object of {@code magazine}, with no span until {@link #takeSpan}. */
        SizeClassedChunk(SizeClassMagazine magazine, int colour) {
            super(null, magazine.heap.allocator, true, magazine.chunkSize - colour);
            slotSize = magazine.slotSize;
            slots = magazine.slots;
            assert slots <= MAX_SLOTS : slots;
            this.colour = colour;
            next = new short[slots];
            indexRecip = indexReciprocal(slotSize);
            ownerThread = magazine.heap.owner;
            this.magazine = magazine;
        }

        /**
         * {@code ceil(2^40 / slotSize)}: for {@code d = i * slotSize}, {@code d * indexReciprocal(slotSize) >>> 40}
         * is {@code i} plus {@code i * e / 2^40}, with {@code e < slotSize}, so exactly {@code i} while
         * {@code d < 2^40}, and the product stays below {@code 2^63} while {@code i < 2^23}.
         */
        static long indexReciprocal(int slotSize) {
            return ((1L << 40) + slotSize - 1) / slotSize;
        }

        /** The index of the slot {@code distance} bytes past the first one. */
        static int index(int distance, long indexRecip) {
            return (int) ((long) distance * indexRecip >>> 40);
        }

        private int index(int offset) {
            return index(offset - base, indexRecip);
        }

        private int offset(int index) {
            return base + index * slotSize;
        }

        /**
         * Owner: this object becomes the chunk of the span of {@code segment} from slice {@code spanStart}, every
         * slot free and none handed out. Nothing is filled: {@link #bump} hands out the slots in order, so the
         * links in {@link #next} are written only as slots are freed.
         */
        void takeSpan(Segment segment, int spanStart) {
            this.segment = segment;
            this.spanStart = spanStart;
            // The region's own buffer, as a large buffer's span reads it: no buffer object per chunk.
            delegate = segment.buffer;
            base = segment.base + spanStart * segment.sliceSize + colour;
            bump = 0;
            localHead = FREE_LIST_EMPTY;
            localFree = slots;
            allocatedBytes = 0;
            remoteFree = REMOTE_EMPTY;
        }

        /** True exactly when this chunk, read from {@link AdaptiveByteBuf#init}, has an owner thread. */
        @Override
        boolean inThreadLocalMagazine() {
            return ownerThread != null;
        }

        boolean takeSlot(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            final int startIndex = takeFreeOffset();
            if (startIndex == FREE_LIST_EMPTY) {
                return false;
            }
            allocatedBytes += slotSize;
            try {
                buf.init(delegate, this, 0, 0, startIndex, size, startingCapacity, maxCapacity);
            } catch (Throwable t) {
                allocatedBytes -= slotSize;
                rollbackSlot(startIndex);
                throw t;
            }
            return true;
        }

        private int takeFreeOffset() {
            int head = localHead;
            if (head >= 0) {
                localHead = next[head];
                localFree--;
                return offset(head);
            }
            int bump = this.bump;
            if (bump < slots) {
                this.bump = bump + 1;
                localFree--;
                return offset(bump);
            }
            return takeRemoteFree();
        }

        /** Take every slot other threads have released, and hand out the first. */
        private int takeRemoteFree() {
            if (remoteFree == REMOTE_EMPTY) {
                return FREE_LIST_EMPTY;
            }
            long taken = REMOTE_FREE.getAndSet(this, REMOTE_EMPTY);
            int head = (int) taken;
            localFree += remoteCount(taken) - 1;
            localHead = next[head];
            return offset(head);
        }

        private static int remoteCount(long remoteFree) {
            return (int) (remoteFree >>> 32);
        }

        private void pushLocalFree(int offset) {
            int index = index(offset);
            next[index] = (short) localHead;
            localHead = index;
            localFree++;
        }

        // Package-private for the tests that cut a release from another thread in two: this, then the note.
        void pushRemoteFree(int offset) {
            int index = index(offset);
            long current;
            long pushed;
            do {
                current = remoteFree;
                next[index] = (short) current;
                pushed = (long) remoteCount(current) + 1 << 32 | index;
            } while (!REMOTE_FREE.compareAndSet(this, current, pushed));
        }

        /**
         * Whether this chunk has a free slot, as the magazine files it (reusable or full) and probes it. Unlike
         * {@link #freeBytes()} it never refreshes the snapshot.
         */
        boolean hasFreeSlot() {
            int remaining = capacity - allocatedBytes;
            if (remaining > 0) {
                return true;
            }
            return localFree > 0 || remoteFree != REMOTE_EMPTY;
        }

        /** Every slot is back. */
        boolean allFree() {
            return localFree + remoteCount(remoteFree) == slots;
        }

        /**
         * The free bytes of this chunk, from the {@link #allocatedBytes} snapshot while it is above one slot, else
         * refreshed from the free counts.
         */
        int freeBytes() {
            int remaining = capacity - allocatedBytes;
            return remaining > slotSize ? remaining : updateRemainingCapacity(remaining);
        }

        private int updateRemainingCapacity(int snapshotted) {
            int freeSlots = remoteCount(remoteFree) + localFree;
            int updated = freeSlots * slotSize;
            if (updated != snapshotted) {
                allocatedBytes = capacity - updated;
            }
            return updated;
        }

        private void rollbackSlot(int startIndex) {
            if (ownerThread != null && Thread.currentThread() == ownerThread) {
                pushLocalFree(startIndex);
            } else {
                pushRemoteFree(startIndex);
            }
        }

        @Override
        void releaseSlot(int offset, int size) {
            release(offset, size, null);
        }

        /**
         * The slot, and the buffer object back to the heap's pool in the same owner decision; {@code buf} is
         * {@code null} when a reallocation gives the slot up and keeps the object.
         */
        @Override
        void release(int offset, int size, AdaptiveByteBuf buf) {
            Heap heap = magazine.heap;
            assert buf == null || buf.pool == heap.pool;
            if (ownerThread != null && Thread.currentThread() == ownerThread) {
                releasedByOwner(offset);
                heap.pool.keep(buf);
                return;
            }
            StampedLock lock = heap.lock;
            long stamp = lock == null ? 0 : lock.tryWriteLock();
            if (stamp == 0) {
                releasedRemotely(offset);
                heap.pool.returnRemotely(buf);
                return;
            }
            try {
                releasedByOwner(offset);
                heap.pool.keep(buf);
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        /** Owner side: the slot goes on the local list and a filed chunk is refiled. */
        private void releasedByOwner(int offset) {
            pushLocalFree(offset);
            // Neither a chunk out of the magazine's lists nor its current chunk is ever moved or evicted by a
            // slot return: the current chunk consumes its own returned slots.
            if (queue != null) {
                magazine.slotReturned(this);
            }
        }

        /** Any thread: the slot goes on the remote list and the owner gets a note. */
        private void releasedRemotely(int offset) {
            pushRemoteFree(offset);
            // The chunk just gained capacity but we could not take the lock to apply the resulting list
            // transition. Leave a note instead; the next drain applies it.
            magazine.heap.notes.push(this);
        }

        /** Owner, with every slot back: the span goes back to its block, by CAS. */
        void releaseSpan() {
            assert allFree();
            segment.releaseRun(spanStart, magazine.slices, System.nanoTime());
        }

        /**
         * Owner, as its heap dies: the span goes back now if every slot is back; else the chunk is abandoned to the
         * store, whose purger becomes its owner ({@link #returnSpanIfAllFree}) and every release takes the CAS path.
         */
        private void releaseOrAbandon() {
            if (allFree()) {
                releaseSpan();
                return;
            }
            ownerThread = null;
            allocator.pageStore.abandon(this);
        }

        /**
         * Purger, for an abandoned chunk: gives the span back if every slot is back, taking the dead stripe's lock
         * to look (a free slot's release still counts on it), and leaves it for the next pass if already taken.
         */
        boolean returnSpanIfAllFree() {
            StampedLock lock = magazine.heap.lock;
            long stamp = lock == null ? 0 : lock.tryWriteLock();
            if (stamp == 0 && lock != null) {
                return false;
            }
            try {
                if (!allFree()) {
                    return false;
                }
                releaseSpan();
                return true;
            } finally {
                if (stamp != 0) {
                    lock.unlockWrite(stamp);
                }
            }
        }
    }

    /**
     * One buffer that owns its memory, freed with it: consecutive whole blocks of a region for a buffer above a
     * block ({@link PageStore#claimBlocks}), else an allocation of its own from the {@link MemorySource}.
     */
    private static final class OneShotChunk extends Chunk {
        /** The region of the {@link #runSlots} blocks from slot {@link #runStart} this buffer holds, or null. */
        private final Region region;
        private final int runStart;
        private final int runSlots;
        /** Where those blocks start in {@code delegate}, the region's buffer; 0 for an allocation of its own. */
        final int base;

        OneShotChunk(AbstractByteBuf delegate, AdaptivePoolingAllocator allocator, Region region, int runStart,
                     int runSlots) {
            super(delegate, allocator, false,
                    region != null ? runSlots * region.store.config.segmentSize : delegate.capacity());
            this.region = region;
            this.runStart = runStart;
            this.runSlots = runSlots;
            base = region != null ? runStart * region.store.config.segmentSize : 0;
        }

        /** Any thread, once: the blocks go back to the store, or the allocation is freed. */
        @Override
        void releaseSlot(int startIndex, int size) {
            if (region != null) {
                allocator.pageStore.releaseBlocks(region, runStart, runSlots);
            } else {
                allocator.memoryReleased(delegate._memoryAddress(), capacity, delegate.isDirect(), false);
                delegate.release();
            }
        }

        @Override
        public String toString() {
            return "OneShotChunk[capacity: " + capacity + ']';
        }
    }
}
