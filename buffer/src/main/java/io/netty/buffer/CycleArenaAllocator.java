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

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ThreadExecutorMap;

import java.lang.reflect.Method;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.atomic.LongAdder;

/**
 * EXPERIMENT (2026-09-22): a per-event-loop bump arena for cycle-scoped buffers - the PoC measured in
 * netty-bench/docs/BACKLOG.md (block B2).
 * <p>
 * One {@link Arena} per thread, two {@link Space}s per arena (heap and direct). A space owns up to
 * {@code arena.maxBlocks} blocks; a block's memory is a chunk buffer taken from the very same
 * {@link AdaptivePoolingAllocator.ChunkAllocator} the {@link AdaptiveByteBufAllocator} uses, so the backing
 * memory is accounted and allocated exactly as adaptive's chunks are. Allocation is a bump pointer in the
 * current block; when it does not fit, the space reuses a wholly-free block, grows (x2 up to
 * {@code arena.maxBlock}), or gives up and the request goes to a delegate {@link AdaptiveByteBufAllocator}.
 * <p>
 * Release must happen on the allocating thread (throws otherwise) and uses a plain int refcount plus a per
 * block live counter - no atomics anywhere. What happens when a block goes idle is chosen by
 * {@code -Darena.release}:
 * <ul>
 *   <li>{@code zero}: a block whose live count returns to zero resets its bump pointer.</li>
 *   <li>{@code lifo} (default): {@code zero}, plus a LIFO pop of the topmost buffer of a block.</li>
 *   <li>{@code hook}: nothing is reset automatically; {@link #endOfCycle()} resets every wholly-free block
 *       and keeps at most {@code arena.retainBytes} worth of blocks, releasing the rest.</li>
 * </ul>
 * {@link #trim()} drops every wholly-free block but the first, for any policy.
 */
public final class CycleArenaAllocator extends AbstractByteBufAllocator {
    static final int INITIAL_BLOCK = Integer.getInteger("arena.initialBlock", 256 * 1024);
    static final int MAX_BLOCK = Integer.getInteger("arena.maxBlock", 8 * 1024 * 1024);
    static final int MAX_BLOCKS = Integer.getInteger("arena.maxBlocks", 4);
    static final long RETAIN_BYTES = Long.getLong("arena.retainBytes", 1024L * 1024L);
    static final int MIN_SIZE = 32;

    /** {@code -Darena.release}: what happens to a block when its live count returns to zero. */
    static final int RELEASE_ZERO = 0;
    static final int RELEASE_LIFO = 1;
    static final int RELEASE_HOOK = 2;
    static final int RELEASE = parseRelease(System.getProperty("arena.release", "lifo"));
    /**
     * {@code -Darena.hook}: {@code iteration} (the default) makes the first allocation on an event loop thread
     * register a tail task with {@code SingleThreadEventLoop.executeAfterEventLoopIteration}, Netty's own
     * end-of-iteration hook, which then closes a cycle after every loop iteration and re-registers itself.
     * {@code off} leaves closing a cycle entirely to whoever calls {@link #endOfCycle()} - the e2e launcher's
     * channelReadComplete handler, for instance. Only consulted with {@code -Darena.release=hook}.
     */
    static final boolean HOOK_ITERATION = !"off".equals(System.getProperty("arena.hook", "iteration"));

    private static int parseRelease(String name) {
        if ("zero".equals(name)) {
            return RELEASE_ZERO;
        }
        if ("lifo".equals(name)) {
            return RELEASE_LIFO;
        }
        if ("hook".equals(name)) {
            return RELEASE_HOOK;
        }
        throw new IllegalArgumentException("-Darena.release must be zero|lifo|hook, was: " + name);
    }

    // TELEMETRY (PoC): where allocations went, and every event that moves a block's bump pointer.
    static final LongAdder T_ARENA_HEAP = new LongAdder();
    static final LongAdder T_ARENA_DIRECT = new LongAdder();
    static final LongAdder T_FB_HEAP = new LongAdder();
    static final LongAdder T_FB_DIRECT = new LongAdder();
    static final LongAdder T_BLOCK_REUSE = new LongAdder();
    static final LongAdder T_GROW = new LongAdder();
    static final LongAdder T_RESET_ZERO = new LongAdder();
    static final LongAdder T_LIFO_POP = new LongAdder();
    static final LongAdder T_HOOK_ITERATION = new LongAdder();
    static final LongAdder T_HOOK_READ_COMPLETE = new LongAdder();
    static final LongAdder T_HOOK_REGISTERED = new LongAdder();
    static final LongAdder T_HOOK_RESET = new LongAdder();
    static final LongAdder T_HOOK_DROP = new LongAdder();
    static final LongAdder T_TRIM_DROP = new LongAdder();
    static final LongAdder T_CAP_IN_PLACE = new LongAdder();
    static final LongAdder T_CAP_MOVE = new LongAdder();
    static final LongAdder T_CAP_SOLO = new LongAdder();
    static final LongAdder T_UNPOOLED_OBJ = new LongAdder();

    public static String counters() {
        return "ARENATELE release=" + System.getProperty("arena.release", "lifo")
                + " arenaHeap=" + T_ARENA_HEAP.sum() + " arenaDirect=" + T_ARENA_DIRECT.sum()
                + " fallbackHeap=" + T_FB_HEAP.sum() + " fallbackDirect=" + T_FB_DIRECT.sum()
                + " blockReuse=" + T_BLOCK_REUSE.sum() + " grow=" + T_GROW.sum()
                + " resetOnZero=" + T_RESET_ZERO.sum() + " lifoPop=" + T_LIFO_POP.sum()
                + " hookIteration=" + T_HOOK_ITERATION.sum()
                + " hookReadComplete=" + T_HOOK_READ_COMPLETE.sum()
                + " hookRegistered=" + T_HOOK_REGISTERED.sum() + " hookReset=" + T_HOOK_RESET.sum()
                + " hookDrop=" + T_HOOK_DROP.sum() + " trimDrop=" + T_TRIM_DROP.sum()
                + " capInPlace=" + T_CAP_IN_PLACE.sum() + " capMove=" + T_CAP_MOVE.sum()
                + " capSolo=" + T_CAP_SOLO.sum() + " unpooledObjects=" + T_UNPOOLED_OBJ.sum();
    }

    private final AdaptiveByteBufAllocator fallback = new AdaptiveByteBufAllocator();
    private final AdaptivePoolingAllocator.ChunkAllocator heapChunks =
            new AdaptiveByteBufAllocator.HeapChunkAllocator(this);
    private final AdaptivePoolingAllocator.ChunkAllocator directChunks =
            new AdaptiveByteBufAllocator.DirectChunkAllocator(this);
    private final FastThreadLocal<Arena> arenas = new FastThreadLocal<Arena>() {
        @Override
        protected Arena initialValue() {
            Arena arena = new Arena(CycleArenaAllocator.this, Thread.currentThread());
            IterationHook hook = newIterationHook();
            arena.heap.hook = hook;
            arena.direct.hook = hook;
            return arena;
        }
    };

    // One thread local per memory kind: going through the Arena to reach its Space would put one more
    // dependent load on the allocation path, which measures as ~1.8ns/op on the E_COMMERCE pattern.
    private final FastThreadLocal<Space> heapSpaces = new FastThreadLocal<Space>() {
        @Override
        protected Space initialValue() {
            return arenas.get().heap;
        }
    };
    private final FastThreadLocal<Space> directSpaces = new FastThreadLocal<Space>() {
        @Override
        protected Space initialValue() {
            return arenas.get().direct;
        }
    };

    public CycleArenaAllocator() {
        super(!PlatformDependent.isExplicitNoPreferDirect());
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        ByteBuf b = heapSpaces.get().allocate(initialCapacity, maxCapacity);
        if (b != null) {
            T_ARENA_HEAP.increment();
            return b;
        }
        T_FB_HEAP.increment();
        return fallback.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        ByteBuf b = directSpaces.get().allocate(initialCapacity, maxCapacity);
        if (b != null) {
            T_ARENA_DIRECT.increment();
            return b;
        }
        T_FB_DIRECT.increment();
        return fallback.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    /**
     * End of a cycle on the calling thread: with {@code -Darena.release=hook} this is the only thing that
     * resets a block. Wholly-free blocks are reset and all but {@code arena.retainBytes} worth of them are
     * released. A no-op when the calling thread never allocated from this allocator.
     */
    public void endOfCycle() {
        T_HOOK_READ_COMPLETE.increment();
        endOfCycle0();
    }

    private void endOfCycle0() {
        Arena a = arenas.getIfExists();
        if (a == null) {
            return;
        }
        a.checkOwner();
        a.heap.endOfCycle();
        a.direct.endOfCycle();
    }

    /**
     * Netty's own per-iteration hook. {@code SingleThreadEventLoop.executeAfterEventLoopIteration} lives in
     * netty-transport, which netty-buffer cannot depend on, so it is reached reflectively: once per thread to
     * look the method up, then one queue offer per iteration in which the arena allocated at all.
     * <p>
     * The task deliberately does NOT re-register itself from inside its own {@code run()}: the tail queue is
     * drained by {@code runAllTasksFrom(tailTasks)}, which polls until the queue is empty, so a self
     * re-registering tail task spins the event loop forever (measured: the loop never returns). Re-arming it
     * from the allocation path instead also means an idle loop keeps no task queued and can still block in
     * select.
     */
    private IterationHook newIterationHook() {
        if (RELEASE != RELEASE_HOOK || !HOOK_ITERATION) {
            return null;
        }
        EventExecutor executor = ThreadExecutorMap.currentExecutor();
        if (executor == null) {
            return null;         // not an event loop thread: endOfCycle() stays the caller's job
        }
        try {
            Method register = executor.getClass().getMethod("executeAfterEventLoopIteration", Runnable.class);
            T_HOOK_REGISTERED.increment();
            return new IterationHook(this, executor, register);
        } catch (Throwable ignored) {
            return null;         // not a SingleThreadEventLoop: nothing to hook
        }
    }

    /** Closes a cycle at the end of the event loop iteration it was armed in. */
    static final class IterationHook implements Runnable {
        private final CycleArenaAllocator allocator;
        private final EventExecutor loop;
        private final Method register;
        boolean armed;

        IterationHook(CycleArenaAllocator allocator, EventExecutor loop, Method register) {
            this.allocator = allocator;
            this.loop = loop;
            this.register = register;
        }

        void arm() {
            armed = true;
            if (loop.isShuttingDown()) {
                return;
            }
            try {
                register.invoke(loop, this);
            } catch (Throwable ignored) {
                armed = false;
            }
        }

        @Override
        public void run() {
            armed = false;
            T_HOOK_ITERATION.increment();
            allocator.endOfCycle0();
        }
    }

    /** Drop every wholly-free block but the first, on the calling thread, for any release policy. */
    public void trim() {
        Arena a = arenas.getIfExists();
        if (a == null) {
            return;
        }
        a.checkOwner();
        a.heap.trim();
        a.direct.trim();
    }

    /** A bump region: a chunk buffer from the same {@code ChunkAllocator} the adaptive allocator uses. */
    static final class Block {
        final Space space;
        final AbstractByteBuf root;
        final byte[] mem;          // null for a direct block
        final long address;        // root memory address, 0 when the root has none
        final boolean hasAddress;
        final int size;
        final boolean solo;        // not part of the space's block array: freed as soon as it goes idle
        int bump;
        int live;

        Block(Space space, int size, boolean solo) {
            this.space = space;
            this.size = size;
            this.solo = solo;
            root = space.chunks.allocate(size, size);
            mem = root.hasArray() ? root.array() : null;
            hasAddress = root.hasMemoryAddress();
            address = hasAddress ? root.memoryAddress() : 0L;
        }
    }

    /** The blocks of one memory kind (heap or direct) for one thread. */
    static final class Space {
        final Arena arena;
        final AdaptivePoolingAllocator.ChunkAllocator chunks;
        final boolean direct;
        final Block[] blocks = new Block[MAX_BLOCKS];
        int blockCount;
        Block current;
        Block reserved;            // out parameter of reserve()
        IterationHook hook;        // only with -Darena.release=hook on an event loop thread

        Space(Arena arena, AdaptivePoolingAllocator.ChunkAllocator chunks, boolean direct) {
            this.arena = arena;
            this.chunks = chunks;
            this.direct = direct;
            current = newBlock(INITIAL_BLOCK);
        }

        private Block newBlock(int size) {
            Block b = new Block(this, size, false);
            blocks[blockCount++] = b;
            return b;
        }

        /**
         * Reserve {@code size} bytes; on success {@link #reserved} holds the block and the offset is returned,
         * on failure -1 is returned and the caller must go elsewhere. The block's live count is NOT bumped.
         */
        int reserve(int size) {
            if (size > MAX_BLOCK) {
                return -1;            // never fits a block: the delegate owns it
            }
            Block b = current;
            int start = b == null ? size + 1 : b.bump;
            if (b == null || start + size > b.size) {
                b = nextBlock(size);
                if (b == null) {
                    return -1;
                }
                start = b.bump;
            }
            b.bump = start + size;
            reserved = b;
            return start;
        }

        /**
         * The hot path keeps the block in a local: storing it in a field would cost a card-table write
         * barrier on every allocation, which {@link #reserve(int)} (only used when a buffer grows) can afford.
         */
        ByteBuf allocate(int size, int maxCapacity) {
            if (size < MIN_SIZE) {
                size = MIN_SIZE;
            } else if (size > MAX_BLOCK) {
                return null;          // never fits a block: the delegate owns it
            }
            if (RELEASE == RELEASE_HOOK) {
                IterationHook h = hook;
                if (h != null && !h.armed) {
                    h.arm();       // close this cycle at the end of the event loop iteration
                }
            }
            Block b = current;
            int start = b.bump;
            int end = start + size;
            if (end > b.size) {
                b = nextBlock(size);
                if (b == null) {
                    return null;
                }
                start = 0;
                end = size;
            }
            b.bump = end;
            b.live++;
            ArenaBuf buf = arena.newBuf();
            buf.init(b, start, size, maxCapacity);
            return buf;
        }

        /** The current block cannot serve the request: reuse an idle one, grow, or give up (fallback). */
        private Block nextBlock(int size) {
            for (int i = 0; i < blockCount; i++) {
                Block b = blocks[i];
                if (b.live == 0 && b.size >= size) {
                    T_BLOCK_REUSE.increment();
                    b.bump = 0;
                    current = b;
                    return b;
                }
            }
            if (blockCount < MAX_BLOCKS) {
                int previous = blockCount == 0 ? INITIAL_BLOCK : blocks[blockCount - 1].size;
                int next = Math.min(MAX_BLOCK, Math.max(size, previous << 1));
                if (next < size) {
                    return null;
                }
                T_GROW.increment();
                current = newBlock(next);
                return current;
            }
            return null;
        }

        /** A block that is not in {@link #blocks}: its root is freed as soon as the block goes idle. */
        Block soloBlock(int size) {
            T_CAP_SOLO.increment();
            return new Block(this, size, true);
        }

        void endOfCycle() {
            long kept = 0;
            int w = 0;
            for (int i = 0; i < blockCount; i++) {
                Block b = blocks[i];
                boolean free = b.live == 0;
                if (free && b.bump != 0) {
                    b.bump = 0;
                    T_HOOK_RESET.increment();
                }
                if (!free || w == 0 || kept + b.size <= RETAIN_BYTES) {
                    kept += b.size;
                    blocks[w++] = b;
                } else {
                    T_HOOK_DROP.increment();
                    drop(b);
                }
            }
            compact(w);
        }

        void trim() {
            int w = 0;
            for (int i = 0; i < blockCount; i++) {
                Block b = blocks[i];
                if (w > 0 && b.live == 0) {
                    T_TRIM_DROP.increment();
                    drop(b);
                } else {
                    blocks[w++] = b;
                }
            }
            compact(w);
        }

        private void drop(Block b) {
            if (current == b) {
                current = null;
            }
            b.root.release();
        }

        private void compact(int w) {
            for (int i = w; i < blockCount; i++) {
                blocks[i] = null;
            }
            blockCount = w;
            if (current == null && w > 0) {
                current = blocks[0];
            }
        }
    }

    static final class Arena {
        final CycleArenaAllocator alloc;
        final Thread owner;
        final Space heap;
        final Space direct;
        static final int POOL = Integer.getInteger("arena.objects", 8192);
        final ArenaBuf[] objects = new ArenaBuf[POOL];   // written once per object, when it is created
        final int[] free = new int[POOL];                // indexes of pooled objects: an int stack, no barriers
        int objCount;
        int freeTop;

        Arena(CycleArenaAllocator alloc, Thread owner) {
            this.alloc = alloc;
            this.owner = owner;
            heap = new Space(this, alloc.heapChunks, false);
            direct = new Space(this, alloc.directChunks, true);
        }

        void checkOwner() {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("cycle arena touched from a thread other than its event loop");
            }
        }

        ArenaBuf newBuf() {
            if (freeTop > 0) {
                return objects[free[--freeTop]];
            }
            if (objCount < POOL) {
                ArenaBuf buf = new ArenaBuf(alloc, this, objCount);
                objects[objCount++] = buf;
                return buf;
            }
            T_UNPOOLED_OBJ.increment();
            return new ArenaBuf(alloc, this, -1);        // beyond the pool: garbage after release
        }

        /** Give a region back to its block, without recycling the buffer object. */
        void releaseRegion(Block b, int start, int end) {
            if (--b.live == 0) {
                if (b.solo) {
                    b.root.release();
                } else if (RELEASE != RELEASE_HOOK) {
                    T_RESET_ZERO.increment();
                    b.bump = 0;              // wholly free: the block is a fresh bump region again
                }
            } else if (RELEASE == RELEASE_LIFO && end == b.bump) {
                T_LIFO_POP.increment();
                b.bump = start;              // the topmost buffer: LIFO pop, its memory is reusable at once
            }
        }

        void release(ArenaBuf buf, Block b, int start, int end) {
            checkOwner();
            releaseRegion(b, start, end);
            int index = buf.index;
            if (index >= 0) {
                free[freeTop++] = index;
            }
        }
    }

    static final class ArenaBuf extends AbstractByteBuf {
        private final ByteBufAllocator alloc;
        private final Arena arena;   // an object never changes arena: stored once
        final int index;             // slot in the arena's object pool, -1 when not pooled
        private Block block;
        private AbstractByteBuf root;
        private int start;
        private int length;
        private int refCnt;
        // The NIO view must be per buffer: the block's root caches one internal ByteBuffer, and a gathering
        // write asks several buffers of the same block for theirs before using any of them.
        private ByteBuffer tmpNioBuf;
        private Block tmpNioBlock;

        ArenaBuf(ByteBufAllocator alloc, Arena arena, int index) {
            super(0);
            this.alloc = alloc;
            this.arena = arena;
            this.index = index;
        }

        void init(Block block, int start, int length, int maxCapacity) {
            // Only store the block and the root when they really change: both are reference fields, and a
            // card-table write barrier per allocation is more than the whole bump costs.
            if (this.block != block) {
                this.block = block;
                this.root = block.root;
            }
            this.start = start;
            this.length = length;
            this.refCnt = 1;
            maxCapacity(maxCapacity);
            setIndex(0, 0);
            markReaderIndex();
            markWriterIndex();
        }

        private void moveTo(Block block, int start, int length) {
            this.block = block;
            this.root = block.root;
            this.start = start;
            this.length = length;
        }

        private int idx(int index) {
            return start + index;
        }

        // --- reference counting: plain int, thread-confined by contract ---
        @Override public int refCnt() { return refCnt; }
        @Override public ByteBuf retain() { refCnt++; return this; }
        @Override public ByteBuf retain(int increment) { refCnt += increment; return this; }
        @Override public ByteBuf touch() { return this; }
        @Override public ByteBuf touch(Object hint) { return this; }
        @Override public boolean release() {
            if (--refCnt == 0) {
                arena.release(this, block, start, start + length);   // block and root are left in place
                return true;
            }
            return false;
        }
        @Override public boolean release(int decrement) {
            refCnt -= decrement - 1;
            return release();
        }

        // --- geometry ---
        @Override public int capacity() { return length; }

        @Override public ByteBuf capacity(int newCapacity) {
            checkNewCapacity(newCapacity);
            if (newCapacity <= length) {
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }
            Block b = block;
            // The last buffer of its block grows in place: bump the block's tail.
            if (b.bump == start + length && start + newCapacity <= b.size) {
                T_CAP_IN_PLACE.increment();
                b.bump = start + newCapacity;
                length = newCapacity;
                return this;
            }
            // Otherwise take a fresh region and copy, the way AdaptiveByteBuf reallocates.
            Space space = b.space;
            int newStart = space.reserve(newCapacity);
            Block newBlock;
            if (newStart < 0) {
                newBlock = space.soloBlock(newCapacity);
                newBlock.bump = newCapacity;
                newStart = 0;
            } else {
                T_CAP_MOVE.increment();
                newBlock = space.reserved;
            }
            newBlock.live++;
            newBlock.root.setBytes(newStart, b.root, start, length);
            int oldStart = start;
            int oldLength = length;
            moveTo(newBlock, newStart, newCapacity);
            tmpNioBuf = null;
            arena.releaseRegion(b, oldStart, oldStart + oldLength);
            return this;
        }

        @Override public ByteBufAllocator alloc() { return alloc; }
        @Override public ByteOrder order() { return ByteOrder.BIG_ENDIAN; }
        @Override public ByteBuf unwrap() { return null; }
        @Override public boolean isDirect() { return block.space.direct; }
        @Override boolean _isDirect() { return block.space.direct; }
        @Override public boolean hasArray() { return block.mem != null; }
        @Override public byte[] array() { ensureAccessible(); return block.mem; }
        @Override public int arrayOffset() { return start; }
        @Override public boolean hasMemoryAddress() { return block.hasAddress; }
        @Override public long memoryAddress() { ensureAccessible(); return block.address + start; }
        @Override long _memoryAddress() { return block.address + start; }
        @Override public int nioBufferCount() { return 1; }

        @Override public ByteBuffer nioBuffer(int index, int len) {
            checkIndex(index, len);
            return root.nioBuffer(idx(index), len);      // a fresh view, per call, for heap and direct alike
        }

        @Override public ByteBuffer internalNioBuffer(int index, int len) {
            checkIndex(index, len);
            ByteBuffer b = tmpNioBuf;
            if (b == null || tmpNioBlock != block) {
                b = root.internalNioBuffer(0, block.size).duplicate();
                tmpNioBuf = b;
                tmpNioBlock = block;
            }
            b.clear().position(idx(index)).limit(idx(index) + len);
            return b;
        }

        @Override public ByteBuffer[] nioBuffers(int index, int len) {
            return new ByteBuffer[] { nioBuffer(index, len) };
        }

        @Override public ByteBuf copy(int index, int len) {
            checkIndex(index, len);
            return root.copy(idx(index), len);
        }

        // --- element access: same shape as AdaptiveByteBuf (root + offset) ---
        @Override protected byte _getByte(int i) { return root._getByte(idx(i)); }
        @Override protected short _getShort(int i) { return root._getShort(idx(i)); }
        @Override protected short _getShortLE(int i) { return root._getShortLE(idx(i)); }
        @Override protected int _getUnsignedMedium(int i) { return root._getUnsignedMedium(idx(i)); }
        @Override protected int _getUnsignedMediumLE(int i) { return root._getUnsignedMediumLE(idx(i)); }
        @Override protected int _getInt(int i) { return root._getInt(idx(i)); }
        @Override protected int _getIntLE(int i) { return root._getIntLE(idx(i)); }
        @Override protected long _getLong(int i) { return root._getLong(idx(i)); }
        @Override protected long _getLongLE(int i) { return root._getLongLE(idx(i)); }
        @Override protected void _setByte(int i, int v) { root._setByte(idx(i), v); }
        @Override protected void _setShort(int i, int v) { root._setShort(idx(i), v); }
        @Override protected void _setShortLE(int i, int v) { root._setShortLE(idx(i), v); }
        @Override protected void _setMedium(int i, int v) { root._setMedium(idx(i), v); }
        @Override protected void _setMediumLE(int i, int v) { root._setMediumLE(idx(i), v); }
        @Override protected void _setInt(int i, int v) { root._setInt(idx(i), v); }
        @Override protected void _setIntLE(int i, int v) { root._setIntLE(idx(i), v); }
        @Override protected void _setLong(int i, long v) { root._setLong(idx(i), v); }
        @Override protected void _setLongLE(int i, long v) { root._setLongLE(idx(i), v); }

        // --- bulk access, delegated ---
        @Override public ByteBuf getBytes(int i, ByteBuf dst, int di, int len) { checkIndex(i, len); root.getBytes(idx(i), dst, di, len); return this; }
        @Override public ByteBuf getBytes(int i, byte[] dst, int di, int len) { checkIndex(i, len); root.getBytes(idx(i), dst, di, len); return this; }
        @Override public ByteBuf getBytes(int i, ByteBuffer dst) { checkIndex(i, dst.remaining()); root.getBytes(idx(i), dst); return this; }
        @Override public ByteBuf getBytes(int i, OutputStream out, int len) throws IOException { checkIndex(i, len); root.getBytes(idx(i), out, len); return this; }
        @Override public int getBytes(int i, GatheringByteChannel out, int len) throws IOException { checkIndex(i, len); return root.getBytes(idx(i), out, len); }
        @Override public int getBytes(int i, FileChannel out, long pos, int len) throws IOException { checkIndex(i, len); return root.getBytes(idx(i), out, pos, len); }
        @Override public ByteBuf setBytes(int i, ByteBuf src, int si, int len) { checkIndex(i, len); root.setBytes(idx(i), src, si, len); return this; }
        @Override public ByteBuf setBytes(int i, byte[] src, int si, int len) { checkIndex(i, len); root.setBytes(idx(i), src, si, len); return this; }
        @Override public ByteBuf setBytes(int i, ByteBuffer src) { checkIndex(i, src.remaining()); root.setBytes(idx(i), src); return this; }
        @Override public int setBytes(int i, InputStream in, int len) throws IOException { checkIndex(i, len); return root.setBytes(idx(i), in, len); }
        @Override public int setBytes(int i, ScatteringByteChannel in, int len) throws IOException { checkIndex(i, len); return root.setBytes(idx(i), in, len); }
        @Override public int setBytes(int i, FileChannel in, long pos, int len) throws IOException { checkIndex(i, len); return root.setBytes(idx(i), in, pos, len); }
    }
}
