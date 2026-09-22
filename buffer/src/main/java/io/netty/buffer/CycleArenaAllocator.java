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

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ThreadExecutorMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * EXPERIMENT (2026-09-22): the event-loop arena of {@code netty-bench/docs/event-loop-arena-design.md}
 * (draft 3, sections 3-6). A proof of concept: the architecture and the lifecycle are the designed ones,
 * the polish is not.
 * <p>
 * One {@link Arena} per thread ({@link FastThreadLocal}), two {@link Space}s per arena - heap and direct.
 * A space owns at most {@link #MAX_BLOCKS} FIXED-size blocks; a block's memory is a chunk buffer taken from
 * the very same {@link AdaptivePoolingAllocator.ChunkAllocator} the {@link AdaptiveByteBufAllocator} uses,
 * so the backing memory is allocated exactly as adaptive's chunks are (outside adaptive's byte budget, by
 * design - see the metrics). Allocation is a bump of the current block; buffer objects come from a per-arena
 * array with an {@code int} free stack.
 * <p>
 * The lifecycle has exactly one rule: <b>memory freed during an iteration is never handed out again before
 * the end-of-iteration hook</b>. Releasing a buffer only decrements its block's live count; at the hook
 * ({@code SingleThreadEventLoop.executeAfterEventLoopIteration}, armed from the allocation path once per
 * iteration) every block whose live count is zero becomes reusable. A block with live buffers at the hook is
 * pinned and is skipped until a later hook finds it empty. Nothing frees a block except {@link #trim()} and
 * thread termination.
 * <p>
 * <b>Invariant A (confinement).</b> Every field of an {@link ArenaBuf} and of a {@link Block} is read and
 * written only by the owning thread. {@link ArenaBuf#retain()}, {@code retain(int)}, {@link ArenaBuf#release()}
 * and {@code release(int)} compare {@link Thread#currentThread()} with the owner BEFORE touching any field and
 * throw {@link IllegalStateException} otherwise, after counting the violation. There is no cross-thread path:
 * no list, no atomics, no wake-up, no drain.
 * <p>
 * Anything the arena cannot serve - a request above {@link #CAP}, an exhausted object pool, a space whose
 * blocks are all pinned - goes to a delegate {@link AdaptiveByteBufAllocator}.
 */
public final class CycleArenaAllocator extends AbstractByteBufAllocator {

    /** Fixed block size. Fixed, not geometric: a pinned block must cost little (design section 2.2). */
    static final int BLOCK_SIZE = Integer.getInteger("arena.blockSize", 256 * 1024);
    /** At most this many blocks per space; when none is reusable and the bound is reached, we delegate. */
    static final int MAX_BLOCKS = Integer.getInteger("arena.maxBlocks", 8);
    /** {@code size > cap} is delegated: it keeps the surviving BYTES out of the arena (design section 1). */
    static final int CAP = Integer.getInteger("arena.cap", 8 * 1024);
    /** Upper bound on the per-arena buffer-object array; past it, allocation delegates. */
    static final int MAX_OBJECTS = Integer.getInteger("arena.maxObjects", 16 * 1024);
    /** {@code -Darena.debug=true}: check that no block is reused before a hook. Folded away when false. */
    static final boolean DEBUG = Boolean.getBoolean("arena.debug");

    private static final int INITIAL_OBJECTS = Math.min(256, MAX_OBJECTS);

    static {
        if (((CAP + 7) & ~7) > BLOCK_SIZE || CAP < 0 || MAX_BLOCKS < 1 || MAX_OBJECTS < 1) {
            throw new IllegalArgumentException("arena.cap=" + CAP + " arena.blockSize=" + BLOCK_SIZE
                    + " arena.maxBlocks=" + MAX_BLOCKS + " arena.maxObjects=" + MAX_OBJECTS);
        }
    }

    /** Every live arena, for {@link #counters()} only: touched when an arena is created or terminated. */
    private static final List<Arena> ARENAS = new ArrayList<Arena>();
    /** Counters of arenas whose thread is gone, so that termination does not lose them. */
    private static final Counters DEAD = new Counters();

    private final AdaptiveByteBufAllocator delegate = new AdaptiveByteBufAllocator();
    private final AdaptivePoolingAllocator.ChunkAllocator heapChunks =
            new AdaptiveByteBufAllocator.HeapChunkAllocator(this);
    private final AdaptivePoolingAllocator.ChunkAllocator directChunks =
            new AdaptiveByteBufAllocator.DirectChunkAllocator(this);

    private final FastThreadLocal<Arena> arenas = new FastThreadLocal<Arena>() {
        @Override
        protected Arena initialValue() {
            Arena arena = new Arena(CycleArenaAllocator.this, Thread.currentThread());
            IterationHook hook = newIterationHook(arena);
            arena.heap.hook = hook;
            arena.direct.hook = hook;
            arena.hook = hook;
            synchronized (ARENAS) {
                ARENAS.add(arena);
            }
            return arena;
        }

        @Override
        protected void onRemoval(Arena arena) {
            arena.terminate();
        }
    };

    // One thread local per memory kind: reaching the Space through the Arena would put one more dependent
    // load on the allocation path (measured at ~1.8ns/op on the E_COMMERCE pattern with the first PoC).
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
        Space space = heapSpaces.get();
        ByteBuf buf = space.allocate(initialCapacity, maxCapacity);
        if (buf != null) {
            return buf;
        }
        return delegateHeap(space, initialCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        Space space = directSpaces.get();
        ByteBuf buf = space.allocate(initialCapacity, maxCapacity);
        if (buf != null) {
            return buf;
        }
        return delegateDirect(space, initialCapacity, maxCapacity);
    }

    private ByteBuf delegateHeap(Space space, int initialCapacity, int maxCapacity) {
        space.delegateAllocations++;
        return delegate.heapBuffer(initialCapacity, maxCapacity);
    }

    private ByteBuf delegateDirect(Space space, int initialCapacity, int maxCapacity) {
        space.delegateAllocations++;
        return delegate.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    /**
     * Give back every reusable block but the first, on the calling thread. Explicit only: nothing else ever
     * returns a block to the chunk allocator.
     */
    public void trim() {
        Arena arena = arenas.getIfExists();
        if (arena == null) {
            return;
        }
        arena.checkOwner();
        arena.heap.trim();
        arena.direct.trim();
    }

    /** Run the end-of-iteration hook on the calling thread's arena. Package private: for tests only. */
    void runHookForTest() {
        Arena arena = arenas.getIfExists();
        if (arena != null) {
            arena.checkOwner();
            arena.hookNow();
        }
    }

    /** The calling thread's arena, or {@code null}. Package private: for tests only. */
    Arena arenaForTest() {
        return arenas.getIfExists();
    }

    /** Terminate the calling thread's arena, as {@code FastThreadLocal} removal does. For tests only. */
    void removeForTest() {
        heapSpaces.remove();
        directSpaces.remove();
        arenas.remove();
    }

    /**
     * Netty's own per-iteration hook. {@code SingleThreadEventLoop.executeAfterEventLoopIteration} lives in
     * netty-transport, which netty-buffer cannot depend on, so it is reached reflectively: once per thread to
     * look the method up, then one queue offer per iteration in which the arena allocated at all.
     * <p>
     * The task deliberately does NOT re-register itself from inside its own {@code run()}: the tail queue is
     * drained by {@code runAllTasksFrom(tailTasks)}, which polls until the queue is empty, so a self
     * re-registering tail task spins the event loop forever (measured: the loop never returns). It is
     * re-armed from the allocation path instead, which also lets an idle loop keep no task queued and go
     * back to sleep in select.
     */
    private static IterationHook newIterationHook(Arena arena) {
        EventExecutor executor = ThreadExecutorMap.currentExecutor();
        if (executor == null) {
            return null;             // not an event loop thread: nothing ever closes an iteration here
        }
        try {
            Method register = executor.getClass().getMethod("executeAfterEventLoopIteration", Runnable.class);
            return new IterationHook(arena, executor, register);
        } catch (Throwable ignored) {
            return null;             // not a SingleThreadEventLoop: nothing to hook
        }
    }

    /** Closes the iteration it was armed in. Armed from the allocation path, never from its own run(). */
    static final class IterationHook implements Runnable {
        private final Arena arena;
        private final EventExecutor loop;
        private final Method register;
        boolean armed;

        IterationHook(Arena arena, EventExecutor loop, Method register) {
            this.arena = arena;
            this.loop = loop;
            this.register = register;
        }

        void arm() {
            armed = true;
            try {
                register.invoke(loop, this);
            } catch (Throwable ignored) {
                // Rejected after isShutdown(), or not an event loop after all: swallow, as the design says.
                armed = false;
                arena.hookRejections++;
            }
        }

        @Override
        public void run() {
            armed = false;
            arena.hookNow();
        }
    }

    /** A fixed-size bump region: a chunk buffer from the same {@code ChunkAllocator} adaptive uses. */
    static final class Block {
        final Space space;
        final AbstractByteBuf root;
        final byte[] mem;          // null for a direct block
        final long address;        // root memory address, 0 when the root has none
        final boolean hasAddress;
        final boolean direct;
        int bump;
        int live;
        /** True when the LAST hook found this block empty; cleared when the allocator takes it. */
        boolean reusable;
        /** The hook generation that marked this block reusable; only read by the debug check. */
        long stamp;

        Block(Space space) {
            this.space = space;
            root = space.chunks.allocate(BLOCK_SIZE, BLOCK_SIZE);
            mem = root.hasArray() ? root.array() : null;
            hasAddress = root.hasMemoryAddress();
            address = hasAddress ? root.memoryAddress() : 0L;
            direct = space.direct;
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
        /** Out parameter of {@link #reserve(int)}, used by the reallocation path only. */
        Block reserved;
        /** No block was available since the last hook: do not scan again until then. */
        boolean exhausted;
        /** Number of hooks that have run, i.e. the index of the current iteration. */
        long generation;
        IterationHook hook;        // null when this thread is not an event loop thread

        // Metrics (plain longs, owner-written).
        long arenaAllocations;
        long delegateAllocations;
        long blockReuses;
        long blockGrowths;
        long reallocInPlace;
        long reallocMoved;
        long reallocDelegated;
        long trims;
        long trimmedBlocks;
        long leakedBlocks;
        long earlyReuses;
        int pinnedBlocks;
        int reusableBlocks;
        int maxPinned;

        Space(Arena arena, AdaptivePoolingAllocator.ChunkAllocator chunks, boolean direct) {
            this.arena = arena;
            this.chunks = chunks;
            this.direct = direct;
            current = newBlock();
        }

        private Block newBlock() {
            Block block = new Block(this);
            block.stamp = generation - 1;      // fresh memory: never "reused before its hook"
            blocks[blockCount++] = block;
            return block;
        }

        /**
         * THE hot path. Everything rare - arming the hook, growing the object array, taking another block,
         * giving up - is a call to a separate method on a branch that is not taken in the common case.
         */
        ByteBuf allocate(int size, int maxCapacity) {
            if (size > CAP) {
                return null;                   // above the cap: the delegate owns it
            }
            IterationHook h = hook;
            if (h != null && !h.armed) {
                h.arm();                       // once per iteration: close it at the end of this one
            }
            Block block = current;
            int start = block.bump;
            int end = start + ((size + 7) & ~7);
            if (end > BLOCK_SIZE) {
                return allocateInAnotherBlock(size, maxCapacity);
            }
            Arena a = arena;
            int top = a.freeTop - 1;
            if (top < 0) {
                return allocateWithNewObject(block, start, end, size, maxCapacity);
            }
            a.freeTop = top;
            block.bump = end;
            block.live++;
            arenaAllocations++;
            ArenaBuf buf = a.objects[a.free[top]];
            buf.init(block, start, size, maxCapacity);
            return buf;
        }

        /** The current block is full: take one the LAST hook marked reusable, or grow, or give up. */
        private ByteBuf allocateInAnotherBlock(int size, int maxCapacity) {
            Block block = nextBlock();
            if (block == null) {
                return null;
            }
            int end = (size + 7) & ~7;
            Arena a = arena;
            int top = a.freeTop - 1;
            if (top < 0) {
                return allocateWithNewObject(block, 0, end, size, maxCapacity);
            }
            a.freeTop = top;
            block.bump = end;
            block.live++;
            arenaAllocations++;
            ArenaBuf buf = a.objects[a.free[top]];
            buf.init(block, 0, size, maxCapacity);
            return buf;
        }

        /** The object free stack is empty: create one more buffer object, or give up. */
        private ByteBuf allocateWithNewObject(Block block, int start, int end, int size, int maxCapacity) {
            ArenaBuf buf = arena.newObject();
            if (buf == null) {
                return null;                   // maxObjects reached: the delegate owns it
            }
            block.bump = end;
            block.live++;
            arenaAllocations++;
            buf.init(block, start, size, maxCapacity);
            return buf;
        }

        /**
         * A block marked reusable by the LAST hook, or a new one, or {@code null} when the bound is reached.
         * Only the current block is ever bumped, so a reusable block always has {@code live == 0, bump == 0}.
         */
        private Block nextBlock() {
            if (exhausted) {
                return null;                   // nothing changed since the last failure: no rescan
            }
            for (int i = 0; i < blockCount; i++) {
                Block block = blocks[i];
                if (block.reusable) {
                    if (DEBUG) {
                        checkNotReusedBeforeHook(block);
                    }
                    block.reusable = false;
                    reusableBlocks--;
                    blockReuses++;
                    current = block;
                    return block;
                }
            }
            if (blockCount < MAX_BLOCKS) {
                blockGrowths++;
                current = newBlock();
                return current;
            }
            exhausted = true;
            return null;
        }

        private void checkNotReusedBeforeHook(Block block) {
            if (block.stamp >= generation) {
                earlyReuses++;
                throw new IllegalStateException("arena block reused in the iteration that freed it: stamp="
                        + block.stamp + " generation=" + generation);
            }
        }

        /** Reserve a region for a reallocation. Returns the offset, or -1; sets {@link #reserved}. */
        int reserve(int size) {
            if (size > CAP) {
                return -1;
            }
            Block block = current;
            int start = block.bump;
            int end = start + ((size + 7) & ~7);
            if (end > BLOCK_SIZE) {
                block = nextBlock();
                if (block == null) {
                    return -1;
                }
                start = 0;
                end = (size + 7) & ~7;
            }
            block.bump = end;
            block.live++;
            reserved = block;
            return start;
        }

        /** End of an iteration: every empty block becomes reusable; the current block stays current. */
        void endOfIteration() {
            Block cur = current;
            long gen = generation;
            int pinned = 0;
            int reusable = 0;
            for (int i = 0; i < blockCount; i++) {
                Block block = blocks[i];
                if (block.live == 0) {
                    block.bump = 0;
                    block.stamp = gen;
                    if (block != cur) {
                        block.reusable = true;
                        reusable++;
                    }
                } else {
                    pinned++;
                }
            }
            generation = gen + 1;
            pinnedBlocks = pinned;
            reusableBlocks = reusable;
            if (pinned > maxPinned) {
                maxPinned = pinned;
            }
            exhausted = false;
        }

        /** Give back every reusable block but the first. */
        void trim() {
            trims++;
            int w = 0;
            boolean keptOne = false;
            for (int i = 0; i < blockCount; i++) {
                Block block = blocks[i];
                if (block.reusable && block != current) {
                    if (keptOne) {
                        trimmedBlocks++;
                        reusableBlocks--;
                        block.root.release();
                        continue;
                    }
                    keptOne = true;
                }
                blocks[w++] = block;
            }
            for (int i = w; i < blockCount; i++) {
                blocks[i] = null;
            }
            blockCount = w;
        }

        /** Thread termination: empty blocks go back, pinned ones leak - no other thread may free them. */
        void terminate() {
            for (int i = 0; i < blockCount; i++) {
                Block block = blocks[i];
                if (block.live == 0) {
                    block.root.release();
                } else {
                    leakedBlocks++;
                }
                blocks[i] = null;
            }
            blockCount = 0;
            current = null;
            reserved = null;
            hook = null;
        }
    }

    static final class Arena {
        final CycleArenaAllocator alloc;
        final Thread owner;
        final Space heap;
        final Space direct;
        ArenaBuf[] objects = new ArenaBuf[INITIAL_OBJECTS];
        int[] free = new int[INITIAL_OBJECTS];
        int objectCount;
        int freeTop;
        IterationHook hook;

        // Metrics.
        long violations;           // written by the violating thread, read at shutdown: racy by construction
        long hooks;
        long hookRejections;
        long objectGrowths;

        Arena(CycleArenaAllocator alloc, Thread owner) {
            this.alloc = alloc;
            this.owner = owner;
            heap = new Space(this, alloc.heapChunks, false);
            direct = new Space(this, alloc.directChunks, true);
        }

        void checkOwner() {
            if (Thread.currentThread() != owner) {
                violations++;
                throw new IllegalStateException("event-loop arena touched from " + Thread.currentThread()
                        + ", owner is " + owner);
            }
        }

        /** Grow the object array, up to {@code maxObjects}; {@code null} means "delegate this one". */
        ArenaBuf newObject() {
            int count = objectCount;
            if (count == objects.length) {
                if (count >= MAX_OBJECTS) {
                    return null;
                }
                int size = Math.min(MAX_OBJECTS, count * 2);
                ArenaBuf[] grown = new ArenaBuf[size];
                System.arraycopy(objects, 0, grown, 0, count);
                objects = grown;
                int[] grownFree = new int[size];
                System.arraycopy(free, 0, grownFree, 0, freeTop);
                free = grownFree;
                objectGrowths++;
            }
            ArenaBuf buf = new ArenaBuf(alloc, this, count);
            objects[count] = buf;
            objectCount = count + 1;
            return buf;
        }

        void hookNow() {
            hooks++;
            heap.endOfIteration();
            direct.endOfIteration();
        }

        void terminate() {
            heap.terminate();
            direct.terminate();
            synchronized (ARENAS) {
                DEAD.add(this);
                ARENAS.remove(this);
            }
        }
    }

    static final class ArenaBuf extends AbstractByteBuf {
        private final ByteBufAllocator alloc;
        private final Arena arena;       // an object never changes arena: stored once, at construction
        private final Thread owner;      // arena.owner, inlined to keep the confinement check to one load
        final int index;                 // stable slot in the arena's object array
        /** The block this buffer's region belongs to, or {@code null} in the DELEGATED state. */
        private Block block;
        /** The block's root, or - in the DELEGATED state - the delegate buffer (unwrapped). */
        private AbstractByteBuf root;
        /** DELEGATED state only: the buffer to release, leak-aware wrapper and all. */
        private ByteBuf delegated;
        private int start;
        private int length;
        private int refCnt;
        // The NIO view must be per buffer: the block's root caches one internal ByteBuffer, and a gathering
        // write asks several buffers of the same block for theirs before using any of them.
        private ByteBuffer tmpNioBuf;
        private AbstractByteBuf tmpNioRoot;

        ArenaBuf(ByteBufAllocator alloc, Arena arena, int index) {
            super(0);
            this.alloc = alloc;
            this.arena = arena;
            this.owner = arena.owner;
            this.index = index;
        }

        /** The block this buffer's region belongs to, {@code null} when DELEGATED. For tests only. */
        Block blockForTest() {
            return block;
        }

        void init(Block block, int start, int length, int maxCapacity) {
            // Only store the block and the root when they really change: both are reference fields, and a
            // card-table write barrier per allocation costs more than the whole bump does.
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

        // --- reference counting: plain int, owner-confined (Invariant A) ---

        @Override
        public int refCnt() {
            return refCnt;              // unspecified off-thread, by contract
        }

        @Override
        public ByteBuf touch() {
            return this;
        }

        @Override
        public ByteBuf touch(Object hint) {
            return this;
        }

        @Override
        public ByteBuf retain() {
            if (Thread.currentThread() != owner) {
                throw violation();
            }
            int cnt = refCnt;
            if (cnt <= 0) {
                throw new IllegalReferenceCountException(cnt, 1);
            }
            refCnt = cnt + 1;
            return this;
        }

        @Override
        public ByteBuf retain(int increment) {
            if (Thread.currentThread() != owner) {
                throw violation();
            }
            int cnt = refCnt;
            if (cnt <= 0 || increment <= 0) {
                throw new IllegalReferenceCountException(cnt, increment);
            }
            refCnt = cnt + increment;
            return this;
        }

        @Override
        public boolean release() {
            if (Thread.currentThread() != owner) {
                throw violation();
            }
            int cnt = refCnt;
            if (cnt <= 0) {
                throw new IllegalReferenceCountException(cnt, -1);
            }
            refCnt = cnt - 1;
            if (cnt != 1) {
                return false;
            }
            Block b = block;
            if (b == null) {
                return releaseDelegated();
            }
            b.live--;
            Arena a = arena;
            a.free[a.freeTop++] = index;
            return true;
        }

        @Override
        public boolean release(int decrement) {
            if (Thread.currentThread() != owner) {
                throw violation();
            }
            int cnt = refCnt;
            if (decrement <= 0 || cnt < decrement) {
                throw new IllegalReferenceCountException(cnt, -decrement);
            }
            refCnt = cnt - decrement;
            if (cnt != decrement) {
                return false;
            }
            Block b = block;
            if (b == null) {
                return releaseDelegated();
            }
            b.live--;
            Arena a = arena;
            a.free[a.freeTop++] = index;
            return true;
        }

        /** DELEGATED: the region is a buffer of the delegate allocator; the object still goes back. */
        private boolean releaseDelegated() {
            ByteBuf buf = delegated;
            delegated = null;
            root = null;
            tmpNioBuf = null;
            tmpNioRoot = null;
            Arena a = arena;
            a.free[a.freeTop++] = index;
            buf.release();
            return true;
        }

        private IllegalStateException violation() {
            arena.violations++;
            return new IllegalStateException("arena buffer of " + owner + " touched from "
                    + Thread.currentThread() + " (event-loop arena buffers are thread confined)");
        }

        // --- geometry ---

        @Override
        public int capacity() {
            return length;
        }

        @Override
        public ByteBuf capacity(int newCapacity) {
            checkNewCapacity(newCapacity);
            if (newCapacity <= length) {
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }
            return grow(newCapacity);
        }

        /**
         * Grow: in place when this buffer is topmost in its block and the block has the room, else a new
         * region (arena when it fits and there is room, else the delegate) plus a copy. The old region is
         * released - so it is handed out again only after the next hook, which keeps any view taken during
         * this iteration valid until the iteration ends.
         */
        private ByteBuf grow(int newCapacity) {
            Block b = block;
            if (b == null) {
                delegated.capacity(newCapacity);   // DELEGATED: the delegate buffer grows itself
                length = newCapacity;
                tmpNioBuf = null;
                tmpNioRoot = null;
                return this;
            }
            Space space = b.space;
            int oldEnd = start + ((length + 7) & ~7);
            int newEnd = start + ((newCapacity + 7) & ~7);
            if (b.bump == oldEnd && newEnd <= BLOCK_SIZE) {
                b.bump = newEnd;
                length = newCapacity;
                space.reallocInPlace++;
                return this;
            }
            int newStart = space.reserve(newCapacity);
            if (newStart >= 0) {
                Block newBlock = space.reserved;
                space.reserved = null;
                newBlock.root.setBytes(newStart, b.root, start, length);
                space.reallocMoved++;
                moveTo(newBlock, newBlock.root, newStart, newCapacity);
            } else {
                ByteBuf buf = space.direct
                        ? arena.alloc.delegateDirect(space, newCapacity, maxCapacity())
                        : arena.alloc.delegateHeap(space, newCapacity, maxCapacity());
                AbstractByteBuf target = unwrapDelegate(buf);
                target.setBytes(0, root, start, length);
                space.reallocDelegated++;
                delegated = buf;
                moveTo(null, target, 0, newCapacity);
            }
            b.live--;
            return this;
        }

        /** The delegate wraps its buffers when leak detection is on; element access needs the raw one. */
        private static AbstractByteBuf unwrapDelegate(ByteBuf buf) {
            ByteBuf unwrapped = buf;
            while (!(unwrapped instanceof AbstractByteBuf)) {
                ByteBuf inner = unwrapped.unwrap();
                if (inner == null) {
                    throw new IllegalStateException("the delegate returned " + unwrapped.getClass());
                }
                unwrapped = inner;
            }
            return (AbstractByteBuf) unwrapped;
        }

        private void moveTo(Block newBlock, AbstractByteBuf newRoot, int newStart, int newLength) {
            block = newBlock;
            root = newRoot;
            start = newStart;
            length = newLength;
        }

        private int idx(int index) {
            return start + index;
        }

        @Override
        public ByteBufAllocator alloc() {
            return alloc;
        }

        @Override
        public ByteOrder order() {
            return ByteOrder.BIG_ENDIAN;
        }

        @Override
        public ByteBuf unwrap() {
            return null;
        }

        @Override
        public boolean isDirect() {
            Block b = block;
            return b == null ? root.isDirect() : b.direct;
        }

        @Override
        boolean _isDirect() {
            return isDirect();
        }

        @Override
        public boolean hasArray() {
            Block b = block;
            return b == null ? root.hasArray() : b.mem != null;
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            Block b = block;
            return b == null ? root.array() : b.mem;
        }

        @Override
        public int arrayOffset() {
            Block b = block;
            return b == null ? root.arrayOffset() : start;
        }

        @Override
        public boolean hasMemoryAddress() {
            Block b = block;
            return b == null ? root.hasMemoryAddress() : b.hasAddress;
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return _memoryAddress();
        }

        @Override
        long _memoryAddress() {
            Block b = block;
            return b == null ? root.memoryAddress() : b.address + start;
        }

        @Override
        public int nioBufferCount() {
            return 1;
        }

        @Override
        public ByteBuffer nioBuffer(int index, int len) {
            checkIndex(index, len);
            return root.nioBuffer(idx(index), len);      // a fresh view per call, heap and direct alike
        }

        @Override
        public ByteBuffer internalNioBuffer(int index, int len) {
            checkIndex(index, len);
            AbstractByteBuf r = root;
            if (block == null) {
                return r.internalNioBuffer(index, len);  // DELEGATED: the delegate buffer has its own
            }
            ByteBuffer buf = tmpNioBuf;
            if (buf == null || tmpNioRoot != r) {
                buf = r.internalNioBuffer(0, r.capacity()).duplicate();
                tmpNioBuf = buf;
                tmpNioRoot = r;
            }
            buf.clear().position(idx(index)).limit(idx(index) + len);
            return buf;
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int len) {
            return new ByteBuffer[] { nioBuffer(index, len) };
        }

        @Override
        public ByteBuf copy(int index, int len) {
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

        // --- bulk access, delegated to the root ---
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

    /** A snapshot of every counter of section 6, summed over arenas. */
    static final class Counters {
        long arenaHeap;
        long arenaDirect;
        long delegateHeap;
        long delegateDirect;
        long blocksHeap;
        long blocksDirect;
        long pinned;
        long reusable;
        long maxPinnedHeap;
        long maxPinnedDirect;
        long violations;
        long hooks;
        long hookRejections;
        long reallocInPlace;
        long reallocMoved;
        long reallocDelegated;
        long trims;
        long trimmedBlocks;
        long leaked;
        long earlyReuses;
        long blockReuses;
        long blockGrowths;
        long objects;

        void add(Counters other) {
            arenaHeap += other.arenaHeap;
            arenaDirect += other.arenaDirect;
            delegateHeap += other.delegateHeap;
            delegateDirect += other.delegateDirect;
            blocksHeap += other.blocksHeap;
            blocksDirect += other.blocksDirect;
            pinned += other.pinned;
            reusable += other.reusable;
            maxPinnedHeap += other.maxPinnedHeap;
            maxPinnedDirect += other.maxPinnedDirect;
            violations += other.violations;
            hooks += other.hooks;
            hookRejections += other.hookRejections;
            reallocInPlace += other.reallocInPlace;
            reallocMoved += other.reallocMoved;
            reallocDelegated += other.reallocDelegated;
            trims += other.trims;
            trimmedBlocks += other.trimmedBlocks;
            leaked += other.leaked;
            earlyReuses += other.earlyReuses;
            blockReuses += other.blockReuses;
            blockGrowths += other.blockGrowths;
            objects += other.objects;
        }

        void add(Arena arena) {
            arenaHeap += arena.heap.arenaAllocations;
            arenaDirect += arena.direct.arenaAllocations;
            delegateHeap += arena.heap.delegateAllocations;
            delegateDirect += arena.direct.delegateAllocations;
            blocksHeap += arena.heap.blockCount;
            blocksDirect += arena.direct.blockCount;
            pinned += arena.heap.pinnedBlocks + arena.direct.pinnedBlocks;
            reusable += arena.heap.reusableBlocks + arena.direct.reusableBlocks;
            maxPinnedHeap += arena.heap.maxPinned;
            maxPinnedDirect += arena.direct.maxPinned;
            violations += arena.violations;
            hooks += arena.hooks;
            hookRejections += arena.hookRejections;
            reallocInPlace += arena.heap.reallocInPlace + arena.direct.reallocInPlace;
            reallocMoved += arena.heap.reallocMoved + arena.direct.reallocMoved;
            reallocDelegated += arena.heap.reallocDelegated + arena.direct.reallocDelegated;
            trims += arena.heap.trims + arena.direct.trims;
            trimmedBlocks += arena.heap.trimmedBlocks + arena.direct.trimmedBlocks;
            leaked += arena.heap.leakedBlocks + arena.direct.leakedBlocks;
            earlyReuses += arena.heap.earlyReuses + arena.direct.earlyReuses;
            blockReuses += arena.heap.blockReuses + arena.direct.blockReuses;
            blockGrowths += arena.heap.blockGrowths + arena.direct.blockGrowths;
            objects += arena.objectCount;
        }

        @Override
        public String toString() {
            long arena = arenaHeap + arenaDirect;
            long all = arena + delegateHeap + delegateDirect;
            return "arenaHeap=" + arenaHeap + " arenaDirect=" + arenaDirect
                    + " delegateHeap=" + delegateHeap + " delegateDirect=" + delegateDirect
                    + " arenaShare=" + (all == 0 ? "n/a" : String.format("%.2f%%", 100.0 * arena / all))
                    + " blocksHeap=" + blocksHeap + " blocksDirect=" + blocksDirect
                    + " pinned=" + pinned + " reusable=" + reusable
                    + " maxPinnedHeap=" + maxPinnedHeap + " maxPinnedDirect=" + maxPinnedDirect
                    + " blockReuses=" + blockReuses + " blockGrowths=" + blockGrowths
                    + " hooks=" + hooks + " hookRejections=" + hookRejections
                    + " violations=" + violations + " earlyReuses=" + earlyReuses
                    + " reallocInPlace=" + reallocInPlace + " reallocMoved=" + reallocMoved
                    + " reallocDelegated=" + reallocDelegated
                    + " trims=" + trims + " trimmedBlocks=" + trimmedBlocks + " leakedBlocks=" + leaked
                    + " objects=" + objects;
        }
    }

    /**
     * Every counter of design section 6, summed over every arena of every instance, plus one line per live
     * arena (per event loop). Called off the hot path - at shutdown, or by a benchmark's teardown - so it
     * reads other threads' plain longs: the values are a snapshot, not a linearizable total.
     */
    public static String counters() {
        StringBuilder sb = new StringBuilder(1024);
        Counters total = new Counters();
        synchronized (ARENAS) {
            total.add(DEAD);
            for (int i = 0; i < ARENAS.size(); i++) {
                Arena arena = ARENAS.get(i);
                total.add(arena);
                Counters one = new Counters();
                one.add(arena);
                sb.append("\nARENALOOP thread=").append(arena.owner.getName()).append(' ').append(one);
            }
        }
        return "ARENATELE blockSize=" + BLOCK_SIZE + " maxBlocks=" + MAX_BLOCKS + " cap=" + CAP
                + " maxObjects=" + MAX_OBJECTS + " debug=" + DEBUG + ' ' + total + sb;
    }
}
