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
 * design - see the metrics).
 * <p>
 * <b>Layout.</b> There is no block object. A block is an id in {@code [0, maxBlocks)} and a column of flat
 * per-space arrays - {@link Space#live}, {@link Space#mem}, {@link Space#base}, {@link Space#nio},
 * {@link Space#roots} - plus two {@code int} bit masks. The current block is plain fields of the space:
 * {@link Space#curId}, {@link Space#curBump}, {@link Space#curMemory}, {@link Space#curAddress}. A buffer
 * stores its block as an {@code int} id; allocation writes ints, one {@code long} address and - for a heap
 * buffer that lands in another block - one guarded {@code byte[]} store, which is the only reference store
 * on any hot path. Switching block is {@code numberOfTrailingZeros(reusableMask)}; the hook scans the live
 * ints, resets the current block's bump in place and rebuilds the mask. There is no {@code bump[]} column:
 * only the current block is ever bumped, and a block switched away from is never bumped again.
 * <p>
 * <b>No in-band metadata.</b> A block's memory holds user payload only. No allocation header, no block
 * header, no free-list link, no fill on retire, nothing written at reset. Every piece of bookkeeping lives
 * on the owner's own cache lines: the buffer objects, {@code int[] live}, the masks, the {@code int[]}
 * object free stack and the flat cursor. Nothing is ever derived from an address: the buffer carries its id.
 * <p>
 * The lifecycle has exactly one rule: <b>memory freed during an iteration is never handed out again before
 * the end-of-iteration hook</b>. Releasing a buffer only decrements its block's live count; at the hook
 * ({@code SingleThreadEventLoop.executeAfterEventLoopIteration}, armed from the allocation path once per
 * iteration) every block whose live count is zero becomes reusable. A block with live buffers at the hook is
 * pinned and is skipped until a later hook finds it empty. Nothing frees a block except {@link #trim()} and
 * thread termination.
 * <p>
 * <b>Invariant A (confinement).</b> Every field of an {@link ArenaBuf} and every block column is read and
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
    /** Upper bound on the per-space buffer-object array; past it, allocation delegates. */
    static final int MAX_OBJECTS = Integer.getInteger("arena.maxObjects", 16 * 1024);
    /** {@code -Darena.debug=true}: check that no block is reused before a hook. Folded away when false. */
    static final boolean DEBUG = Boolean.getBoolean("arena.debug");
    /** One {@code ArenaIteration} event every this many hooks, and one {@code ArenaAllocationSample}. */
    static final int JFR_PERIOD = Integer.getInteger("arena.jfr.period", 1000);

    static final String REASON_CAP = "CAP";
    static final String REASON_BOUND = "BOUND";
    static final String REASON_OBJECTS = "OBJECTS";
    static final String REASON_OFF_LOOP = "OFF_LOOP";
    private static final String NEXT_REUSE = "REUSE";
    private static final String NEXT_GROWTH = "GROWTH";
    private static final String NEXT_DELEGATE = "DELEGATE";

    /** Every slot of a full space: the block masks are {@code int}s, so {@code maxBlocks <= 32}. */
    private static final int FULL_MASK = MAX_BLOCKS == 32 ? -1 : (1 << MAX_BLOCKS) - 1;
    private static final int INITIAL_OBJECTS = Math.min(256, MAX_OBJECTS);
    /**
     * The DELEGATED state: the buffer wraps a buffer of the delegate allocator. It is a real column slot,
     * one past the last block, so that release stays branch-free on the block columns; its live count is
     * the number of delegated buffers in flight.
     */
    static final int DELEGATE_SLOT = MAX_BLOCKS;
    private static final int NO_BLOCK = -1;
    private static final boolean UNSAFE = PlatformDependent.hasUnsafe();

    static {
        if (((CAP + 7) & ~7) > BLOCK_SIZE || CAP < 0 || MAX_BLOCKS < 1 || MAX_BLOCKS > 32 || MAX_OBJECTS < 1) {
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
        space.delegated(initialCapacity);
        return delegate.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        Space space = directSpaces.get();
        ByteBuf buf = space.allocate(initialCapacity, maxCapacity);
        if (buf != null) {
            return buf;
        }
        space.delegated(initialCapacity);
        return delegate.directBuffer(initialCapacity, maxCapacity);
    }

    /** The reallocation path's delegate call; the allocation path calls the delegate in place. */
    private ByteBuf delegateForGrow(Space space, int capacity, int maxCapacity) {
        space.delegated(capacity);
        return space.direct ? delegate.directBuffer(capacity, maxCapacity)
                : delegate.heapBuffer(capacity, maxCapacity);
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

    /**
     * Close the current iteration on the calling thread, exactly as the event-loop hook does. On an event
     * loop nothing calls this: the hook is armed from the allocation path. It exists for drivers that are
     * not event loops and still have an iteration boundary - tests, and benchmarks that model a cycle.
     */
    public void endOfIteration() {
        Arena arena = arenas.getIfExists();
        if (arena != null) {
            arena.checkOwner();
            arena.hookNow();
        }
    }

    /** Run the end-of-iteration hook on the calling thread's arena. Package private: for tests only. */
    void runHookForTest() {
        endOfIteration();
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

    /**
     * The blocks of one memory kind (heap or direct) for one thread, as flat columns indexed by block id.
     * Every field here is written by the owning thread only.
     */
    static final class Space {
        final Arena arena;
        final AdaptivePoolingAllocator.ChunkAllocator chunkAllocator;
        final boolean direct;
        /** {@link #CAP}, or -1 when this space cannot serve anything (direct without unsafe). */
        final int cap;

        // --- block columns, indexed by block id ---
        /**
         * Allocations and releases per block. A block is empty iff {@code allocs[id] == frees[id]}; both
         * are zeroed when the block is reset, and their values are added to the arena's totals then - the
         * way a TLAB's statistics are accumulated when it is retired, never per object.
         */
        final int[] allocs = new int[MAX_BLOCKS + 1];
        final int[] frees = new int[MAX_BLOCKS + 1];
        /** Base address per block, 0 when the chunks of this space have none. */
        final long[] base = new long[MAX_BLOCKS + 1];
        /** Backing array per block, null for a direct space. */
        final byte[][] mem = new byte[MAX_BLOCKS + 1][];
        /** One NIO view of the whole block, the source the per-buffer views are duplicated from. */
        final ByteBuffer[] nio = new ByteBuffer[MAX_BLOCKS + 1];
        /** The chunk buffer per block: bulk access, and the memory to give back. */
        final AbstractByteBuf[] roots = new AbstractByteBuf[MAX_BLOCKS + 1];
        /** Debug only: the hook generation that last reset each block. */
        final long[] stamp = new long[MAX_BLOCKS + 1];
        /** Bit i: slot i holds a chunk. */
        int allocatedMask;
        /** Bit i: block i was empty at the LAST hook and its bump was reset. Zero means "grow or delegate". */
        int reusableMask;
        /**
         * Every block is pinned and the bound is reached: allocation delegates without touching anything
         * else. Set by {@link #switchBlock()}, cleared by the hook and by {@link #trim()}.
         */
        boolean exhausted;
        boolean hasAddress;
        /**
         * -1 when the chunks of this space have a usable memory address, 0 when they do not. It turns the
         * offset into the block into zero for a heap space, so that a heap buffer's address stays 0 and
         * {@code hasMemoryAddress()} is false - as {@link UnpooledHeapByteBuf}'s is. Without it a heap
         * buffer would report {@code start} as an absolute address and every unsafe copy into it would
         * write to whatever lives there.
         */
        int addressMask;

        // --- the current block, flat ---
        int curId;
        int curBump;
        byte[] curMemory;
        long curAddress;

        // --- the buffer objects of this space ---
        ArenaBuf[] objects = new ArenaBuf[INITIAL_OBJECTS];
        int[] free = new int[INITIAL_OBJECTS];
        int objectCount;
        int freeTop;

        /** Number of hooks that have run, i.e. the index of the current iteration. */
        long generation;
        IterationHook hook;        // null when this thread is not an event loop thread

        // Metrics (plain longs, owner-written). Nothing here is touched by the bump or release path:
        // allocations are counted in the block columns and accumulated when a block is reset, and bytes
        // are added from the cursor when a block is retired - as the JDK does with a TLAB.
        long retiredAllocations;
        long bytesBumped;
        // JFR bookkeeping, touched only where an event may be emitted.
        int jfrTick;
        long sampledBytes;
        long sampledAllocations;
        long reportedBytes;
        long reportedAllocations;
        long reportedDelegated;
        long reportedHooks;
        long delegateAllocations;
        long blockReuses;
        long blockGrowths;
        long objectGrowths;
        long reallocInPlace;
        long reallocMoved;
        long reallocDelegated;
        long trims;
        long trimmedBlocks;
        long leakedBlocks;
        long earlyReuses;
        int pinnedBlocks;
        int maxPinned;

        Space(Arena arena, AdaptivePoolingAllocator.ChunkAllocator chunkAllocator, boolean direct) {
            this.arena = arena;
            this.chunkAllocator = chunkAllocator;
            this.direct = direct;
            cap = direct && !UNSAFE ? -1 : CAP;
            if (cap >= 0) {
                newBlock();
            }
        }

        /**
         * THE hot path. It touches the space's own flat fields and the object stack, nothing else.
         * Everything rare - arming the hook, switching block, growing the object array, giving up - is a
         * call to a separate method on a branch that is not taken in the common case.
         */
        ByteBuf allocate(int size, int maxCapacity) {
            if (size > cap) {
                return null;                   // above the cap: the delegate owns it
            }
            if (exhausted) {
                return null;                   // every block pinned and the bound reached
            }
            IterationHook h = hook;
            if (h != null && !h.armed) {
                h.arm();                       // once per iteration: close it at the end of this one
            }
            int start = curBump;
            int end = start + ((size + 7) & ~7);
            if (end > BLOCK_SIZE) {
                return allocateInAnotherBlock(size, maxCapacity);
            }
            int top = freeTop - 1;
            if (top < 0) {
                return allocateWithNewObject(start, end, size, maxCapacity);
            }
            freeTop = top;
            curBump = end;
            allocs[curId]++;
            ArenaBuf buf = objects[free[top]];
            buf.init(curId, start, size, maxCapacity);
            return buf;
        }

        /** The current block is full: take one the LAST hook marked reusable, or grow, or give up. */
        private ByteBuf allocateInAnotherBlock(int size, int maxCapacity) {
            if (!switchBlock()) {
                return null;
            }
            int end = (size + 7) & ~7;
            int top = freeTop - 1;
            if (top < 0) {
                return allocateWithNewObject(0, end, size, maxCapacity);
            }
            freeTop = top;
            curBump = end;
            allocs[curId]++;
            ArenaBuf buf = objects[free[top]];
            buf.init(curId, 0, size, maxCapacity);
            return buf;
        }

        /** The object stack is empty: create one more buffer object, or give up. */
        private ByteBuf allocateWithNewObject(int start, int end, int size, int maxCapacity) {
            ArenaBuf buf = newObject();
            if (buf == null) {
                return null;                   // maxObjects reached: the delegate owns it
            }
            curBump = end;
            allocs[curId]++;
            buf.init(curId, start, size, maxCapacity);
            return buf;
        }

        /**
         * Make another block current: the lowest one the LAST hook marked reusable, else a new one. A zero
         * mask means "nothing was empty at the last hook", which is also the latch that stops the rescan.
         */
        private boolean switchBlock() {
            boolean record = PlatformDependent.isJfrEnabled() && ArenaEvents.BlockSwitch.isEventEnabled();
            int mask = reusableMask;
            if (record) {
                switchEvent(mask != 0 ? NEXT_REUSE
                        : Integer.bitCount(allocatedMask) < MAX_BLOCKS ? NEXT_GROWTH : NEXT_DELEGATE);
            }
            bytesBumped += curBump;            // the retired block's used bytes, counted once
            if (++jfrTick >= JFR_PERIOD && PlatformDependent.isJfrEnabled()
                    && ArenaEvents.AllocationSample.isEventEnabled()) {
                jfrTick = 0;
                sampleEvent();
            }
            if (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                if (DEBUG) {
                    checkNotReusedBeforeHook(id);
                }
                reusableMask = mask & (mask - 1);
                blockReuses++;
                makeCurrent(id);
                return true;
            }
            if (Integer.bitCount(allocatedMask) < MAX_BLOCKS) {
                blockGrowths++;
                newBlock();
                return true;
            }
            exhausted = true;
            return false;
        }

        /** One delegated allocation: a counter, and - when recording - one ArenaAllocationOutside event. */
        void delegated(int size) {
            delegateAllocations++;
            if (PlatformDependent.isJfrEnabled() && ArenaEvents.AllocationOutside.isEventEnabled()) {
                outsideEvent(size);
            }
        }

        private void outsideEvent(int size) {
            ArenaEvents.AllocationOutside event = new ArenaEvents.AllocationOutside();
            event.allocatorType = CycleArenaAllocator.class;
            event.size = size;
            event.space = direct ? "DIRECT" : "HEAP";
            event.reason = size > cap ? REASON_CAP
                    : hook == null ? REASON_OFF_LOOP
                    : exhausted ? REASON_BOUND : REASON_OBJECTS;
            event.commit();
        }

        /** One retired block: the analogue of jdk.ObjectAllocationInNewTLAB. */
        private void switchEvent(String next) {
            ArenaEvents.BlockSwitch event = new ArenaEvents.BlockSwitch();
            event.allocatorType = CycleArenaAllocator.class;
            event.bytesBumped = curBump;
            event.allocations = allocs[curId];
            event.liveAtRetire = allocs[curId] - frees[curId];
            event.blockId = curId;
            event.next = next;
            event.space = direct ? "DIRECT" : "HEAP";
            event.commit();
        }

        /** Sampled bump weight, taken only at a block switch or at a hook. */
        private void sampleEvent() {
            long bytes = bytesBumpedTotal();
            long all = arenaAllocations();
            ArenaEvents.AllocationSample event = new ArenaEvents.AllocationSample();
            event.allocatorType = CycleArenaAllocator.class;
            event.weight = bytes - sampledBytes;
            event.allocations = all - sampledAllocations;
            event.space = direct ? "DIRECT" : "HEAP";
            event.commit();
            sampledBytes = bytes;
            sampledAllocations = all;
        }

        private void iterationEvent(int blocksReset, int pinned) {
            long bytes = bytesBumpedTotal();
            long all = arenaAllocations();
            ArenaEvents.Iteration event = new ArenaEvents.Iteration();
            event.allocatorType = CycleArenaAllocator.class;
            event.blocksReset = blocksReset;
            event.pinned = pinned;
            event.bytesBumped = bytes - reportedBytes;
            event.allocations = all - reportedAllocations;
            event.delegated = delegateAllocations - reportedDelegated;
            event.hooks = arena.hooks - reportedHooks;
            event.space = direct ? "DIRECT" : "HEAP";
            event.commit();
            reportedBytes = bytes;
            reportedAllocations = all;
            reportedDelegated = delegateAllocations;
            reportedHooks = arena.hooks;
        }

        private void makeCurrent(int id) {
            curId = id;
            curBump = 0;                       // the hook reset it; nothing has been handed out since
            curMemory = mem[id];
            curAddress = base[id];
        }

        /** Take a free slot and give it a chunk. Cold: growth only. */
        private void newBlock() {
            int id = Integer.numberOfTrailingZeros(~allocatedMask & FULL_MASK);
            AbstractByteBuf root = chunkAllocator.allocate(BLOCK_SIZE, BLOCK_SIZE);
            roots[id] = root;
            mem[id] = root.hasArray() ? root.array() : null;
            hasAddress = root.hasMemoryAddress();
            addressMask = hasAddress ? -1 : 0;
            base[id] = hasAddress ? root.memoryAddress() : 0L;
            nio[id] = root.internalNioBuffer(0, BLOCK_SIZE).duplicate();
            allocs[id] = 0;
            frees[id] = 0;
            stamp[id] = generation - 1;        // fresh memory: never "reused before its hook"
            allocatedMask |= 1 << id;
            makeCurrent(id);
        }

        private void checkNotReusedBeforeHook(int id) {
            if (stamp[id] >= generation) {
                earlyReuses++;
                throw new IllegalStateException("arena block reused in the iteration that freed it: block="
                        + id + " stamp=" + stamp[id] + " generation=" + generation);
            }
        }

        /** Grow the object array, up to {@code maxObjects}; {@code null} means "delegate this one". */
        private ArenaBuf newObject() {
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
            ArenaBuf buf = new ArenaBuf(arena.alloc, this, count);
            objects[count] = buf;
            objectCount = count + 1;
            return buf;
        }

        /**
         * Reserve a region for a reallocation. Returns the block id, or -1; the offset is left in
         * {@link #reservedStart}. Cold path.
         */
        int reservedStart;

        int reserve(int size) {
            if (size > cap) {
                return NO_BLOCK;
            }
            int start = curBump;
            int end = start + ((size + 7) & ~7);
            if (end > BLOCK_SIZE) {
                if (!switchBlock()) {
                    return NO_BLOCK;
                }
                start = 0;
                end = (size + 7) & ~7;
            }
            curBump = end;
            allocs[curId]++;
            reservedStart = start;
            return curId;
        }

        /** End of an iteration: every empty block becomes reusable; the current block stays current. */
        void endOfIteration() {
            long gen = generation;
            int cur = curId;
            int mask = 0;
            int pinned = 0;
            for (int id = 0; id < MAX_BLOCKS; id++) {
                if ((allocatedMask & (1 << id)) == 0) {
                    continue;
                }
                int a = allocs[id];
                if (a == frees[id]) {
                    retiredAllocations += a;   // statistics accumulate when a block is reset, not per op
                    allocs[id] = 0;
                    frees[id] = 0;
                    if (DEBUG) {
                        stamp[id] = gen;
                    }
                    if (id == cur) {
                        bytesBumped += curBump;
                        curBump = 0;           // the current block restarts in place, and stays current
                    } else {
                        mask |= 1 << id;
                    }
                } else {
                    pinned++;
                }
            }
            reusableMask = mask;
            exhausted = false;
            generation = gen + 1;
            pinnedBlocks = pinned;
            if (pinned > maxPinned) {
                maxPinned = pinned;
            }
            if (++jfrTick >= JFR_PERIOD && PlatformDependent.isJfrEnabled()) {
                jfrTick = 0;
                if (ArenaEvents.Iteration.isEventEnabled()) {
                    iterationEvent(Integer.bitCount(mask), pinned);
                }
                if (ArenaEvents.AllocationSample.isEventEnabled()) {
                    sampleEvent();
                }
            }
        }

        /** Give back every reusable block but the first. The slot is freed, block ids never move. */
        void trim() {
            trims++;
            exhausted = false;
            int mask = reusableMask;
            if (mask == 0) {
                return;
            }
            mask &= mask - 1;                  // keep the first reusable block
            reusableMask &= ~mask;
            allocatedMask &= ~mask;
            while (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                mask &= mask - 1;
                trimmedBlocks++;
                dropBlock(id);
            }
        }

        /** Thread termination: empty blocks go back, pinned ones leak - no other thread may free them. */
        void terminate() {
            int mask = allocatedMask;
            while (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                mask &= mask - 1;
                if (allocs[id] == frees[id]) {
                    dropBlock(id);
                } else {
                    leakedBlocks++;
                }
            }
            allocatedMask = 0;
            reusableMask = 0;
            curMemory = null;
            curAddress = 0;
            hook = null;
        }

        private void dropBlock(int id) {
            AbstractByteBuf root = roots[id];
            roots[id] = null;
            mem[id] = null;
            nio[id] = null;
            base[id] = 0L;
            root.release();
        }

        /** Allocations served by this space: what the reset blocks accumulated plus what the live ones hold. */
        long arenaAllocations() {
            long total = retiredAllocations + allocs[DELEGATE_SLOT];
            int mask = allocatedMask;
            while (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                mask &= mask - 1;
                total += allocs[id];
            }
            return total;
        }

        /** Bytes bump-allocated: what the retired blocks used plus the current cursor. */
        long bytesBumpedTotal() {
            return bytesBumped + curBump;
        }

        int liveBuffers() {
            int live = allocs[DELEGATE_SLOT] - frees[DELEGATE_SLOT];
            int mask = allocatedMask;
            while (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                mask &= mask - 1;
                live += allocs[id] - frees[id];
            }
            return live;
        }

        int blockCount() {
            return Integer.bitCount(allocatedMask);
        }

        int reusableBlocks() {
            return Integer.bitCount(reusableMask);
        }
    }

    static final class Arena {
        final CycleArenaAllocator alloc;
        final Thread owner;
        final Space heap;
        final Space direct;
        IterationHook hook;

        // Metrics.
        long violations;           // written by the violating thread, read at shutdown: racy by construction
        long hooks;
        long hookRejections;

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

    /**
     * A buffer over a region of a block - or, in the DELEGATED state, over a buffer of the delegate
     * allocator. Element access goes straight to {@link #memory} + {@link #start} (heap) or to
     * {@link #address} (direct), never through the block columns.
     */
    static final class ArenaBuf extends AbstractByteBuf {
        private final ByteBufAllocator alloc;
        private final Space space;       // an object belongs to one space for its whole life
        private final Thread owner;      // arena.owner, inlined to keep the confinement check to one load
        final int index;                 // stable slot in the space's object array
        /** The block this buffer's region belongs to, or {@link #DELEGATE_SLOT} when DELEGATED. */
        private int blockId;
        /** Offset of this buffer's index 0 inside {@link #memory} (heap), and inside its block. */
        private int start;
        private int length;
        private int refCnt;
        /** The block's array (heap), or the delegate buffer's array; null for direct memory. */
        private byte[] memory;
        /** The address of this buffer's index 0 (direct), 0 when there is none. */
        private long address;
        /** DELEGATED state only: the buffer to release, leak-aware wrapper and all. */
        private ByteBuf delegated;
        /** DELEGATED state only: the same buffer, unwrapped, for the bulk and view paths. */
        private AbstractByteBuf delegateRoot;
        // The NIO view must be per buffer: several buffers of one block are asked for their view before any
        // of them is used (a gathering write), so a view shared per block would be handed out twice.
        private ByteBuffer tmpNioBuf;
        private int tmpNioBlock = NO_BLOCK;

        ArenaBuf(ByteBufAllocator alloc, Space space, int index) {
            super(0);
            this.alloc = alloc;
            this.space = space;
            this.owner = space.arena.owner;
            this.index = index;
        }

        /** The block id, or {@link #DELEGATE_SLOT} when DELEGATED. For tests only. */
        int blockIdForTest() {
            return blockId;
        }

        /**
         * Ints, one long, and - only when this buffer lands in another block - one guarded {@code byte[]}
         * store. That store is the single reference store on the allocation path and it is paid once per
         * block switch, not once per allocation.
         */
        void init(int blockId, int start, int length, int maxCapacity) {
            this.blockId = blockId;
            this.start = start;
            this.length = length;
            this.refCnt = 1;
            byte[] m = space.curMemory;
            if (memory != m) {
                memory = m;
            }
            address = space.curAddress + (space.addressMask & start);
            resetForReuse(maxCapacity);
        }

        /** The buffer that holds the bytes, for the bulk and view paths only. */
        private AbstractByteBuf root() {
            int id = blockId;
            return id == DELEGATE_SLOT ? delegateRoot : space.roots[id];
        }

        /** This buffer's index 0 inside {@link #root()}. */
        private int rootStart() {
            return blockId == DELEGATE_SLOT ? 0 : start;
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
                throw violation("RETAIN");
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
                throw violation("RETAIN");
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
                throw violation("RELEASE");
            }
            int cnt = refCnt;
            if (cnt <= 0) {
                throw new IllegalReferenceCountException(cnt, -1);
            }
            refCnt = cnt - 1;
            if (cnt != 1) {
                return false;
            }
            Space s = space;
            int id = blockId;
            s.frees[id]++;
            s.free[s.freeTop++] = index;
            if (id == DELEGATE_SLOT) {
                releaseDelegated();
            }
            return true;
        }

        @Override
        public boolean release(int decrement) {
            if (Thread.currentThread() != owner) {
                throw violation("RELEASE");
            }
            int cnt = refCnt;
            if (decrement <= 0 || cnt < decrement) {
                throw new IllegalReferenceCountException(cnt, -decrement);
            }
            refCnt = cnt - decrement;
            if (cnt != decrement) {
                return false;
            }
            Space s = space;
            int id = blockId;
            s.frees[id]++;
            s.free[s.freeTop++] = index;
            if (id == DELEGATE_SLOT) {
                releaseDelegated();
            }
            return true;
        }

        /** DELEGATED: the object is already back in the pool; the delegate buffer goes back too. */
        private void releaseDelegated() {
            ByteBuf buf = delegated;
            delegated = null;
            delegateRoot = null;
            tmpNioBuf = null;
            tmpNioBlock = NO_BLOCK;
            memory = null;
            address = 0;
            buf.release();
        }

        private IllegalStateException violation(String operation) {
            space.arena.violations++;
            if (PlatformDependent.isJfrEnabled() && ArenaEvents.ConfinementViolation.isEventEnabled()) {
                ArenaEvents.ConfinementViolation event = new ArenaEvents.ConfinementViolation();
                event.allocatorType = CycleArenaAllocator.class;
                event.owner = owner.getName();
                event.offender = Thread.currentThread().getName();
                event.operation = operation;
                event.commit();
            }
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
         * Grow: in place when this buffer is topmost in the CURRENT block and the block has the room, else a
         * new region (arena when it fits and there is room, else the delegate) plus a copy. The old region is
         * released - so it is handed out again only after the next hook, which keeps any view taken during
         * this iteration valid until the iteration ends.
         */
        private ByteBuf grow(int newCapacity) {
            int id = blockId;
            Space space = this.space;
            if (id == DELEGATE_SLOT) {
                delegated.capacity(newCapacity);   // DELEGATED: the delegate buffer grows itself
                length = newCapacity;
                adoptDelegate(delegated);          // its memory may have moved
                return this;
            }
            int newEnd = start + ((newCapacity + 7) & ~7);
            if (id == space.curId && space.curBump == start + ((length + 7) & ~7) && newEnd <= BLOCK_SIZE) {
                space.curBump = newEnd;
                length = newCapacity;
                space.reallocInPlace++;
                return this;
            }
            AbstractByteBuf oldRoot = space.roots[id];
            int oldStart = start;
            int oldLength = length;
            int newId = space.reserve(newCapacity);
            if (newId >= 0) {
                int newStart = space.reservedStart;
                space.roots[newId].setBytes(newStart, oldRoot, oldStart, oldLength);
                space.reallocMoved++;
                blockId = newId;
                start = newStart;
                length = newCapacity;
                memory = space.mem[newId];
                address = space.base[newId] + (space.addressMask & newStart);
                tmpNioBuf = null;
                tmpNioBlock = NO_BLOCK;
            } else {
                ByteBuf buf = space.arena.alloc.delegateForGrow(space, newCapacity, maxCapacity());
                AbstractByteBuf target = unwrapDelegate(buf);
                target.setBytes(0, oldRoot, oldStart, oldLength);
                space.reallocDelegated++;
                delegated = buf;
                delegateRoot = target;
                length = newCapacity;
                adoptDelegate(buf);            // counts the delegate slot itself
            }
            space.frees[id]++;
            return this;
        }

        /** Point the data fields at a delegate buffer's memory; the id becomes {@link #DELEGATE_SLOT}. */
        private void adoptDelegate(ByteBuf buf) {
            AbstractByteBuf target = delegateRoot;
            if (blockId != DELEGATE_SLOT) {
                space.allocs[DELEGATE_SLOT]++;
            }
            blockId = DELEGATE_SLOT;
            memory = target.hasArray() ? target.array() : null;
            start = memory != null ? target.arrayOffset() : 0;
            address = target.hasMemoryAddress() ? target.memoryAddress() : 0L;
            tmpNioBuf = null;
            tmpNioBlock = NO_BLOCK;
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
            return space.direct;
        }

        @Override
        boolean _isDirect() {
            return space.direct;
        }

        @Override
        public boolean hasArray() {
            return memory != null;
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            if (memory == null) {
                throw new UnsupportedOperationException("direct buffer");
            }
            return memory;
        }

        @Override
        public int arrayOffset() {
            return start;
        }

        @Override
        public boolean hasMemoryAddress() {
            return address != 0;
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return address;
        }

        @Override
        long _memoryAddress() {
            return address;
        }

        @Override
        public int nioBufferCount() {
            return 1;
        }

        @Override
        public ByteBuffer nioBuffer(int index, int len) {
            checkIndex(index, len);
            return root().nioBuffer(rootStart() + index, len);   // a fresh view per call
        }

        @Override
        public ByteBuffer internalNioBuffer(int index, int len) {
            checkIndex(index, len);
            int id = blockId;
            if (id == DELEGATE_SLOT) {
                return delegateRoot.internalNioBuffer(index, len);   // the delegate has its own
            }
            ByteBuffer buf = tmpNioBuf;
            if (buf == null || tmpNioBlock != id) {
                buf = space.nio[id].duplicate();
                tmpNioBuf = buf;
                tmpNioBlock = id;
            }
            buf.clear().position(start + index).limit(start + index + len);
            return buf;
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int len) {
            return new ByteBuffer[] { nioBuffer(index, len) };
        }

        @Override
        public ByteBuf copy(int index, int len) {
            checkIndex(index, len);
            return root().copy(rootStart() + index, len);
        }

        // --- element access: straight to the array or to the address, no block column is read ---
        @Override protected byte _getByte(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getByte(m, start + i) : HeapByteBufUtil.getByte(m, start + i)) : UnsafeByteBufUtil.getByte(address + i); }
        @Override protected short _getShort(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getShort(m, start + i) : HeapByteBufUtil.getShort(m, start + i)) : UnsafeByteBufUtil.getShort(address + i); }
        @Override protected short _getShortLE(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getShortLE(m, start + i) : HeapByteBufUtil.getShortLE(m, start + i)) : UnsafeByteBufUtil.getShortLE(address + i); }
        @Override protected int _getUnsignedMedium(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getUnsignedMedium(m, start + i) : HeapByteBufUtil.getUnsignedMedium(m, start + i)) : UnsafeByteBufUtil.getUnsignedMedium(address + i); }
        @Override protected int _getUnsignedMediumLE(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getUnsignedMediumLE(m, start + i) : HeapByteBufUtil.getUnsignedMediumLE(m, start + i)) : UnsafeByteBufUtil.getUnsignedMediumLE(address + i); }
        @Override protected int _getInt(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getInt(m, start + i) : HeapByteBufUtil.getInt(m, start + i)) : UnsafeByteBufUtil.getInt(address + i); }
        @Override protected int _getIntLE(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getIntLE(m, start + i) : HeapByteBufUtil.getIntLE(m, start + i)) : UnsafeByteBufUtil.getIntLE(address + i); }
        @Override protected long _getLong(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getLong(m, start + i) : HeapByteBufUtil.getLong(m, start + i)) : UnsafeByteBufUtil.getLong(address + i); }
        @Override protected long _getLongLE(int i) { byte[] m = memory; return m != null ? (UNSAFE ? UnsafeByteBufUtil.getLongLE(m, start + i) : HeapByteBufUtil.getLongLE(m, start + i)) : UnsafeByteBufUtil.getLongLE(address + i); }
        @Override protected void _setByte(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setByte(m, start + i, v); } else { HeapByteBufUtil.setByte(m, start + i, v); } } else { UnsafeByteBufUtil.setByte(address + i, v); } }
        @Override protected void _setShort(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setShort(m, start + i, v); } else { HeapByteBufUtil.setShort(m, start + i, v); } } else { UnsafeByteBufUtil.setShort(address + i, v); } }
        @Override protected void _setShortLE(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setShortLE(m, start + i, v); } else { HeapByteBufUtil.setShortLE(m, start + i, v); } } else { UnsafeByteBufUtil.setShortLE(address + i, v); } }
        @Override protected void _setMedium(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setMedium(m, start + i, v); } else { HeapByteBufUtil.setMedium(m, start + i, v); } } else { UnsafeByteBufUtil.setMedium(address + i, v); } }
        @Override protected void _setMediumLE(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setMediumLE(m, start + i, v); } else { HeapByteBufUtil.setMediumLE(m, start + i, v); } } else { UnsafeByteBufUtil.setMediumLE(address + i, v); } }
        @Override protected void _setInt(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setInt(m, start + i, v); } else { HeapByteBufUtil.setInt(m, start + i, v); } } else { UnsafeByteBufUtil.setInt(address + i, v); } }
        @Override protected void _setIntLE(int i, int v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setIntLE(m, start + i, v); } else { HeapByteBufUtil.setIntLE(m, start + i, v); } } else { UnsafeByteBufUtil.setIntLE(address + i, v); } }
        @Override protected void _setLong(int i, long v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setLong(m, start + i, v); } else { HeapByteBufUtil.setLong(m, start + i, v); } } else { UnsafeByteBufUtil.setLong(address + i, v); } }
        @Override protected void _setLongLE(int i, long v) { byte[] m = memory; if (m != null) { if (UNSAFE) { UnsafeByteBufUtil.setLongLE(m, start + i, v); } else { HeapByteBufUtil.setLongLE(m, start + i, v); } } else { UnsafeByteBufUtil.setLongLE(address + i, v); } }

        // --- bulk access, through the block's chunk (or the delegate buffer) ---
        @Override public ByteBuf getBytes(int i, ByteBuf dst, int di, int len) { checkIndex(i, len); root().getBytes(rootStart() + i, dst, di, len); return this; }
        @Override public ByteBuf getBytes(int i, byte[] dst, int di, int len) { checkIndex(i, len); root().getBytes(rootStart() + i, dst, di, len); return this; }
        @Override public ByteBuf getBytes(int i, ByteBuffer dst) { checkIndex(i, dst.remaining()); root().getBytes(rootStart() + i, dst); return this; }
        @Override public ByteBuf getBytes(int i, OutputStream out, int len) throws IOException { checkIndex(i, len); root().getBytes(rootStart() + i, out, len); return this; }
        @Override public int getBytes(int i, GatheringByteChannel out, int len) throws IOException { checkIndex(i, len); return root().getBytes(rootStart() + i, out, len); }
        @Override public int getBytes(int i, FileChannel out, long pos, int len) throws IOException { checkIndex(i, len); return root().getBytes(rootStart() + i, out, pos, len); }
        @Override public ByteBuf setBytes(int i, ByteBuf src, int si, int len) { checkIndex(i, len); root().setBytes(rootStart() + i, src, si, len); return this; }
        @Override public ByteBuf setBytes(int i, byte[] src, int si, int len) { checkIndex(i, len); root().setBytes(rootStart() + i, src, si, len); return this; }
        @Override public ByteBuf setBytes(int i, ByteBuffer src) { checkIndex(i, src.remaining()); root().setBytes(rootStart() + i, src); return this; }
        @Override public int setBytes(int i, InputStream in, int len) throws IOException { checkIndex(i, len); return root().setBytes(rootStart() + i, in, len); }
        @Override public int setBytes(int i, ScatteringByteChannel in, int len) throws IOException { checkIndex(i, len); return root().setBytes(rootStart() + i, in, len); }
        @Override public int setBytes(int i, FileChannel in, long pos, int len) throws IOException { checkIndex(i, len); return root().setBytes(rootStart() + i, in, pos, len); }
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
        long bytesHeap;
        long bytesDirect;
        long liveHeap;
        long liveDirect;

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
            bytesHeap += other.bytesHeap;
            bytesDirect += other.bytesDirect;
            liveHeap += other.liveHeap;
            liveDirect += other.liveDirect;
        }

        void add(Arena arena) {
            arenaHeap += arena.heap.arenaAllocations();
            arenaDirect += arena.direct.arenaAllocations();
            bytesHeap += arena.heap.bytesBumpedTotal();
            bytesDirect += arena.direct.bytesBumpedTotal();
            liveHeap += arena.heap.liveBuffers();
            liveDirect += arena.direct.liveBuffers();
            delegateHeap += arena.heap.delegateAllocations;
            delegateDirect += arena.direct.delegateAllocations;
            blocksHeap += arena.heap.blockCount();
            blocksDirect += arena.direct.blockCount();
            pinned += arena.heap.pinnedBlocks + arena.direct.pinnedBlocks;
            reusable += arena.heap.reusableBlocks() + arena.direct.reusableBlocks();
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
            objects += arena.heap.objectCount + arena.direct.objectCount;
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
                    + " objects=" + objects
                    + " bytesHeap=" + bytesHeap + " bytesDirect=" + bytesDirect
                    + " liveHeap=" + liveHeap + " liveDirect=" + liveDirect;
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
