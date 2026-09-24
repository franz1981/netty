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

import io.netty.util.ByteProcessor;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * per-space arrays - {@link Space#allocs}, {@link Space#frees}, {@link Space#roots} - plus two {@code int}
 * bit masks. The current block is plain fields of the space: {@link Space#curId}, {@link Space#curBump} and
 * {@link Space#curRoot}, the chunk buffer that backs it. A buffer stores its block as an {@code int} id and
 * reaches its bytes the way {@link AdaptivePoolingAllocator.AdaptiveByteBuf} does - through that root parent
 * and an offset; allocation writes ints and - for a buffer that lands in another block - one guarded
 * reference store, which is the only reference store on any hot path. Switching block is
 * {@code numberOfTrailingZeros(reusableMask)}; the hook scans the live
 * ints, resets the current block's bump in place and rebuilds the mask. There is no {@code bump[]} column:
 * only the current block is ever bumped, and a block switched away from is never bumped again.
 * <p>
 * <b>No in-band metadata.</b> A block's memory holds user payload only. No allocation header, no block
 * header, no free-list link, no fill on retire, nothing written at reset. Every piece of bookkeeping lives
 * on the owner's own cache lines: the buffer objects, {@code int[] live}, the masks, the {@code int[]}
 * object free stack and the flat cursor. Nothing is ever derived from an address: the buffer carries its id.
 * <p>
 * Without the ring (the default) the lifecycle has exactly one rule: <b>memory freed during
 * an iteration is never handed out again before the end-of-iteration hook</b>. Releasing a buffer only
 * decrements its block's live count; at the hook
 * ({@code SingleThreadEventLoop.executeAfterEventLoopIteration}, armed from the allocation path once per
 * iteration) every block whose live count is zero becomes reusable. A block with live buffers at the hook is
 * pinned and is skipped until a later hook finds it empty. Nothing frees a block except {@link #trim()} and
 * thread termination.
 * <p>
 * <b>The ring ({@code -Darena.ring=true}; off by default, see {@link #RING}).</b> A block is used as a ring
 * with variable-sized slots, the way a GPU upload ring is: the tail bumps forward, and it is allowed to
 * wrap into the bytes the head has already given back. This WEAKENS the rule above to adaptive's own
 * guarantee - <b>released memory may be handed out again before the end of the iteration</b>, but never
 * while it is live. A view taken on a buffer and used after that buffer was released may therefore read
 * someone else's bytes. The default, {@code -Darena.ring=false}, is the strict "reuse only at the hook"
 * behaviour.
 * <p>
 * The ring needs to know where the live buffers of a block are. It keeps, per block and out of band, one bit
 * per 8-byte slot - {@link Space#startBits}, {@code BLOCK_SIZE/8/64} longs - set at the START of every live
 * buffer: allocation ORs one bit in, release ANDs it out, and nothing else is maintained. The tail is
 * {@link Space#curBump} and the wall in front of it is {@link Space#curLimit}, so the hot path compares a
 * field instead of a constant. When the tail hits the wall (the cold path, {@link Space#advanceRing(int)}):
 * <ul>
 *   <li>not wrapped ({@code curLimit == BLOCK_SIZE}): {@code head} = the lowest live start in the block.
 *       No live start at all means the block is empty - the tail restarts at 0 in place
 *       ({@code ringResets}). Otherwise, if {@code head} leaves room for the request, the tail wraps:
 *       {@code curBump = 0, curLimit = head} ({@code ringWraps}). Otherwise the space switches block.</li>
 *   <li>wrapped ({@code curLimit < BLOCK_SIZE}): the buffers above the wall may have died since, so
 *       {@code newHead} = the lowest live start at or above the tail; the wall moves out to it (or to
 *       {@code BLOCK_SIZE} when nothing is live up there). If the request still does not fit, the space
 *       switches block.</li>
 * </ul>
 * <b>Re-entry.</b> When the ring stalls and no block is EMPTY, the space does not grow or delegate until it
 * has looked at the OTHER blocks: a pinned block is not a full one - it still holds the window the ring was
 * left at ({@code tailBump..tailLimit}) and whatever has died below its head since. The block with the
 * largest such window is made current again at that window ({@code ringReentries}). The scan is cold and
 * latched on {@code frees[]}: a block nothing has been released from is never rescanned.
 * <p>
 * <b>Invariant B (the ring never hands out live bytes).</b> {@code [curBump, curLimit)} holds no live
 * buffer. Proof. A live buffer that STARTS in that region would have its start bit set in it, and both
 * branches above put the wall at the lowest live start at or above the tail - so there is none. A live
 * buffer that starts BELOW the tail ends at or before the tail: every buffer of the current pass was handed
 * out by bumping, so its end is a value the tail has already taken, and the tail only moves forward within a
 * pass; the in-place growth path is the only other way to move the tail and it refuses to grow past
 * {@code curLimit}. A live buffer that starts at or above the wall is outside the region by construction. A
 * wrap resets the tail to 0 with the wall at the lowest live start of the whole block, so no live buffer
 * starts - and therefore none lies - in {@code [0, head)}. QED.
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
    /**
     * {@code -Darena.ring=true}: reuse inside a block, as a ring. OFF by default, on measured cost: the two
     * bitmap stores and the {@code curLimit} load are 29 x86 instructions per allocate/release pair direct
     * and 9 heap on this Zen 4 box (perfnorm, cycle k=64 FIFO SMALL, 2300 MHz: 12783 -> 14648 and
     * 14559 -> 15107 instructions per 64 pairs), against a budget of ~6. Two cheaper layouts were measured
     * and are NOT here because neither is cheaper: ONE flat {@code long[]} for the space, indexed
     * {@code (id << log2(BLOCK_LONGS)) + (start >>> 9)}, costs +36.0/+17.5 per pair, and that flat array with
     * {@code PlatformDependent}'s unchecked {@code long[]} access costs +29.1/+17.5 - the unchecked form
     * buys back exactly the bounds-check pair on the direct path (6.9 instructions, so 3.5 per bit) and
     * nothing at all on the heap path, and it never beats the row below.
     * It is worth turning on where no hook ever runs - that is the one setting in which a
     * block is otherwise never reused at all. A {@code static final} read once, so every ring branch and
     * every bitmap store folds away when it is false.
     */
    static final boolean RING = Boolean.getBoolean("arena.ring");
    /** Longs in a block's live-start bitmap: one bit per 8-byte slot. */
    static final int BLOCK_LONGS = BLOCK_SIZE >>> 9;
    /**
     * {@code -Darena.ringStats=true}: at every ring stall, measure what the ring leaves behind - the
     * stranded bytes and the sizes of the holes between the live buffers. It costs one walk of the space's
     * buffer objects plus a sort per stall, so it is OFF by default and every ring-stats field folds away.
     * A run made to report these numbers is NOT a run whose ns/op may be compared with a run without them.
     */
    static final boolean RING_STATS = Boolean.getBoolean("arena.ringStats");
    /** One {@code ArenaIteration} event every this many hooks, and one {@code ArenaAllocationSample}. */
    static final int JFR_PERIOD = Integer.getInteger("arena.jfr.period", 1000);

    /**
     * {@code -Darena.debugPinned=true}: attribute the blocks that a hook finds PINNED. Every arena
     * allocation then captures its own allocation stack ({@code new Throwable().getStackTrace()}) and the
     * hook generation it was made in; every {@link #DEBUG_PINNED_PERIOD}-th hook that finds at least one
     * pinned block walks the space's buffer objects and charges each pinned block to the allocation stack
     * of the OLDEST buffer still live in it. Reported by {@link #pinnedSites()}.
     * <p>
     * This is a diagnostic mode and it is expensive: one stack capture per allocation. Cap the captured
     * depth with {@code -XX:MaxJavaStackTraceDepth=<n>} and compare a run that sets it only with another
     * run that sets it. Every field and branch below folds away when the flag is false.
     */
    static final boolean DEBUG_PINNED = Boolean.getBoolean("arena.debugPinned");
    /** Hooks between two pinned-block attribution walks. Only hooks that find a pinned block count. */
    static final int DEBUG_PINNED_PERIOD = Integer.getInteger("arena.debugPinned.period", 64);
    /** Frames kept per attributed stack, after the allocator's own {@code io.netty.buffer} frames. */
    static final int DEBUG_PINNED_FRAMES = Integer.getInteger("arena.debugPinned.frames", 16);
    /** Pinned-block attribution of arenas whose thread is gone. Key: the formatted allocation stack. */
    private static final Map<String, long[]> DEAD_SITES = new HashMap<String, long[]>();

    /**
     * The bytes a buffer occupies: its size rounded up to an 8-byte slot, and NEVER zero. The ring's
     * live-start bitmap has one bit per slot, so two buffers must never share a start - a zero-length
     * buffer handed out at the tail would set, and on release clear, the bit of the buffer that came
     * after it, and the ring would then hand out live bytes. It also keeps {@code start < BLOCK_SIZE}:
     * a zero-length request at {@code curBump == curLimit == BLOCK_SIZE} passes the
     * {@code end > curLimit} test and indexes the bitmap one word past its end.
     */
    static int slotBytes(int size) {
        return Math.max(8, (size + 7) & ~7);
    }

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
        if (((CAP + 7) & ~7) > BLOCK_SIZE || CAP < 0 || MAX_BLOCKS < 1 || MAX_BLOCKS > 32 || MAX_OBJECTS < 1
                || (RING && (BLOCK_SIZE & 511) != 0)) {
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
        /** The chunk buffer per block: the root parent of every buffer of the block, and the memory to
         * give back. */
        final AbstractByteBuf[] roots = new AbstractByteBuf[MAX_BLOCKS + 1];
        /** Debug only: the hook generation that last reset each block. */
        final long[] stamp = new long[MAX_BLOCKS + 1];
        /**
         * RING re-entry. A block switched away from still has free bytes - the tail region the ring left
         * behind, and whatever has died at the bottom since. {@link #tailBump}/{@link #tailLimit} are the
         * ring's tail and wall at the moment the block stopped being current; {@link #freeBase}/
         * {@link #freeLimit} are the best free window found for it and {@link #freeHint} its size, all
         * computed by {@link #refreshHint(int)} on the COLD path only. {@link #hintFrees} is the latch:
         * {@code frees[id]} when the hint was taken, so a block whose occupancy has not changed is never
         * rescanned - -1 forces the next scan.
         */
        final int[] tailBump = RING ? new int[MAX_BLOCKS + 1] : null;
        final int[] tailLimit = RING ? new int[MAX_BLOCKS + 1] : null;
        final int[] freeBase = RING ? new int[MAX_BLOCKS + 1] : null;
        final int[] freeLimit = RING ? new int[MAX_BLOCKS + 1] : null;
        final int[] freeHint = RING ? new int[MAX_BLOCKS + 1] : null;
        final int[] hintFrees = RING ? new int[MAX_BLOCKS + 1] : null;
        /**
         * RING only: per block, one bit per 8-byte slot, set at the START of every live buffer. A row per
         * block, with the CURRENT row cached in {@link #curBits}: that cache is why allocation pays one
         * field load and one bounds check for a bit, and it is why ONE flat array measured WORSE - the
         * flat form has to add a base to every index and load the array as well, costing allocation more
         * than it saves release (+35.8 instructions per allocate/release pair direct against +29.1 here).
         * The {@link #DELEGATE_SLOT} column is a one-long scratch: a DELEGATED buffer's start is always 0,
         * so release can clear its bit without a branch.
         */
        final long[][] startBits = RING ? new long[MAX_BLOCKS + 1][] : null;
        /** Bit i: slot i holds a chunk. */
        int allocatedMask;
        /** Bit i: block i was empty at the LAST hook and its bump was reset. Zero means "grow or delegate". */
        int reusableMask;
        /**
         * Every block is pinned and the bound is reached: allocation delegates without touching anything
         * else. Set by {@link #switchBlock(int)}, cleared by the hook and by {@link #trim()}.
         */
        boolean exhausted;
        /**
         * RING: the space's total {@code frees[]} when the ring last gave up. A failed pass can only become
         * a winning one once SOME buffer of the space has gone - of the current block for a wrap, of any
         * block for a re-entry - so this is the exact latch that stops the rescan, and the only thing that
         * un-latches {@link #exhausted} when no hook ever runs.
         */
        int ringGiveUpFrees;

        // --- the current block, flat ---
        int curId;
        int curBump;
        /**
         * RING: the wall in front of the tail - {@code BLOCK_SIZE} when the ring has not wrapped, the lowest
         * live start above the tail when it has. {@code [curBump, curLimit)} is free (Invariant B).
         */
        int curLimit = BLOCK_SIZE;
        /** RING: {@code startBits[curId]}, so the allocation path reads no column to set a start bit. */
        long[] curBits;
        /**
         * Where the current pass over the current block started. Zero except after a re-entry, which
         * resumes at a tail the block kept; {@code curBump - curPassStart} is what THIS pass bump-allocated
         * and is what {@link #bytesBumped} takes when the pass ends, so re-entry cannot count bytes twice.
         */
        int curPassStart;
        /** {@code roots[curId]}: allocation reads no column at all to give a buffer its root parent. */
        AbstractByteBuf curRoot;

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
        long blockSwitches;
        long ringWraps;
        long ringResets;
        long ringScans;
        long ringReentries;
        // RING_STATS only: what a stalled ring leaves behind. See ringStall().
        long ringStalls;
        long stallBytes;
        long strandedBytes;
        long holes256;
        long holes1k;
        long holes4k;
        long holes8k;
        long holesBig;
        long[] statScratch;
        int pinnedBlocks;
        int maxPinned;
        // DEBUG_PINNED only: the attribution of the blocks a hook found pinned. Owner-written, read at
        // shutdown - racy by construction, exactly as the plain long counters above are.
        int pinnedTick;
        long pinnedSamples;        // pinned blocks looked at
        long pinnedUnattributed;   // pinned blocks with no live buffer object carrying a stack
        Map<String, long[]> pinnedSites;

        Space(Arena arena, AdaptivePoolingAllocator.ChunkAllocator chunkAllocator, boolean direct) {
            this.arena = arena;
            this.chunkAllocator = chunkAllocator;
            this.direct = direct;
            cap = direct && !UNSAFE ? -1 : CAP;
            if (RING) {
                startBits[DELEGATE_SLOT] = new long[1];
            }
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
            if (exhausted && !ringRetry()) {
                return null;                   // every block pinned and the bound reached
            }
            IterationHook h = hook;
            if (h != null && !h.armed) {
                h.arm();                       // once per iteration: close it at the end of this one
            }
            int start = curBump;
            int end = start + slotBytes(size);
            if (end > (RING ? curLimit : BLOCK_SIZE)) {
                return allocateInAnotherBlock(size, maxCapacity);
            }
            int top = freeTop - 1;
            if (top < 0) {
                return allocateWithNewObject(start, end, size, maxCapacity);
            }
            freeTop = top;
            curBump = end;
            allocs[curId]++;
            if (RING) {
                curBits[start >>> 9] |= 1L << (start >>> 3);     // this buffer is live at `start`
            }
            ArenaBuf buf = objects[free[top]];
            buf.init(curId, start, size, maxCapacity);
            return buf;
        }

        /**
         * The tail hit the wall: move the wall (the ring), else take a block the LAST hook marked reusable,
         * else grow, else give up.
         */
        private ByteBuf allocateInAnotherBlock(int size, int maxCapacity) {
            int need = slotBytes(size);
            if (!(RING && advanceRing(need)) && !switchBlock(need)) {
                return null;
            }
            int start = curBump;
            int end = start + need;
            int top = freeTop - 1;
            if (top < 0) {
                return allocateWithNewObject(start, end, size, maxCapacity);
            }
            freeTop = top;
            curBump = end;
            allocs[curId]++;
            if (RING) {
                curBits[start >>> 9] |= 1L << (start >>> 3);
            }
            ArenaBuf buf = objects[free[top]];
            buf.init(curId, start, size, maxCapacity);
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
            if (RING) {
                curBits[start >>> 9] |= 1L << (start >>> 3);
            }
            buf.init(curId, start, size, maxCapacity);
            return buf;
        }

        /**
         * THE ring. The tail cannot serve {@code need} bytes before {@link #curLimit}: move the wall, and
         * say whether the request fits afterwards. Cold - one call per pass over the block, not per
         * allocation. See Invariant B in the class javadoc for why the region it opens holds no live byte.
         */
        private boolean advanceRing(int need) {
            if (curLimit == BLOCK_SIZE) {
                // Not wrapped: the tail is at the top of the block. Where is the lowest live buffer?
                int head = lowestLiveStart(0);
                if (head < 0) {
                    bytesBumped += curBump - curPassStart;   // nothing is live: it restarts in place
                    curBump = 0;
                    curPassStart = 0;
                    curLimit = BLOCK_SIZE;
                    ringResets++;
                    return true;               // need <= align8(cap) <= BLOCK_SIZE, checked at class init
                }
                if (head < need) {
                    if (RING_STATS) {
                        ringStall(head);       // the ring gives up: the space will switch block
                    }
                    return false;              // the bottom of the block is pinned: switch block
                }
                if (RING_STATS) {
                    ringStall(head);           // measure BEFORE the tail moves: it is the ring's head
                }
                bytesBumped += curBump - curPassStart;   // this pass's bytes, counted once
                curBump = 0;
                curPassStart = 0;
                curLimit = head;
                ringWraps++;
                return true;
            }
            // Wrapped: the buffers between the wall and the top of the block may have died since.
            int head = lowestLiveStart(curBump);
            int limit = head < 0 ? BLOCK_SIZE : head;
            if (limit == curLimit) {
                if (RING_STATS) {
                    ringStall(head);
                }
                return false;                  // nothing died up there: switch block
            }
            curLimit = limit;
            if (curBump + need <= limit) {
                return true;
            }
            if (RING_STATS) {
                ringStall(head);               // the wall moved, but not far enough: switch block
            }
            return false;
        }

        /**
         * RING_STATS only. The ring's OCCUPIED region runs from the head - the lowest live start at or
         * above the tail, taken cyclically - forward to the tail; the free region is the rest. Everything
         * in the occupied region that is not a live buffer is STRANDED: dead bytes the ring cannot reach
         * without a free list. This records the stranded bytes, the block bytes they are a fraction of,
         * and a histogram of the individual holes - the numbers that say whether a first-fit over the
         * bitmap would be worth building. Cold: one walk of the buffer objects and one sort per stall.
         */
        private void ringStall(int head) {
            ringStalls++;
            stallBytes += BLOCK_SIZE;
            if (head < 0) {
                return;                        // nothing is live: nothing is stranded
            }
            int tail = curBump;
            int n = liveExtents();
            long[] live = statScratch;
            strandedBytes += tail > head ? holesIn(live, n, head, tail)
                    : holesIn(live, n, head, BLOCK_SIZE) + holesIn(live, n, 0, tail);
        }

        /** The live buffers of the current block as {@code (start << 32) | end}, sorted by start. */
        private int liveExtents() {
            int id = curId;
            int count = objectCount;
            long[] out = statScratch;
            if (out == null || out.length < count) {
                statScratch = out = new long[Math.max(64, count)];
            }
            int n = 0;
            for (int i = 0; i < count; i++) {
                ArenaBuf buf = objects[i];
                if (buf.refCnt > 0 && buf.blockId == id) {
                    int start = buf.start;
                    out[n++] = ((long) start << 32) | (start + slotBytes(buf.length));
                }
            }
            java.util.Arrays.sort(out, 0, n);
            return n;
        }

        /** Sum the holes of {@code [lo, hi)} and bucket each one; the live extents are sorted by start. */
        private int holesIn(long[] live, int n, int lo, int hi) {
            int stranded = 0;
            int cursor = lo;
            for (int i = 0; i < n; i++) {
                int start = (int) (live[i] >>> 32);
                if (start < lo) {
                    continue;
                }
                if (start >= hi) {
                    break;
                }
                if (start > cursor) {
                    stranded += start - cursor;
                    bucketHole(start - cursor);
                }
                int end = (int) live[i];
                if (end > cursor) {
                    cursor = end;
                }
            }
            if (hi > cursor) {
                stranded += hi - cursor;
                bucketHole(hi - cursor);
            }
            return stranded;
        }

        private void bucketHole(int hole) {
            if (hole <= 256) {
                holes256++;
            } else if (hole <= 1024) {
                holes1k++;
            } else if (hole <= 4096) {
                holes4k++;
            } else if (hole <= 8192) {
                holes8k++;
            } else {
                holesBig++;
            }
        }

        /**
         * The lowest live buffer start at or above {@code from}, or -1 when there is none. One bit per
         * 8-byte slot: word {@code w} holds the starts of {@code [w*512, w*512+512)}.
         */
        private int lowestLiveStart(int from) {
            ringScans++;
            return lowestLiveStart(curBits, from);
        }

        /** {@link #lowestLiveStart(int)} over any block's row of the bitmap. */
        private static int lowestLiveStart(long[] bits, int from) {
            int w = from >>> 9;
            if (w >= bits.length) {
                return -1;
            }
            long word = bits[w] & (-1L << (from >>> 3));         // mask the slots below `from` away
            for (;;) {
                if (word != 0) {
                    return (w << 9) | (Long.numberOfTrailingZeros(word) << 3);
                }
                if (++w == bits.length) {
                    return -1;
                }
                word = bits[w];
            }
        }

        /**
         * Make another block current: the lowest one the LAST hook marked reusable, else a new one. A zero
         * mask means "nothing was empty at the last hook", which is also the latch that stops the rescan.
         */
        private boolean switchBlock(int need) {
            boolean record = PlatformDependent.isJfrEnabled() && ArenaEvents.BlockSwitch.isEventEnabled();
            blockSwitches++;
            int mask = reusableMask;
            if (record) {
                switchEvent(mask != 0 ? NEXT_REUSE
                        : Integer.bitCount(allocatedMask) < MAX_BLOCKS ? NEXT_GROWTH : NEXT_DELEGATE);
            }
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
                leaveCurrent();
                reusableMask = mask & (mask - 1);
                blockReuses++;
                makeCurrent(id);
                return true;
            }
            if (RING && reenter(need)) {       // no block is EMPTY: is one of them re-enterable?
                return true;
            }
            leaveCurrent();
            if (Integer.bitCount(allocatedMask) < MAX_BLOCKS) {
                blockGrowths++;
                newBlock();
                return true;
            }
            exhausted = true;
            if (RING) {
                ringGiveUpFrees = spaceFrees();
            }
            return false;
        }

        /**
         * Stop bumping the current block: bank what THIS pass used, remember where its ring stood so a
         * later {@link #reenter(int)} can resume there, and drop its hint.
         */
        private void leaveCurrent() {
            bytesBumped += curBump - curPassStart;   // the retired pass's used bytes, counted once
            if (RING) {
                int id = curId;
                tailBump[id] = curBump;
                tailLimit[id] = curLimit;
                hintFrees[id] = -1;            // frees[] is never negative: this forces the next scan
            }
        }

        /**
         * RING re-entry. Every block is pinned, so none is EMPTY - but a pinned block is not a FULL block:
         * it still has the window the ring left at its tail, and whatever has died below its head since.
         * Take the block with the largest such window, if the request fits in it. Cold: one pass over the
         * {@code int} columns, and a bitmap scan only of the blocks whose {@code frees[]} has moved since
         * the last scan - a block nothing has been released from is never rescanned.
         */
        private boolean reenter(int need) {
            int mask = allocatedMask & ~(1 << curId);
            int best = NO_BLOCK;
            int bestFree = 0;
            while (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                mask &= mask - 1;
                if (hintFrees[id] != frees[id]) {
                    refreshHint(id);
                }
                int free = freeHint[id];
                if (free > bestFree) {
                    bestFree = free;
                    best = id;
                }
            }
            if (best == NO_BLOCK || bestFree < need) {
                return false;
            }
            leaveCurrent();
            ringReentries++;
            int base = freeBase[best];
            curId = best;
            curBump = base;
            curPassStart = base;
            curLimit = freeLimit[best];
            if (RING) {
                curBits = startBits[best];
            }
            curRoot = roots[best];
            hintFrees[best] = -1;              // the window is being consumed: the hint is spent
            return true;
        }

        /**
         * The best free window of a block that is not current, and the latch that says when to look again.
         * Two candidates, both free by Invariant B: {@code [0, head)} - no live buffer starts there and
         * none reaches in from below - and {@code [tailBump, tailLimit)}, the window the ring was left
         * holding, which nothing has been handed out of since.
         */
        private void refreshHint(int id) {
            hintFrees[id] = frees[id];
            ringScans++;
            int head = lowestLiveStart(startBits[id], 0);
            int headFree = head < 0 ? BLOCK_SIZE : head;
            int tailFree = tailLimit[id] - tailBump[id];
            if (headFree >= tailFree) {
                freeBase[id] = 0;
                freeLimit[id] = headFree;
                freeHint[id] = headFree;
            } else {
                freeBase[id] = tailBump[id];
                freeLimit[id] = tailLimit[id];
                freeHint[id] = tailFree;
            }
        }

        /** Worth another ring pass? Only if a buffer of this space was released since we gave up. */
        private boolean ringRetry() {
            if (!RING || spaceFrees() == ringGiveUpFrees) {
                return false;
            }
            exhausted = false;
            return true;
        }

        /** Releases this space has seen into its blocks. Only read when the space is {@link #exhausted}. */
        private int spaceFrees() {
            int total = 0;
            int mask = allocatedMask;
            while (mask != 0) {
                int id = Integer.numberOfTrailingZeros(mask);
                mask &= mask - 1;
                total += frees[id];
            }
            return total;
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
            curPassStart = 0;
            if (RING) {
                curLimit = BLOCK_SIZE;         // the block is empty, so the whole of it is free
                curBits = startBits[id];
            }
            curRoot = roots[id];
        }

        /** Take a free slot and give it a chunk. Cold: growth only. */
        private void newBlock() {
            int id = Integer.numberOfTrailingZeros(~allocatedMask & FULL_MASK);
            roots[id] = chunkAllocator.allocate(BLOCK_SIZE, BLOCK_SIZE);
            if (RING && startBits[id] == null) {
                startBits[id] = new long[BLOCK_LONGS];
            }
            allocs[id] = 0;
            frees[id] = 0;
            if (RING) {
                tailBump[id] = 0;
                tailLimit[id] = BLOCK_SIZE;
                hintFrees[id] = -1;
            }
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
            int need = slotBytes(size);
            int start = curBump;
            int end = start + need;
            if (end > (RING ? curLimit : BLOCK_SIZE)) {
                if (!(RING && advanceRing(need)) && !switchBlock(need)) {
                    return NO_BLOCK;
                }
                start = curBump;
                end = start + need;
            }
            curBump = end;
            allocs[curId]++;
            if (RING) {
                curBits[start >>> 9] |= 1L << (start >>> 3);
            }
            reservedStart = start;
            return curId;
        }

        /**
         * End of an iteration: every empty block becomes reusable; the current block stays current. With
         * the ring an empty CURRENT block has already restarted in place ({@code ringResets}), so what is
         * left for the hook is flagging the OTHER empty blocks - and clearing {@link #exhausted}.
         */
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
                        bytesBumped += curBump - curPassStart;
                        curBump = 0;           // the current block restarts in place, and stays current
                        curPassStart = 0;
                        if (RING) {
                            curLimit = BLOCK_SIZE;    // it is empty: the whole block is free again
                            hintFrees[id] = -1;
                        }
                    } else {
                        if (RING) {
                            tailBump[id] = 0;
                            tailLimit[id] = BLOCK_SIZE;
                            hintFrees[id] = -1;
                        }
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
            if (DEBUG_PINNED && pinned > 0 && ++pinnedTick >= DEBUG_PINNED_PERIOD) {
                pinnedTick = 0;
                attributePinned();
            }
        }

        /**
         * DEBUG_PINNED, cold: charge every block this hook left pinned to the allocation stack of the
         * oldest buffer still live in it. One walk of the space's buffer objects per pinned block - which
         * is why it runs once every {@link #DEBUG_PINNED_PERIOD} hooks that find one, and never otherwise.
         */
        void attributePinnedForTest() {
            attributePinned();
        }

        private void attributePinned() {
            Map<String, long[]> sites = pinnedSites;
            if (sites == null) {
                sites = new HashMap<String, long[]>();
                pinnedSites = sites;
            }
            long gen = generation - 1;          // the generation this hook closed
            int count = objectCount;
            ArenaBuf[] objs = objects;
            for (int id = 0; id < MAX_BLOCKS; id++) {
                if ((allocatedMask & (1 << id)) == 0 || allocs[id] == frees[id]) {
                    continue;                   // never allocated, or reset by this hook
                }
                pinnedSamples++;
                ArenaBuf oldest = null;
                int live = 0;
                for (int i = 0; i < count; i++) {
                    ArenaBuf b = objs[i];
                    if (b.refCnt > 0 && b.blockId == id) {
                        live++;
                        if (oldest == null || b.allocGen < oldest.allocGen) {
                            oldest = b;
                        }
                    }
                }
                if (oldest == null || oldest.allocSite == null) {
                    pinnedUnattributed++;
                    continue;
                }
                String key = formatSite(oldest.allocSite);
                long[] v = sites.get(key);
                if (v == null) {
                    v = new long[4];
                    sites.put(key, v);
                }
                v[0]++;                                  // pinned blocks charged to this stack
                long age = gen - oldest.allocGen;        // hooks the oldest buffer has survived
                if (age > v[1]) {
                    v[1] = age;
                }
                v[2] += live;                            // live buffers in the block at this hook
                v[3] += oldest.length;                   // bytes of the oldest live buffer
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
            curRoot = null;
            hook = null;
        }

        private void dropBlock(int id) {
            AbstractByteBuf root = roots[id];
            roots[id] = null;
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

        /** Bytes bump-allocated: what the retired passes used plus what the current one has taken. */
        long bytesBumpedTotal() {
            return bytesBumped + curBump - curPassStart;
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
                if (DEBUG_PINNED) {
                    mergeSites(DEAD_SITES, heap.pinnedSites);
                    mergeSites(DEAD_SITES, direct.pinnedSites);
                    long[] totals = DEAD_SITES.get(DEAD_TOTALS);
                    if (totals == null) {
                        totals = new long[4];
                        DEAD_SITES.put(DEAD_TOTALS, totals);
                    }
                    totals[0] += heap.pinnedSamples + direct.pinnedSamples;
                    totals[1] += heap.pinnedUnattributed + direct.pinnedUnattributed;
                }
                ARENAS.remove(this);
            }
        }
    }

    /**
     * A buffer over a region of a block - or, in the DELEGATED state, over a buffer of the delegate
     * allocator. Element access is {@link AdaptivePoolingAllocator.AdaptiveByteBuf}'s: the block's chunk
     * buffer is the root parent, {@link #start} is this buffer's index 0 inside it, and every accessor is
     * one forward to the root's own primitive. Everything {@link AbstractByteBuf} already builds on top of
     * those primitives - the indexed reads and writes, the char sequences, {@code setZero} - is inherited.
     */
    static final class ArenaBuf extends AbstractByteBuf {
        private final ByteBufAllocator alloc;
        private final Space space;       // an object belongs to one space for its whole life
        private final Thread owner;      // arena.owner, inlined to keep the confinement check to one load
        final int index;                 // stable slot in the space's object array
        /** The block this buffer's region belongs to, or {@link #DELEGATE_SLOT} when DELEGATED. */
        private int blockId;
        /** This buffer's index 0 inside {@link #rootParent}. */
        private int start;
        private int length;
        private int refCnt;
        /** The block's chunk buffer, or - when DELEGATED - the unwrapped delegate buffer. */
        private AbstractByteBuf rootParent;
        /** DELEGATED state only: the buffer to release, leak-aware wrapper and all. */
        private ByteBuf delegated;
        // The NIO view must be per buffer: several buffers of one block are asked for their view before any
        // of them is used (a gathering write), so a view shared per block would be handed out twice. It is
        // anchored at this buffer's index 0, so it is dropped whenever the region moves.
        private ByteBuffer tmpNioBuf;
        /** DEBUG_PINNED only: the hook generation this buffer was allocated in. */
        long allocGen;
        /** DEBUG_PINNED only: where it was allocated. Never read on any hot path. */
        StackTraceElement[] allocSite;

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

        /** This buffer's index 0 inside its block. For tests only. */
        int startForTest() {
            return start;
        }

        /**
         * Ints, and - only when this buffer lands in another block - one guarded reference store. That
         * store is the single reference store on the allocation path and it is paid once per block switch,
         * not once per allocation. The NIO view is cleared the same guarded way: most buffers never take
         * one, so the common case writes no reference at all.
         */
        void init(int blockId, int start, int length, int maxCapacity) {
            this.blockId = blockId;
            this.start = start;
            this.length = length;
            this.refCnt = 1;
            AbstractByteBuf root = space.curRoot;
            if (rootParent != root) {
                rootParent = root;
            }
            if (tmpNioBuf != null) {
                tmpNioBuf = null;
            }
            if (DEBUG_PINNED) {
                debugPinnedInit();
            }
            resetForReuse(maxCapacity);
        }

        /** DEBUG_PINNED only, never inlined into anything that matters: folded away when the flag is off. */
        private void debugPinnedInit() {
            allocGen = space.generation;
            allocSite = new Throwable().getStackTrace();
        }

        /** The buffer that holds the bytes: the block's chunk, or the delegate buffer when DELEGATED. */
        private AbstractByteBuf rootParent() {
            AbstractByteBuf root = rootParent;
            if (root != null) {
                return root;
            }
            throw new IllegalReferenceCountException(0);
        }

        /** This buffer's index {@code i} in the root parent's index space. */
        private int idx(int i) {
            return i + start;
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
            if (RING) {
                int st = start;                // no longer live: the ring may wrap into these bytes
                s.startBits[id][st >>> 9] &= ~(1L << (st >>> 3));
            }
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
            if (RING) {
                int st = start;                // no longer live: the ring may wrap into these bytes
                s.startBits[id][st >>> 9] &= ~(1L << (st >>> 3));
            }
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
            rootParent = null;
            tmpNioBuf = null;
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
         * released - without the ring that means it is handed out again only after the next hook, which keeps
         * any view taken during this iteration valid until the iteration ends; with the ring it may come back
         * as soon as the tail wraps over it.
         */
        private ByteBuf grow(int newCapacity) {
            int id = blockId;
            Space space = this.space;
            if (id == DELEGATE_SLOT) {
                delegated.capacity(newCapacity);   // DELEGATED: the delegate buffer grows itself
                length = newCapacity;
                tmpNioBuf = null;                  // its memory may have moved
                return this;
            }
            int newEnd = start + slotBytes(newCapacity);
            // Growing in place moves the tail, so it must stop at the ring's wall, not at the block's top:
            // that is what keeps "a live buffer below the tail ends at or before the tail" true.
            if (id == space.curId && space.curBump == start + slotBytes(length)
                    && newEnd <= (RING ? space.curLimit : BLOCK_SIZE)) {
                space.curBump = newEnd;
                length = newCapacity;
                tmpNioBuf = null;                  // the view is capped at the old length
                space.reallocInPlace++;
                return this;
            }
            AbstractByteBuf oldRoot = rootParent();
            int oldStart = start;
            int oldLength = length;
            int newId = space.reserve(newCapacity);
            if (newId >= 0) {
                AbstractByteBuf newRoot = space.roots[newId];
                int newStart = space.reservedStart;
                newRoot.setBytes(newStart, oldRoot, oldStart, oldLength);
                space.reallocMoved++;
                blockId = newId;
                start = newStart;
                length = newCapacity;
                rootParent = newRoot;
                tmpNioBuf = null;
            } else {
                ByteBuf buf = space.arena.alloc.delegateForGrow(space, newCapacity, maxCapacity());
                AbstractByteBuf target = unwrapDelegate(buf);
                target.setBytes(0, oldRoot, oldStart, oldLength);
                space.reallocDelegated++;
                delegated = buf;
                length = newCapacity;
                adoptDelegate(target);             // counts the delegate slot itself
            }
            if (RING) {
                space.startBits[id][oldStart >>> 9] &= ~(1L << (oldStart >>> 3));
            }
            space.frees[id]++;
            return this;
        }

        /** Point this buffer at a delegate buffer's memory; the id becomes {@link #DELEGATE_SLOT}. */
        private void adoptDelegate(AbstractByteBuf target) {
            if (blockId != DELEGATE_SLOT) {
                space.allocs[DELEGATE_SLOT]++;
            }
            blockId = DELEGATE_SLOT;
            rootParent = target;               // the delegate buffer's own index 0 is this buffer's index 0
            start = 0;
            tmpNioBuf = null;
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

        // --- memory, views and copies: the root parent's, shifted by start ---

        @Override
        public boolean hasArray() {
            return rootParent().hasArray();
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            return rootParent().array();
        }

        @Override
        public int arrayOffset() {
            return idx(rootParent().arrayOffset());
        }

        @Override
        public boolean hasMemoryAddress() {
            return rootParent().hasMemoryAddress();
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return _memoryAddress();
        }

        @Override
        long _memoryAddress() {
            AbstractByteBuf root = rootParent;
            return root != null ? root._memoryAddress() + start : 0L;
        }

        @Override
        public int nioBufferCount() {
            return 1;
        }

        @Override
        public ByteBuffer nioBuffer(int i, int len) {
            checkIndex(i, len);
            return rootParent().nioBuffer(idx(i), len);          // a fresh view per call
        }

        @Override
        public ByteBuffer[] nioBuffers(int i, int len) {
            checkIndex(i, len);
            return rootParent().nioBuffers(idx(i), len);
        }

        @Override
        public ByteBuffer internalNioBuffer(int i, int len) {
            checkIndex(i, len);
            return (ByteBuffer) internalNioBuffer().position(i).limit(i + len);
        }

        /** This buffer's own view, anchored at its index 0; {@link #init} and {@link #grow} drop it. */
        private ByteBuffer internalNioBuffer() {
            ByteBuffer buf = tmpNioBuf;
            if (buf == null) {
                tmpNioBuf = buf = rootParent().nioBuffer(start, length);
            }
            return (ByteBuffer) buf.clear();
        }

        @Override
        public ByteBuf copy(int i, int len) {
            checkIndex(i, len);
            return rootParent().copy(idx(i), len);
        }

        @Override
        public int forEachByte(int i, int len, ByteProcessor processor) {
            checkIndex(i, len);
            return forEachResult(rootParent().forEachByte(idx(i), len, processor));
        }

        @Override
        public int forEachByteDesc(int i, int len, ByteProcessor processor) {
            checkIndex(i, len);
            return forEachResult(rootParent().forEachByteDesc(idx(i), len, processor));
        }

        private int forEachResult(int ret) {
            return ret < start ? -1 : ret - start;
        }

        // --- element access: one forward to the root parent's primitive ---

        @Override
        protected byte _getByte(int i) {
            return rootParent()._getByte(idx(i));
        }

        @Override
        protected short _getShort(int i) {
            return rootParent()._getShort(idx(i));
        }

        @Override
        protected short _getShortLE(int i) {
            return rootParent()._getShortLE(idx(i));
        }

        @Override
        protected int _getUnsignedMedium(int i) {
            return rootParent()._getUnsignedMedium(idx(i));
        }

        @Override
        protected int _getUnsignedMediumLE(int i) {
            return rootParent()._getUnsignedMediumLE(idx(i));
        }

        @Override
        protected int _getInt(int i) {
            return rootParent()._getInt(idx(i));
        }

        @Override
        protected int _getIntLE(int i) {
            return rootParent()._getIntLE(idx(i));
        }

        @Override
        protected long _getLong(int i) {
            return rootParent()._getLong(idx(i));
        }

        @Override
        protected long _getLongLE(int i) {
            return rootParent()._getLongLE(idx(i));
        }

        @Override
        protected void _setByte(int i, int v) {
            rootParent()._setByte(idx(i), v);
        }

        @Override
        protected void _setShort(int i, int v) {
            rootParent()._setShort(idx(i), v);
        }

        @Override
        protected void _setShortLE(int i, int v) {
            rootParent()._setShortLE(idx(i), v);
        }

        @Override
        protected void _setMedium(int i, int v) {
            rootParent()._setMedium(idx(i), v);
        }

        @Override
        protected void _setMediumLE(int i, int v) {
            rootParent()._setMediumLE(idx(i), v);
        }

        @Override
        protected void _setInt(int i, int v) {
            rootParent()._setInt(idx(i), v);
        }

        @Override
        protected void _setIntLE(int i, int v) {
            rootParent()._setIntLE(idx(i), v);
        }

        @Override
        protected void _setLong(int i, long v) {
            rootParent()._setLong(idx(i), v);
        }

        @Override
        protected void _setLongLE(int i, long v) {
            rootParent()._setLongLE(idx(i), v);
        }

        // --- bulk access: the same forward, with the index check this class owns ---

        @Override
        public ByteBuf getBytes(int i, ByteBuf dst, int di, int len) {
            checkIndex(i, len);
            rootParent().getBytes(idx(i), dst, di, len);
            return this;
        }

        @Override
        public ByteBuf getBytes(int i, byte[] dst, int di, int len) {
            checkIndex(i, len);
            rootParent().getBytes(idx(i), dst, di, len);
            return this;
        }

        @Override
        public ByteBuf getBytes(int i, ByteBuffer dst) {
            checkIndex(i, dst.remaining());
            rootParent().getBytes(idx(i), dst);
            return this;
        }

        @Override
        public ByteBuf getBytes(int i, OutputStream out, int len) throws IOException {
            checkIndex(i, len);
            rootParent().getBytes(idx(i), out, len);
            return this;
        }

        @Override
        public int getBytes(int i, GatheringByteChannel out, int len) throws IOException {
            checkIndex(i, len);
            return rootParent().getBytes(idx(i), out, len);
        }

        @Override
        public int getBytes(int i, FileChannel out, long pos, int len) throws IOException {
            checkIndex(i, len);
            return rootParent().getBytes(idx(i), out, pos, len);
        }

        @Override
        public ByteBuf setBytes(int i, ByteBuf src, int si, int len) {
            checkIndex(i, len);
            rootParent().setBytes(idx(i), src, si, len);
            return this;
        }

        @Override
        public ByteBuf setBytes(int i, byte[] src, int si, int len) {
            checkIndex(i, len);
            rootParent().setBytes(idx(i), src, si, len);
            return this;
        }

        @Override
        public ByteBuf setBytes(int i, ByteBuffer src) {
            checkIndex(i, src.remaining());
            rootParent().setBytes(idx(i), src);
            return this;
        }

        @Override
        public int setBytes(int i, InputStream in, int len) throws IOException {
            checkIndex(i, len);
            return rootParent().setBytes(idx(i), in, len);
        }

        @Override
        public int setBytes(int i, ScatteringByteChannel in, int len) throws IOException {
            checkIndex(i, len);
            return rootParent().setBytes(idx(i), in, len);
        }

        @Override
        public int setBytes(int i, FileChannel in, long pos, int len) throws IOException {
            checkIndex(i, len);
            return rootParent().setBytes(idx(i), in, pos, len);
        }
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
        long blockSwitches;
        long ringWraps;
        long ringResets;
        long ringScans;
        long ringReentries;
        long ringStalls;
        long stallBytes;
        long strandedBytes;
        long holes256;
        long holes1k;
        long holes4k;
        long holes8k;
        long holesBig;
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
            blockSwitches += other.blockSwitches;
            ringWraps += other.ringWraps;
            ringResets += other.ringResets;
            ringScans += other.ringScans;
            ringReentries += other.ringReentries;
            ringStalls += other.ringStalls;
            stallBytes += other.stallBytes;
            strandedBytes += other.strandedBytes;
            holes256 += other.holes256;
            holes1k += other.holes1k;
            holes4k += other.holes4k;
            holes8k += other.holes8k;
            holesBig += other.holesBig;
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
            blockSwitches += arena.heap.blockSwitches + arena.direct.blockSwitches;
            ringWraps += arena.heap.ringWraps + arena.direct.ringWraps;
            ringResets += arena.heap.ringResets + arena.direct.ringResets;
            ringScans += arena.heap.ringScans + arena.direct.ringScans;
            ringReentries += arena.heap.ringReentries + arena.direct.ringReentries;
            ringStalls += arena.heap.ringStalls + arena.direct.ringStalls;
            stallBytes += arena.heap.stallBytes + arena.direct.stallBytes;
            strandedBytes += arena.heap.strandedBytes + arena.direct.strandedBytes;
            holes256 += arena.heap.holes256 + arena.direct.holes256;
            holes1k += arena.heap.holes1k + arena.direct.holes1k;
            holes4k += arena.heap.holes4k + arena.direct.holes4k;
            holes8k += arena.heap.holes8k + arena.direct.holes8k;
            holesBig += arena.heap.holesBig + arena.direct.holesBig;
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
                    + " blockSwitches=" + blockSwitches
                    + " ringWraps=" + ringWraps + " ringResets=" + ringResets + " ringScans=" + ringScans
                    + " ringReentries=" + ringReentries
                    + " ringStalls=" + ringStalls + " stallBytes=" + stallBytes
                    + " strandedBytes=" + strandedBytes
                    + " strandedShare=" + (stallBytes == 0 ? "n/a"
                            : String.format("%.2f%%", 100.0 * strandedBytes / stallBytes))
                    + " holes<=256=" + holes256 + " holes<=1k=" + holes1k + " holes<=4k=" + holes4k
                    + " holes<=8k=" + holes8k + " holes>8k=" + holesBig
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
                + " maxObjects=" + MAX_OBJECTS + " debug=" + DEBUG + " ring=" + RING
                + " ringStats=" + RING_STATS + ' ' + total + sb + pinnedSites();
    }

    /**
     * The pinned-block attribution collected under {@code -Darena.debugPinned=true}: one
     * {@code ARENAPINNED} summary line and one {@code ARENAPINNEDSITE} line per allocation stack, most
     * frequent first. Empty when the flag is off. Read off the owner threads' maps without
     * synchronisation with them, exactly as {@link #counters()} reads their plain longs: call it at
     * shutdown, not while the loops run.
     */
    public static String pinnedSites() {
        return DEBUG_PINNED ? pinnedSitesReport() : "";
    }

    /** The report itself, without the flag test: {@link #pinnedSites()} and the tests. */
    static String pinnedSitesReport() {
        Map<String, long[]> merged = new HashMap<String, long[]>();
        long samples = 0;
        long unattributed = 0;
        synchronized (ARENAS) {
            mergeSites(merged, DEAD_SITES);
            for (int i = 0; i < ARENAS.size(); i++) {
                Arena arena = ARENAS.get(i);
                samples += arena.heap.pinnedSamples + arena.direct.pinnedSamples;
                unattributed += arena.heap.pinnedUnattributed + arena.direct.pinnedUnattributed;
                mergeSites(merged, arena.heap.pinnedSites);
                mergeSites(merged, arena.direct.pinnedSites);
            }
            long[] dead = DEAD_SITES.get(DEAD_TOTALS);
            if (dead != null) {
                samples += dead[0];
                unattributed += dead[1];
            }
        }
        List<Map.Entry<String, long[]>> rows = new ArrayList<Map.Entry<String, long[]>>(merged.entrySet());
        Collections.sort(rows, new Comparator<Map.Entry<String, long[]>>() {
            @Override
            public int compare(Map.Entry<String, long[]> a, Map.Entry<String, long[]> b) {
                return Long.compare(b.getValue()[0], a.getValue()[0]);
            }
        });
        StringBuilder sb = new StringBuilder(4096);
        sb.append("\nARENAPINNED period=").append(DEBUG_PINNED_PERIOD)
          .append(" frames=").append(DEBUG_PINNED_FRAMES)
          .append(" pinnedBlockSamples=").append(samples)
          .append(" unattributed=").append(unattributed)
          .append(" sites=").append(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Map.Entry<String, long[]> e = rows.get(i);
            if (DEAD_TOTALS.equals(e.getKey())) {
                continue;
            }
            long[] v = e.getValue();
            sb.append("\nARENAPINNEDSITE blocks=").append(v[0])
              .append(" maxAgeHooks=").append(v[1])
              .append(" avgLiveInBlock=").append(v[0] == 0 ? 0 : v[2] / v[0])
              .append(" avgOldestBytes=").append(v[0] == 0 ? 0 : v[3] / v[0])
              .append(" stack=").append(e.getKey());
        }
        return sb.toString();
    }

    /** The key {@link #DEAD_SITES} parks the dead arenas' sample/unattributed totals under. */
    private static final String DEAD_TOTALS = "<totals>";

    private static void mergeSites(Map<String, long[]> into, Map<String, long[]> from) {
        if (from == null) {
            return;
        }
        for (Map.Entry<String, long[]> e : from.entrySet()) {
            long[] v = into.get(e.getKey());
            if (v == null) {
                v = new long[4];
                into.put(e.getKey(), v);
            }
            long[] o = e.getValue();
            v[0] += o[0];
            v[1] = Math.max(v[1], o[1]);
            v[2] += o[2];
            v[3] += o[3];
        }
    }

    /**
     * The allocation stack as one line: the allocator's own {@code io.netty.buffer} frames dropped from
     * the top, then at most {@link #DEBUG_PINNED_FRAMES} frames, innermost first, separated by {@code <-}.
     */
    static String formatSite(StackTraceElement[] trace) {
        int i = 0;
        while (i < trace.length && trace[i].getClassName().startsWith("io.netty.buffer.")) {
            i++;
        }
        if (i == trace.length) {
            i = 0;                             // nothing but allocator frames: report them rather than ""
        }
        StringBuilder sb = new StringBuilder(256);
        int kept = 0;
        for (; i < trace.length && kept < DEBUG_PINNED_FRAMES; i++, kept++) {
            if (kept > 0) {
                sb.append(" <- ");
            }
            StackTraceElement f = trace[i];
            String cls = f.getClassName();
            int dot = cls.lastIndexOf('.');
            sb.append(dot < 0 ? cls : cls.substring(dot + 1)).append('.').append(f.getMethodName());
        }
        return sb.toString();
    }
}
