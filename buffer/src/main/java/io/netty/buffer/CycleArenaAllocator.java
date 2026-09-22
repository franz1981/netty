package io.netty.buffer;

import io.netty.util.concurrent.FastThreadLocal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * EXPERIMENT (floor measurement, heap buffers only): per-thread bump arena for cycle-scoped buffers.
 * Allocation: bump pointer in the current block; block exhausted -> next block (x2, up to MAX_BLOCK), then a
 * delegate allocator. Release: must happen on the allocating thread (throws otherwise), plain int refcount,
 * live counter per block; a block whose live count returns to zero is reset. No atomics anywhere.
 */
/**
 * EXPERIMENT (2026-09-22): a per-event-loop bump arena for cycle-scoped buffers - the PoC measured in
 * netty-bench/docs/BACKLOG.md (block B2). Heap only, no shrinking, release must happen on the allocating thread.
 */
public final class CycleArenaAllocator extends AbstractByteBufAllocator {
    static final int INITIAL_BLOCK = Integer.getInteger("arena.initialBlock", 256 * 1024);
    static final int MAX_BLOCK = Integer.getInteger("arena.maxBlock", 8 * 1024 * 1024);
    static final int MAX_BLOCKS = Integer.getInteger("arena.maxBlocks", 4);
    static final int MIN_SIZE = 32;
    // TELEMETRY (PoC): where allocations went
    static final java.util.concurrent.atomic.LongAdder T_ARENA = new java.util.concurrent.atomic.LongAdder();
    static final java.util.concurrent.atomic.LongAdder T_FALLBACK = new java.util.concurrent.atomic.LongAdder();
    static final java.util.concurrent.atomic.LongAdder T_BLOCK_REUSE = new java.util.concurrent.atomic.LongAdder();
    static final java.util.concurrent.atomic.LongAdder T_GROW = new java.util.concurrent.atomic.LongAdder();
    static final java.util.concurrent.atomic.LongAdder T_LIFO_POP = new java.util.concurrent.atomic.LongAdder();
    static final java.util.concurrent.atomic.LongAdder T_UNPOOLED_OBJ = new java.util.concurrent.atomic.LongAdder();
    public static String counters() {
        return "ARENATELE arena=" + T_ARENA.sum() + " fallback=" + T_FALLBACK.sum() + " blockReuse=" + T_BLOCK_REUSE.sum()
                + " grow=" + T_GROW.sum() + " lifoPop=" + T_LIFO_POP.sum() + " unpooledObjects=" + T_UNPOOLED_OBJ.sum();
    }

    private final ByteBufAllocator fallback = new AdaptiveByteBufAllocator();
    private final FastThreadLocal<Arena> arenas = new FastThreadLocal<Arena>() {
        @Override
        protected Arena initialValue() {
            return new Arena(CycleArenaAllocator.this, Thread.currentThread());
        }
    };

    public CycleArenaAllocator() {
        super(false);
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        Arena a = arenas.get();
        ByteBuf b = a.allocate(initialCapacity, maxCapacity);
        if (b != null) { T_ARENA.increment(); return b; }
        T_FALLBACK.increment();
        return fallback.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        return fallback.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }

    /** The arena block as a buffer, with the element accessors exposed (what the mimalloc port's adapter does). */
    static final class Root extends UnpooledUnsafeHeapByteBuf {
        Root(int size) { super(UnpooledByteBufAllocator.DEFAULT, size, size); }
        @Override public byte _getByte(int i) { return super._getByte(i); }
        @Override public short _getShort(int i) { return super._getShort(i); }
        @Override public short _getShortLE(int i) { return super._getShortLE(i); }
        @Override public int _getUnsignedMedium(int i) { return super._getUnsignedMedium(i); }
        @Override public int _getUnsignedMediumLE(int i) { return super._getUnsignedMediumLE(i); }
        @Override public int _getInt(int i) { return super._getInt(i); }
        @Override public int _getIntLE(int i) { return super._getIntLE(i); }
        @Override public long _getLong(int i) { return super._getLong(i); }
        @Override public long _getLongLE(int i) { return super._getLongLE(i); }
        @Override public void _setByte(int i, int v) { super._setByte(i, v); }
        @Override public void _setShort(int i, int v) { super._setShort(i, v); }
        @Override public void _setShortLE(int i, int v) { super._setShortLE(i, v); }
        @Override public void _setMedium(int i, int v) { super._setMedium(i, v); }
        @Override public void _setMediumLE(int i, int v) { super._setMediumLE(i, v); }
        @Override public void _setInt(int i, int v) { super._setInt(i, v); }
        @Override public void _setIntLE(int i, int v) { super._setIntLE(i, v); }
        @Override public void _setLong(int i, long v) { super._setLong(i, v); }
        @Override public void _setLongLE(int i, long v) { super._setLongLE(i, v); }
    }

    static final class Block {
        final byte[] mem;
        final Root root;
        int bump;
        int live;
        Block(int size) {
            root = new Root(size);
            mem = root.array();
        }
    }

    static final class Arena {
        final CycleArenaAllocator alloc;
        final Thread owner;
        final Block[] blocks = new Block[MAX_BLOCKS];
        int blockCount;
        Block current;
        static final int POOL = Integer.getInteger("arena.objects", 8192);
        final ArenaBuf[] objects = new ArenaBuf[POOL];   // written once per object, when it is created
        final int[] free = new int[POOL];                // indexes of pooled objects: an int stack, no barriers
        int objCount;
        int freeTop;

        Arena(CycleArenaAllocator alloc, Thread owner) {
            this.alloc = alloc;
            this.owner = owner;
            current = newBlock(INITIAL_BLOCK);
        }

        private Block newBlock(int size) {
            Block b = new Block(size);
            blocks[blockCount++] = b;
            return b;
        }

        ByteBuf allocate(int size, int maxCapacity) {
            size = Math.max(size, MIN_SIZE);
            Block b = current;
            int start = b.bump;
            int end = start + size;
            if (end > b.mem.length) {
                b = nextBlock(size);
                if (b == null) {
                    return null;
                }
                start = 0;
                end = size;
            }
            b.bump = end;
            b.live++;
            ArenaBuf buf;
            if (freeTop > 0) {
                buf = objects[free[--freeTop]];
            } else if (objCount < POOL) {
                buf = new ArenaBuf(alloc, this, objCount);
                objects[objCount++] = buf;
            } else {
                T_UNPOOLED_OBJ.increment();
                buf = new ArenaBuf(alloc, this, -1);       // beyond the pool: garbage after release
            }
            buf.init(b, start, size, maxCapacity);
            return buf;
        }

        /** The current block is full: reuse an idle one, grow, or give up (fallback). */
        private Block nextBlock(int size) {
            for (int i = 0; i < blockCount; i++) {
                Block b = blocks[i];
                if (b.live == 0 && b.mem.length >= size) {
                    T_BLOCK_REUSE.increment();
                    b.bump = 0;
                    current = b;
                    return b;
                }
            }
            if (blockCount < MAX_BLOCKS) {
                T_GROW.increment();
                int next = Math.min(MAX_BLOCK, Math.max(size, blocks[blockCount - 1].mem.length << 1));
                current = newBlock(next);
                return current;
            }
            return null;
        }

        void release(ArenaBuf buf, Block b, int start, int end) {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("released on a thread other than the allocating event loop");
            }
            if (--b.live == 0) {
                b.bump = 0;              // wholly free: the block is a fresh bump region again
            } else if (end == b.bump) {
                T_LIFO_POP.increment();
                b.bump = start;          // the topmost buffer: LIFO pop, its memory is reusable at once
            }
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
        private Block block;         // stored only when the buffer lands in another block
        private Root root;
        private int start;
        private int length;
        private int refCnt;

        ArenaBuf(ByteBufAllocator alloc, Arena arena, int index) {
            super(0);
            this.alloc = alloc;
            this.arena = arena;
            this.index = index;
        }

        void init(Block block, int start, int length, int maxCapacity) {
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
            if (newCapacity <= length) {
                length = newCapacity;
                return this;
            }
            Block b = block;
            // The last buffer of its block grows in place: bump the block's tail.
            if (b != null && b.bump == start + length && start + newCapacity <= b.mem.length) {
                b.bump = start + newCapacity;
                length = newCapacity;
                return this;
            }
            throw new UnsupportedOperationException("arena buffer cannot grow in place: " + length + " -> " + newCapacity);
        }
        @Override public ByteBufAllocator alloc() { return alloc; }
        @Override public ByteOrder order() { return ByteOrder.BIG_ENDIAN; }
        @Override public ByteBuf unwrap() { return null; }
        @Override public boolean isDirect() { return false; }
        @Override public boolean hasArray() { return true; }
        @Override public byte[] array() { return block.mem; }
        @Override public int arrayOffset() { return start; }
        @Override public boolean hasMemoryAddress() { return false; }
        @Override public long memoryAddress() { throw new UnsupportedOperationException(); }
        @Override public int nioBufferCount() { return 1; }
        @Override public ByteBuffer nioBuffer(int index, int len) { return root.nioBuffer(idx(index), len); }
        @Override public ByteBuffer internalNioBuffer(int index, int len) { return root.internalNioBuffer(idx(index), len); }
        @Override public ByteBuffer[] nioBuffers(int index, int len) { return root.nioBuffers(idx(index), len); }
        @Override public ByteBuf copy(int index, int len) { return alloc.heapBuffer(len).writeBytes(root, idx(index), len); }

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
