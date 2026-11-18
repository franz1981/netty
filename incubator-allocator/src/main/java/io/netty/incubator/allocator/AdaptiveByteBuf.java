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
import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.internal.CustomAllocatorAccess;
import io.netty.incubator.allocator.AdaptivePoolingAllocator.Chunk;
import io.netty.util.ByteProcessor;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

final class AdaptiveByteBuf extends AbstractReferenceCountedByteBuf {

    private static final CustomAllocatorAccess ACCESS = CustomAllocatorAccess.get();

    /** The pool this object goes back to: the heap's whose chunk holds it; {@code null} for a fallback buffer. */
    AdaptivePoolingAllocator.BufferPool pool;

    // this both act as adjustment and the start index for a free list segment allocation
    private int startIndex;
    private AbstractByteBuf rootParent;
    Chunk chunk;
    private int length;
    private int maxFastCapacity;
    private ByteBuffer tmpNioBuf;
    private boolean hasArray;
    private boolean hasMemoryAddress;

    AdaptiveByteBuf(AdaptivePoolingAllocator.BufferPool pool) {
        super(0);
        this.pool = pool;
    }

    /**
     * Points this buffer at a slot. The indices are not touched: a buffer from the pool was {@link #reset} when taken,
     * a new one starts at 0, and a reallocation sets them after it copied. The JFR event goes first, from the values:
     * if it throws, nothing of the buffer has changed, and what follows only assigns.
     */
    void init(AbstractByteBuf unwrapped, Chunk wrapped, int startIndex, int size, int capacity, int maxCapacity) {
        BufferEvents.allocated(unwrapped, wrapped, startIndex, size, capacity, maxCapacity);
        this.startIndex = startIndex;
        chunk = wrapped;
        length = size;
        maxFastCapacity = capacity;
        maxCapacity(maxCapacity);
        hasArray = unwrapped.hasArray();
        // The chunk kept its root's address: no call on the root per allocation.
        hasMemoryAddress = wrapped.delegateAddress != 0;
        rootParent = unwrapped;
        tmpNioBuf = null;
    }

    /**
     * Resets a buffer taken from its pool: reference count, indices and marks. The accessors and the indices are
     * the public ones: this class is outside the package of {@code AbstractByteBuf}.
     */
    void reset() {
        resetRefCnt();
        clear();
        markReaderIndex();
        markWriterIndex();
    }

    private RootAccess root() {
        final AbstractByteBuf rootParent = this.rootParent;
        if (rootParent != null) {
            return (RootAccess) rootParent;
        }
        throw new IllegalReferenceCountException();
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
        return Math.min(maxFastCapacity, maxCapacity()) - writerIndex();
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

        BufferEvents.reallocated(this, newCapacity);

        // Reallocation required.
        Chunk chunk = this.chunk;
        AdaptivePoolingAllocator allocator = chunk.allocator;
        int readerIndex = readerIndex();
        int writerIndex = writerIndex();
        int baseOldRootIndex = startIndex;
        int oldLength = length;
        int oldCapacity = maxFastCapacity;
        AbstractByteBuf oldRoot = rootParent();
        allocator.reallocate(newCapacity, maxCapacity(), this);
        oldRoot.getBytes(baseOldRootIndex, this, 0, oldLength);
        chunk.releaseSlot(baseOldRootIndex, oldCapacity);
        assert oldCapacity < maxFastCapacity && newCapacity <= maxFastCapacity :
                "Capacity increase failed";
        setIndex(readerIndex, writerIndex);
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
        if (!hasMemoryAddress) {
            throw new UnsupportedOperationException();
        }
        return address();
    }

    /** The address of the first byte, computed from the chunk's: 0 if released (checkAccessible=false). */
    private long address() {
        final Chunk c = chunk;
        return c == null ? 0L : c.delegateAddress + startIndex;
    }

    /**
     * As {@link AbstractByteBuf#indexOf}, but a forward search over a root with an address (Unsafe) or an array reads
     * words straight from the root's memory, instead of one call into the root per word.
     */
    @Override
    public int indexOf(int fromIndex, int toIndex, byte value) {
        if (!PageStoreBufUtil.WORD_SEARCH || fromIndex > toIndex) {
            return super.indexOf(fromIndex, toIndex, value);
        }
        fromIndex = Math.max(fromIndex, 0);
        if (fromIndex >= toIndex || capacity() == 0) {
            return -1;
        }
        // the same bounds and accessibility check as the base
        checkIndex(fromIndex, toIndex - fromIndex);
        // chunk is null once released: with checkAccessible=false the check above passes, and the base path throws
        final Chunk c = chunk;
        final int found;
        if (hasMemoryAddress && c != null) {
            found = PageStoreBufUtil.firstIndexOf(c.delegateAddress + startIndex + fromIndex, toIndex - fromIndex,
                    value);
        } else if (hasArray) {
            found = PageStoreBufUtil.firstIndexOf(rootParent().array(), arrayOffset() + fromIndex,
                    toIndex - fromIndex, value);
        } else {
            return super.indexOf(fromIndex, toIndex, value);
        }
        return found < 0 ? -1 : fromIndex + found;
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
        return root()._getByte(idx(index));
    }

    @Override
    protected short _getShort(int index) {
        return root()._getShort(idx(index));
    }

    @Override
    protected short _getShortLE(int index) {
        return root()._getShortLE(idx(index));
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return root()._getUnsignedMedium(idx(index));
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        return root()._getUnsignedMediumLE(idx(index));
    }

    @Override
    protected int _getInt(int index) {
        return root()._getInt(idx(index));
    }

    @Override
    protected int _getIntLE(int index) {
        return root()._getIntLE(idx(index));
    }

    @Override
    protected long _getLong(int index) {
        return root()._getLong(idx(index));
    }

    @Override
    protected long _getLongLE(int index) {
        return root()._getLongLE(idx(index));
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
        root()._setByte(idx(index), value);
    }

    @Override
    protected void _setShort(int index, int value) {
        root()._setShort(idx(index), value);
    }

    @Override
    protected void _setShortLE(int index, int value) {
        root()._setShortLE(idx(index), value);
    }

    @Override
    protected void _setMedium(int index, int value) {
        root()._setMedium(idx(index), value);
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        root()._setMediumLE(idx(index), value);
    }

    @Override
    protected void _setInt(int index, int value) {
        root()._setInt(idx(index), value);
    }

    @Override
    protected void _setIntLE(int index, int value) {
        root()._setIntLE(idx(index), value);
    }

    @Override
    protected void _setLong(int index, long value) {
        root()._setLong(idx(index), value);
    }

    @Override
    protected void _setLongLE(int index, long value) {
        root()._setLongLE(idx(index), value);
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        checkIndex(index, length);
        if (tmpNioBuf == null && PlatformDependent.javaVersion() >= 13) {
            ByteBuffer dstBuffer = ACCESS.internalNioBuffer(rootParent());
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
            ByteBuffer dstBuffer = ACCESS.internalNioBuffer(rootParent());
            ByteBuffer srcBuffer = ACCESS.internalNioBuffer(srcBuf.rootParent());
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
            PageStoreBufUtil.readBytes(alloc(), tmp.hasArray() ? tmp : tmp.duplicate(), index, length, out);
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
        byte[] tmp = PageStoreBufUtil.threadLocalTempArray(length);
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
        rootParent().setZero(idx(writerIndex()), length);
        writerIndex(writerIndex() + length);
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

    /** The address of the first byte for the JFR events: unlike {@link #memoryAddress()} it never throws, 0 if none. */
    long eventAddress() {
        return hasMemoryAddress ? address() : 0L;
    }

    @Override
    protected void deallocate() {
        BufferEvents.freed(this);

        Chunk chunk = this.chunk;
        tmpNioBuf = null;
        this.chunk = null;
        rootParent = null;
        hasMemoryAddress = false;
        if (chunk != null) {
            chunk.release(startIndex, maxFastCapacity, this);
        } else if (pool != null) {
            pool.recycle(this);
        }
    }
}
