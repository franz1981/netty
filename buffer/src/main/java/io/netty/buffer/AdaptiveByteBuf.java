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

import io.netty.buffer.AdaptivePoolingAllocator.Chunk;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.Recycler.EnhancedHandle;
import io.netty.util.internal.ObjectUtil;
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
import java.nio.charset.Charset;

final class AdaptiveByteBuf extends AbstractReferenceCountedByteBuf {

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

        BufferEvents.allocated(this, wrapped);
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

        BufferEvents.reallocated(this, newCapacity);

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
        chunk.releaseSlot(baseOldRootIndex, oldCapacity);
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
        BufferEvents.freed(this);

        if (chunk != null) {
            chunk.releaseSlot(startIndex, maxFastCapacity);
        }
        tmpNioBuf = null;
        chunk = null;
        rootParent = null;
        handle.unguardedRecycle(this);
    }
}
