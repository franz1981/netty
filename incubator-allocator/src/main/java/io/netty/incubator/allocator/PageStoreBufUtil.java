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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.CleanableDirectBuffer;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SWARUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** The package-private helpers of {@code ByteBufUtil} that the buffers need, copied: streams and word searches. */
final class PageStoreBufUtil {
    private static final int MAX_TL_ARRAY_LEN = 1024;
    private static final int WRITE_CHUNK_SIZE = 8192;
    /**
     * The word searches below read native-order words from raw memory at any offset: Unsafe, and a platform that
     * allows unaligned access. Both are tested, as {@code PlatformDependent} does: {@code isUnaligned()} is the
     * unaligned flag of {@code PlatformDependent0}, which does not say that {@code hasUnsafe()} is true (it is not
     * on Android and IKVM.NET).
     */
    static final boolean WORD_SEARCH = PlatformDependent.hasUnsafe() && PlatformDependent.isUnaligned();
    private static final boolean NATIVE_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    private static final FastThreadLocal<byte[]> BYTE_ARRAYS = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return PlatformDependent.allocateUninitializedArray(MAX_TL_ARRAY_LEN);
        }
    };

    /**
     * The offset of the first {@code value} in the {@code length} bytes at {@code address}, or -1. Whole words first,
     * then the {@code length & 7} tail. Requires {@link #WORD_SEARCH}.
     */
    static int firstIndexOf(long address, int length, byte value) {
        final long pattern = SWARUtil.compilePattern(value);
        int offset = 0;
        for (final int words = length & ~7; offset < words; offset += Long.BYTES) {
            final long result = SWARUtil.applyPattern(PlatformDependent.getLong(address + offset), pattern);
            if (result != 0) {
                return offset + SWARUtil.getIndex(result, NATIVE_BIG_ENDIAN);
            }
        }
        for (; offset < length; offset++) {
            if (PlatformDependent.getByte(address + offset) == value) {
                return offset;
            }
        }
        return -1;
    }

    /** As {@link #firstIndexOf(long, int, byte)}, for {@code length} bytes of {@code array} from {@code index}. */
    static int firstIndexOf(byte[] array, int index, int length, byte value) {
        final long pattern = SWARUtil.compilePattern(value);
        int offset = 0;
        for (final int words = length & ~7; offset < words; offset += Long.BYTES) {
            final long result = SWARUtil.applyPattern(PlatformDependent.getLong(array, index + offset), pattern);
            if (result != 0) {
                return offset + SWARUtil.getIndex(result, NATIVE_BIG_ENDIAN);
            }
        }
        for (; offset < length; offset++) {
            if (array[index + offset] == value) {
                return offset;
            }
        }
        return -1;
    }

    /** Allocates a new array if {@code minLength} is above {@value #MAX_TL_ARRAY_LEN}. */
    static byte[] threadLocalTempArray(int minLength) {
        // Only make use of ThreadLocal if we use a FastThreadLocalThread to make the implementation
        // Virtual Thread friendly.
        // See https://github.com/netty/netty/issues/14609
        if (minLength <= MAX_TL_ARRAY_LEN && FastThreadLocalThread.currentThreadHasFastThreadLocal()) {
            return BYTE_ARRAYS.get();
        }
        return PlatformDependent.allocateUninitializedArray(minLength);
    }

    /**
     * Read bytes from the given {@link ByteBuffer} into the given {@link OutputStream} using the {@code position} and
     * {@code length}. The position and limit of the given {@link ByteBuffer} may be adjusted.
     */
    static void readBytes(ByteBufAllocator allocator, ByteBuffer buffer, int position, int length, OutputStream out)
            throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), position + buffer.arrayOffset(), length);
        } else {
            int chunkLen = Math.min(length, WRITE_CHUNK_SIZE);
            buffer.clear().position(position).limit(position + length);

            if (length <= MAX_TL_ARRAY_LEN || !allocator.isDirectBufferPooled()) {
                getBytes(buffer, threadLocalTempArray(chunkLen), 0, chunkLen, out, length);
            } else {
                // if direct buffers are pooled chances are good that heap buffers are pooled as well.
                ByteBuf tmpBuf = allocator.heapBuffer(chunkLen);
                try {
                    byte[] tmp = tmpBuf.array();
                    int offset = tmpBuf.arrayOffset();
                    getBytes(buffer, tmp, offset, chunkLen, out, length);
                } finally {
                    tmpBuf.release();
                }
            }
        }
    }

    private static void getBytes(ByteBuffer inBuffer, byte[] in, int inOffset, int inLen, OutputStream out, int outLen)
            throws IOException {
        do {
            int len = Math.min(inLen, outLen);
            inBuffer.get(in, inOffset, len);
            out.write(in, inOffset, len);
            outLen -= len;
        } while (outLen > 0);
    }

    /** The address of {@code memory}'s first byte, or 0 if neither it nor the JVM can tell. */
    static long addressOf(CleanableDirectBuffer memory) {
        if (memory.hasMemoryAddress()) {
            return memory.memoryAddress();
        }
        ByteBuffer buffer = memory.buffer();
        return PlatformDependent.hasDirectByteBufferAddress(buffer) ? PlatformDependent.directBufferAddress(buffer) : 0;
    }

    private PageStoreBufUtil() { }
}
