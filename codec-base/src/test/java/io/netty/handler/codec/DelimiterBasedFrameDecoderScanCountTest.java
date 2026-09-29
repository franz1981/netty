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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Counts the bytes the decoder reads from the cumulation while searching for delimiters. No timing.
 */
public class DelimiterBasedFrameDecoderScanCountTest {

    /** Heap buffer that counts every byte read through the _getXxx primitives (used by indexOf/getByte). */
    static final class CountingByteBuf extends UnpooledHeapByteBuf {
        long bytesRead;

        CountingByteBuf(int capacity) {
            super(UnpooledByteBufAllocator.DEFAULT, capacity, capacity);
        }

        @Override
        protected byte _getByte(int index) {
            bytesRead += 1;
            return super._getByte(index);
        }

        @Override
        protected short _getShort(int index) {
            bytesRead += 2;
            return super._getShort(index);
        }

        @Override
        protected short _getShortLE(int index) {
            bytesRead += 2;
            return super._getShortLE(index);
        }

        @Override
        protected int _getUnsignedMedium(int index) {
            bytesRead += 3;
            return super._getUnsignedMedium(index);
        }

        @Override
        protected int _getUnsignedMediumLE(int index) {
            bytesRead += 3;
            return super._getUnsignedMediumLE(index);
        }

        @Override
        protected int _getInt(int index) {
            bytesRead += 4;
            return super._getInt(index);
        }

        @Override
        protected int _getIntLE(int index) {
            bytesRead += 4;
            return super._getIntLE(index);
        }

        @Override
        protected long _getLong(int index) {
            bytesRead += 8;
            return super._getLong(index);
        }

        @Override
        protected long _getLongLE(int index) {
            bytesRead += 8;
            return super._getLongLE(index);
        }
    }

    private static ByteBuf delim(String s) {
        return Unpooled.copiedBuffer(s, CharsetUtil.ISO_8859_1);
    }

    /** One byte per decode() call, no delimiter ever arrives (the advisory's trickle). */
    private static long trickle(DelimiterBasedFrameDecoder decoder, int n) throws Exception {
        CountingByteBuf buf = new CountingByteBuf(n);
        try {
            for (int i = 0; i < n; i++) {
                buf.writeByte('a');
                Object frame = decoder.decode(null, buf);
                assertEquals(null, frame);
            }
            return buf.bytesRead;
        } finally {
            buf.release();
        }
    }

    /** All data arrives in a single read and is decoded like ByteToMessageDecoder.callDecode does. */
    private static long oneRead(DelimiterBasedFrameDecoder decoder, byte[] data, int expectedFrames)
            throws Exception {
        CountingByteBuf buf = new CountingByteBuf(data.length);
        try {
            buf.writeBytes(data);
            int frames = 0;
            for (;;) {
                Object frame = decoder.decode(null, buf);
                if (frame == null) {
                    break;
                }
                frames++;
                ReferenceCountUtil.release(frame);
            }
            assertEquals(expectedFrames, frames);
            return buf.bytesRead;
        } finally {
            buf.release();
        }
    }

    @Test
    public void trickleSingleNulDelimiterIsLinear() throws Exception {
        int n = 8192;
        long read = trickle(new DelimiterBasedFrameDecoder(Integer.MAX_VALUE, Delimiters.nulDelimiter()), n);
        System.out.println("trickle, nul delimiter, n=" + n + " bytesRead=" + read);
        assertTrue(read <= 16L * n, "bytesRead=" + read + " for n=" + n);
    }

    @Test
    public void trickleMultipleMultiByteDelimitersIsLinear() throws Exception {
        int n = 8192;
        long read = trickle(new DelimiterBasedFrameDecoder(Integer.MAX_VALUE,
                delim("###"), delim("\0"), delim("ab")), n);
        System.out.println("trickle, 3 delimiters, n=" + n + " bytesRead=" + read);
        assertTrue(read <= 3 * 16L * n, "bytesRead=" + read + " for n=" + n);
    }

    /**
     * Two delimiters, the second never occurs. Every emitted frame restarts the search for the absent
     * delimiter from the reader index, so a single read carrying many short frames is scanned
     * frames x remaining-bytes times.
     */
    @Test
    public void manyFramesInOneReadWithAnAbsentSecondDelimiterIsLinear() throws Exception {
        int frames = 8192;
        byte[] data = new byte[frames * 2];
        for (int i = 0; i < data.length; i += 2) {
            data[i] = 'a';
            data[i + 1] = 0;
        }
        long read = oneRead(new DelimiterBasedFrameDecoder(Integer.MAX_VALUE,
                Delimiters.nulDelimiter()[0], delim("##")), data, frames);
        System.out.println("one read, " + frames + " frames of 2 bytes, delimiters {nul, ##} bytesRead=" + read);
        assertTrue(read <= 32L * data.length, "bytesRead=" + read + " for " + data.length + " bytes");
    }
}
