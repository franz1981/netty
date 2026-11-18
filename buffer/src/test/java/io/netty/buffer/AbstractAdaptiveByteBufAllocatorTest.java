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

import io.netty.util.NettyRuntime;
import io.netty.util.test.DisabledForSlowLeakDetection;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import io.netty.buffer.AbstractByteBufTest.TestGatheringByteChannel;

/**
 * The tests every allocator that follows the adaptive allocator's contract shares: they use the public API, and
 * {@link #bufferClass()} for the class of the buffers it hands out.
 */
public abstract class AbstractAdaptiveByteBufAllocatorTest<T extends AbstractByteBufAllocator>
        extends AbstractByteBufAllocatorTest<T> {

    /** The class of the (non leak aware) buffers the allocator returns. */
    protected abstract Class<? extends ByteBuf> bufferClass();

    /** An allocator made by the allocator's no-argument constructor. */
    protected abstract T newDefaultAllocator();

    @Override
    @Test
    public void testUnsafeHeapBufferAndUnsafeDirectBuffer() {
        T allocator = newUnpooledAllocator();
        ByteBuf directBuffer = allocator.directBuffer();
        assertInstanceOf(directBuffer, bufferClass());
        assertTrue(directBuffer.isDirect());
        directBuffer.release();

        ByteBuf heapBuffer = allocator.heapBuffer();
        assertInstanceOf(heapBuffer, bufferClass());
        assertFalse(heapBuffer.isDirect());
        heapBuffer.release();
    }

    @Test
    public void getBytesBoundaryCheckWithFileChannel() {
        T allocator = newAllocator(false);
        final ByteBuf buf = allocator.directBuffer(7);
        try {
            assertThrows(IndexOutOfBoundsException.class, new Executable() {
                @Override
                public void execute() throws IOException {
                    // capacity 7，4+8=12 <= maxFastCapacity 32
                    buf.getBytes(4, (FileChannel) null, 0L, 8);
                }
            });
        } finally {
            buf.release();
        }
    }

    @Test
    public void testGetBytesBoundaryCheckWithGatheringByteChannel() throws Exception {
        T allocator = newAllocator(false);
        TestGatheringByteChannel channel = new TestGatheringByteChannel();
        final ByteBuf buf = allocator.directBuffer(7);
        try {
            assertThrows(IndexOutOfBoundsException.class, new Executable() {
                @Override
                public void execute() throws IOException {
                    // capacity 7，4+8=12 <= maxFastCapacity 32
                    buf.getBytes(4, channel, 8);
                }
            });
        } finally {
            channel.close();
            buf.release();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void sliceOrDuplicateUnwrapLetNotEscapeRootParent(boolean slice) {
        T allocator = newAllocator(false);
        ByteBuf buffer = allocator.buffer(8);
        assertInstanceOf(buffer, bufferClass());
        // Unwrap if this is wrapped by a leak aware buffer.
        if (buffer instanceof SimpleLeakAwareByteBuf) {
            assertNull(buffer.unwrap().unwrap());
        } else {
            assertNull(buffer.unwrap());
        }

        ByteBuf derived = slice ? buffer.slice(0, 4) : buffer.duplicate();
        // When we unwrap the derived buffer we should get our original buffer of type adaptive buffer back.
        ByteBuf unwrapped = derived instanceof SimpleLeakAwareByteBuf ?
                derived.unwrap().unwrap() : derived.unwrap();
        assertInstanceOf(unwrapped, bufferClass());
        assertSameBuffer(buffer instanceof SimpleLeakAwareByteBuf ? buffer.unwrap() : buffer, unwrapped);

        ByteBuf retainedDerived = slice ? buffer.retainedSlice(0, 4) : buffer.retainedDuplicate();
        // When we unwrap the derived buffer we should get our original buffer of type adaptive buffer back.
        ByteBuf unwrappedRetained = retainedDerived instanceof SimpleLeakAwareByteBuf ?
                retainedDerived.unwrap().unwrap() :  retainedDerived.unwrap();
        assertInstanceOf(unwrappedRetained, bufferClass());
        assertSameBuffer(buffer instanceof SimpleLeakAwareByteBuf ? buffer.unwrap() : buffer, unwrappedRetained);
        retainedDerived.release();

        assertTrue(buffer.release());
    }

    @Test
    public void testAllocateWithoutLock() throws InterruptedException {
        final T alloc = newDefaultAllocator();
        // Make `threadCount` bigger than `AdaptivePoolingAllocator.MAX_STRIPES`, to let thread collision easily happen.
        int threadCount = NettyRuntime.availableProcessors() * 4;
        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<Throwable>();
        for (int i = 0; i < threadCount; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 1024; j++) {
                        try {
                            ByteBuf buffer = null;
                            try {
                                buffer = alloc.heapBuffer(128);
                                buffer.ensureWritable(ThreadLocalRandom.current().nextInt(512, 32769));
                            } finally {
                                if (buffer != null) {
                                    buffer.release();
                                }
                            }
                        } catch (Throwable t) {
                            throwableAtomicReference.set(t);
                        }
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Throwable throwable = throwableAtomicReference.get();
        if (throwable != null) {
            fail("Expected no exception, but got", throwable);
        }
    }

    @DisabledForSlowLeakDetection
    @RepeatedTest(100)
    void buddyAllocationConsistency(RepetitionInfo info) {
        SplittableRandom rng = new SplittableRandom(info.getCurrentRepetition());
        T allocator = newAllocator(true);
        int small = 32768;
        int large = 2 * small;
        int xlarge = 2 * large;

        int[] allocationSizes = {
                small, small, small, small, small, small, small, small,
                large, large, large, large,
                xlarge, xlarge,
        };

        shuffle(rng, allocationSizes);

        ByteBuf[] bufs = new ByteBuf[allocationSizes.length];
        Arrays.setAll(bufs, i -> allocator.buffer(allocationSizes[i], allocationSizes[i]));

        shuffle(rng, bufs);

        int[] reallocations = new int[bufs.length / 2];
        for (int i = 0; i < reallocations.length; i++) {
            reallocations[i] = bufs[i].capacity();
            bufs[i].release();
            bufs[i] = null;
        }
        for (int i = 0; i < reallocations.length; i++) {
            assertNull(bufs[i]);
            bufs[i] = allocator.buffer(reallocations[i], reallocations[i]);
        }

        for (int i = 0; i < bufs.length; i++) {
            while (bufs[i].isWritable()) {
                bufs[i].writeByte(i + 1);
            }
        }
        try {
            for (int i = 0; i < bufs.length; i++) {
                while (bufs[i].isReadable()) {
                    int b = Byte.toUnsignedInt(bufs[i].readByte());
                    if (b != i + 1) {
                        fail("Expected byte " + (i + 1) +
                                " at index " + (bufs[i].readerIndex() - 1) +
                                " but got " + b);
                    }
                }
            }
        } finally {
            for (ByteBuf buf : bufs) {
                buf.release();
            }
        }
    }

    private static void shuffle(SplittableRandom rng, Object array) {
        int len = Array.getLength(array);
        for (int i = 0; i < len; i++) {
            int n = rng.nextInt(i, len);
            Object value = Array.get(array, i);
            Array.set(array, i, Array.get(array, n));
            Array.set(array, n, value);
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testSetBytesFromUnslicedBufferOfTheSameAllocator(boolean srcDirect, boolean dstDirect) {
        final byte[] bytes = new byte[64];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i + 1);
        }
        T allocator = newAllocator(false);
        final ByteBuf src = srcDirect ? allocator.directBuffer(64) : allocator.heapBuffer(64);
        final ByteBuf dst = dstDirect ? allocator.directBuffer(128) : allocator.heapBuffer(128);
        try {
            src.writeBytes(bytes);
            dst.writeZero(128);
            dst.setBytes(16, src, 4, 40);
            for (int i = 0; i < 128; i++) {
                byte expected = i >= 16 && i < 56 ? bytes[i - 16 + 4] : 0;
                assertEquals(expected, dst.getByte(i), "index " + i);
            }
            assertEquals(0, src.readerIndex());
            assertEquals(64, src.writerIndex());
        } finally {
            src.release();
            dst.release();
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testSetBytesAfterCapacityGrowth(boolean srcDirect, boolean dstDirect) {
        final byte[] bytes = new byte[300];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i * 7 + 1);
        }
        T allocator = newAllocator(false);
        final ByteBuf src = srcDirect ? allocator.directBuffer(16) : allocator.heapBuffer(16);
        final ByteBuf dst = dstDirect ? allocator.directBuffer(16) : allocator.heapBuffer(16);
        try {
            // both move to a bigger slot
            src.writeBytes(bytes, 0, 8).capacity(300).writerIndex(0).writeBytes(bytes);
            dst.writeZero(16).capacity(400).writerIndex(0).writeZero(400);
            dst.setBytes(10, src, 5, 290);
            dst.setBytes(300, bytes, 20, 50);
            for (int i = 0; i < 400; i++) {
                byte expected = 0;
                if (i >= 10 && i < 300) {
                    expected = bytes[i - 10 + 5];
                } else if (i >= 300 && i < 350) {
                    expected = bytes[i - 300 + 20];
                }
                assertEquals(expected, dst.getByte(i), "index " + i);
            }
        } finally {
            src.release();
            dst.release();
        }
    }
}
