/*
* Copyright 2017 The Netty Project
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
package io.netty.util.internal;

import java.nio.ByteBuffer;

/**
 * Allows to free direct {@link ByteBuffer}s.
 */
interface Cleaner {
    /**
     * Create a direct {@link ByteBuffer} and return it alongside its cleaning mechanism,
     * in a {@link CleanableDirectBuffer}.
     *
     * @param capacity The desired capacity of the direct buffer.
     * @return The new {@link CleanableDirectBuffer} instance.
     */
    CleanableDirectBuffer allocate(int capacity);

    /**
     * Create a direct {@link ByteBuffer} of exactly {@code capacity} bytes whose first byte is at an address that is a
     * multiple of {@code alignment}, and return it alongside its cleaning mechanism, which frees the whole allocation.
     * <p>
     * The default implementation allocates {@code capacity + alignment} bytes with {@link #allocate(int)} (rounded up
     * to the alignment when the allocation's address cannot be read) and returns the aligned part of them:
     * {@link CleanableDirectBuffer#allocatedCapacity()} then reports the bytes actually allocated, which is also what
     * the direct memory counter was charged. Implementations with an aligned allocation primitive may override it.
     *
     * @param capacity  the capacity of the returned buffer
     * @param alignment a power of two
     * @throws UnsupportedOperationException when this platform cannot tell where an aligned address is in a buffer
     *                                       (no memory address and no {@code ByteBuffer.alignedSlice}); nothing is
     *                                       left allocated then
     */
    default CleanableDirectBuffer allocateAligned(int capacity, int alignment) {
        return AlignedCleanableDirectBuffer.allocate(this, capacity, alignment);
    }

    /**
     * Reallocate a direct buffer with a new capacity. The old buffer is consumed and
     * must not be used after this call.
     * <p>
     * The default implementation allocates a new buffer, copies the data, and frees the old one.
     * Implementations may override this to provide more efficient reallocation (e.g. via
     * {@code Unsafe.reallocateMemory}).
     */
    default CleanableDirectBuffer reallocate(CleanableDirectBuffer old, int newCapacity) {
        CleanableDirectBuffer newBuf = allocate(newCapacity);
        ByteBuffer oldBB = old.buffer();
        ByteBuffer newBB = newBuf.buffer();
        int bytesToCopy = Math.min(oldBB.capacity(), newCapacity);
        oldBB.position(0).limit(bytesToCopy);
        newBB.position(0).limit(bytesToCopy);
        newBB.put(oldBB).clear();
        old.clean();
        return newBuf;
    }

    /**
     * Free a direct {@link ByteBuffer} if possible
     *
     * @deprecated Instead allocate buffers from {@link #allocate(int)}
     * and use the associated {@link CleanableDirectBuffer#clean()} method.
     */
    @Deprecated
    void freeDirectBuffer(ByteBuffer buffer);

    /**
     * Check if the clean operation is "relatively expensive".
     * Expensive clean operations are fine for pooling allocators, but should be avoided for unpooled buffers.
     * @return {@code true} if this Cleaner has an expensive clean
     * (i.e. {@link CleanableDirectBuffer#clean()}) operation.
     */
    boolean hasExpensiveClean();
}
