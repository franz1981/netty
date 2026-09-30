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
package io.netty.util.internal;

import java.nio.ByteBuffer;

/**
 * The aligned part of a larger direct allocation: {@link #buffer()} starts at a multiple of the alignment and has
 * exactly the requested capacity; {@link #clean()} frees the whole allocation, whose size {@link #allocatedCapacity()}
 * reports. See {@link Cleaner#allocateAligned(int, int)}.
 */
final class AlignedCleanableDirectBuffer implements CleanableDirectBuffer {
    private final CleanableDirectBuffer whole;
    private final ByteBuffer aligned;
    private final boolean hasMemoryAddress;
    private final long memoryAddress;

    private AlignedCleanableDirectBuffer(CleanableDirectBuffer whole, ByteBuffer aligned, boolean hasMemoryAddress,
                                         long memoryAddress) {
        this.whole = whole;
        this.aligned = aligned;
        this.hasMemoryAddress = hasMemoryAddress;
        this.memoryAddress = memoryAddress;
    }

    /**
     * {@code capacity + alignment} bytes from {@code cleaner}, and the aligned part of them. When the address of the
     * allocation is known the aligned part is found by arithmetic; otherwise by {@code ByteBuffer.alignedSlice},
     * which also rounds the end down to the alignment, so the allocation is then
     * {@code capacity + alignment} rounded up to the alignment.
     *
     * @throws UnsupportedOperationException when neither is possible; nothing stays allocated then
     */
    static CleanableDirectBuffer allocate(Cleaner cleaner, int capacity, int alignment) {
        checkAlignment(capacity, alignment);
        CleanableDirectBuffer whole = cleaner.allocate(capacity + alignment);
        try {
            ByteBuffer buffer = whole.buffer();
            boolean known = whole.hasMemoryAddress();
            if (known || PlatformDependent.hasDirectByteBufferAddress(buffer)) {
                long base = known ? whole.memoryAddress() : PlatformDependent.directBufferAddress(buffer);
                long address = PlatformDependent.align(base, alignment);
                ByteBuffer aligned = PlatformDependent.offsetSlice(buffer, (int) (address - base), capacity);
                return new AlignedCleanableDirectBuffer(whole, aligned, true, address);
            }
            if (!PlatformDependent.hasAlignDirectByteBuffer()) {
                throw new UnsupportedOperationException("Cannot align a direct buffer on this platform");
            }
            int rounded = capacity + alignment - 1 & -alignment;
            if (rounded != capacity) {
                if (rounded > Integer.MAX_VALUE - alignment) {
                    throw new IllegalArgumentException("capacity " + capacity + " rounded up to " + alignment
                            + " overflows");
                }
                CleanableDirectBuffer larger = cleaner.allocate(rounded + alignment);
                whole.clean();
                whole = larger;
                buffer = whole.buffer();
            }
            ByteBuffer slice = PlatformDependent.alignDirectBuffer(buffer.duplicate(), alignment);
            slice.limit(capacity);
            ByteBuffer aligned = slice.slice();
            return new AlignedCleanableDirectBuffer(whole, aligned, false, 0);
        } catch (Throwable t) {
            whole.clean();
            throw t;
        }
    }

    static void checkAlignment(int capacity, int alignment) {
        ObjectUtil.checkPositiveOrZero(capacity, "capacity");
        if (alignment <= 0 || (alignment & alignment - 1) != 0) {
            throw new IllegalArgumentException("alignment: " + alignment + " (expected: a power of two)");
        }
        if (capacity > Integer.MAX_VALUE - alignment) {
            throw new IllegalArgumentException("capacity " + capacity + " + alignment " + alignment + " overflows");
        }
    }

    @Override
    public ByteBuffer buffer() {
        return aligned;
    }

    @Override
    public void clean() {
        whole.clean();
    }

    @Override
    public int allocatedCapacity() {
        return whole.allocatedCapacity();
    }

    @Override
    public boolean hasMemoryAddress() {
        return hasMemoryAddress;
    }

    @Override
    public long memoryAddress() {
        return memoryAddress;
    }
}
