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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link Cleaner#allocateAligned(int, int)}: the buffer has exactly the capacity asked for and starts aligned, the
 * memory counter is charged what was allocated, and {@link CleanableDirectBuffer#clean()} frees all of it.
 */
public class AlignedDirectAllocationTest {
    private static final int MIB = 1024 * 1024;

    private static long address(CleanableDirectBuffer buffer) {
        if (buffer.hasMemoryAddress()) {
            return buffer.memoryAddress();
        }
        assumeTrue(PlatformDependent.hasDirectByteBufferAddress(buffer.buffer()), "no way to read the address");
        return PlatformDependent.directBufferAddress(buffer.buffer());
    }

    /** Writes at both ends: the whole capacity is usable. */
    private static void touchEnds(ByteBuffer buffer) {
        int last = buffer.capacity() - 1;
        buffer.put(0, (byte) 1);
        buffer.put(last, (byte) 2);
        assertEquals(1, buffer.get(0));
        assertEquals(2, buffer.get(last));
    }

    private static void assertAligned(CleanableDirectBuffer buffer, int capacity, int alignment) {
        assertEquals(capacity, buffer.buffer().capacity());
        assertEquals(0, address(buffer) & (alignment - 1), "aligned to " + alignment);
        if (buffer.hasMemoryAddress() && PlatformDependent.hasDirectByteBufferAddress(buffer.buffer())) {
            assertEquals(PlatformDependent.directBufferAddress(buffer.buffer()), buffer.memoryAddress());
        }
        touchEnds(buffer.buffer());
    }

    /** Whatever cleaner the platform picked. */
    @Test
    void platformAllocatesAligned() {
        for (int[] c : new int[][] {{4 * MIB, 2 * MIB}, {36 * MIB, 2 * MIB}, {1000, 4096}, {64, 64}}) {
            CleanableDirectBuffer buffer;
            try {
                buffer = PlatformDependent.allocateDirectAligned(c[0], c[1]);
            } catch (UnsupportedOperationException e) {
                assumeTrue(false, "cannot align on this platform: " + e);
                return;
            }
            try {
                assertAligned(buffer, c[0], c[1]);
                int allocated = buffer.allocatedCapacity();
                assertTrue(allocated == c[0] || allocated == c[0] + c[1], "allocated " + allocated);
            } finally {
                buffer.clean();
            }
        }
    }

    /** Unsafe without a cleaner: over-allocates, charges the counter the whole, and gives it all back. */
    @Test
    void directCleanerOverAllocates() {
        assumeTrue(PlatformDependent.hasUnsafe() && PlatformDependent0.hasDirectBufferNoCleanerConstructor());
        Cleaner cleaner = new DirectCleaner();
        long before = PlatformDependent.usedDirectMemory();
        CleanableDirectBuffer buffer = cleaner.allocateAligned(3 * MIB, 2 * MIB);
        try {
            assertAligned(buffer, 3 * MIB, 2 * MIB);
            assertEquals(5 * MIB, buffer.allocatedCapacity());
            if (before >= 0) {
                assertEquals(before + 5 * MIB, PlatformDependent.usedDirectMemory());
            }
        } finally {
            buffer.clean();
        }
        if (before >= 0) {
            assertEquals(before, PlatformDependent.usedDirectMemory());
        }
    }

    /**
     * The default method over a cleaner whose buffers have no known address: the aligned slice is found by
     * {@code alignedSlice} (or Unsafe), and clean() is the whole buffer's.
     */
    @Test
    void defaultMethodWithoutAddress() {
        final AtomicInteger cleaned = new AtomicInteger();
        Cleaner cleaner = new Cleaner() {
            @Override
            public CleanableDirectBuffer allocate(final int capacity) {
                final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
                return new CleanableDirectBuffer() {
                    @Override
                    public ByteBuffer buffer() {
                        return buffer;
                    }

                    @Override
                    public void clean() {
                        cleaned.incrementAndGet();
                    }
                };
            }

            @Override
            public void freeDirectBuffer(ByteBuffer buffer) {
            }

            @Override
            public boolean hasExpensiveClean() {
                return false;
            }
        };
        CleanableDirectBuffer buffer;
        try {
            buffer = cleaner.allocateAligned(MIB, 64 * 1024);
        } catch (UnsupportedOperationException e) {
            assumeTrue(false, "cannot align on this platform: " + e);
            return;
        }
        assertAligned(buffer, MIB, 64 * 1024);
        assertEquals(MIB + 64 * 1024, buffer.allocatedCapacity());
        buffer.clean();
        assertEquals(1, cleaned.get());
    }

    /** aligned_alloc through the libc linker: exact size when it is a multiple of the alignment. */
    @Test
    void linkerUsesAlignedAlloc() {
        assumeTrue(CleanerJava24Linker.isSupported(), "the libc linker cleaner needs Java 24+ with native access");
        assertTrue(CleanerJava24Linker.hasAlignedAlloc(), "aligned_alloc links wherever malloc does (C11)");
        Cleaner cleaner = new CleanerJava24Linker();
        long before = PlatformDependent.usedDirectMemory();
        CleanableDirectBuffer exact = cleaner.allocateAligned(36 * MIB, 2 * MIB);
        try {
            assertAligned(exact, 36 * MIB, 2 * MIB);
            assertEquals(36 * MIB, exact.allocatedCapacity(), "exact: aligned_alloc");
            if (before >= 0) {
                assertEquals(before + exact.allocatedCapacity(), PlatformDependent.usedDirectMemory());
            }
        } finally {
            exact.clean();
        }
        if (before >= 0) {
            assertEquals(before, PlatformDependent.usedDirectMemory());
        }
        // Not a multiple of the alignment: malloc'd with room, and the aligned part.
        CleanableDirectBuffer odd = cleaner.allocateAligned(3 * MIB + 4096, 2 * MIB);
        try {
            assertAligned(odd, 3 * MIB + 4096, 2 * MIB);
            assertEquals(5 * MIB + 4096, odd.allocatedCapacity());
        } finally {
            odd.clean();
        }
        if (before >= 0) {
            assertEquals(before, PlatformDependent.usedDirectMemory());
        }
    }

    @Test
    void rejectsBadArguments() {
        assertThrows(IllegalArgumentException.class, () -> PlatformDependent.allocateDirectAligned(1024, 0));
        assertThrows(IllegalArgumentException.class, () -> PlatformDependent.allocateDirectAligned(1024, 3));
        assertThrows(IllegalArgumentException.class, () -> PlatformDependent.allocateDirectAligned(1024, -8));
        assertThrows(IllegalArgumentException.class, () -> PlatformDependent.allocateDirectAligned(-1, 64));
        assertThrows(IllegalArgumentException.class,
                () -> PlatformDependent.allocateDirectAligned(Integer.MAX_VALUE - 10, 64));
    }
}
