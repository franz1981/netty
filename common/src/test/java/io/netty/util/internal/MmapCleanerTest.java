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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class MmapCleanerTest {

    @BeforeEach
    void mmapAvailable() {
        assumeTrue(PlatformDependent.hasDirectMmap(), "mmap(2) direct buffers need Java 22+, Linux and native access");
    }

    @Test
    void allocateWriteReadClean() throws IOException {
        long pageSize = MmapCleaner.pageSize();
        int capacity = (int) (3 * pageSize + 17);
        long usedBefore = PlatformDependent.usedDirectMemory();
        CleanableDirectBuffer cleanable = PlatformDependent.allocateDirectMmap(capacity);
        long address = cleanable.memoryAddress();
        try {
            assertTrue(cleanable.hasMemoryAddress());
            assertEquals(0, address % pageSize, "a mapping is page-aligned");
            ByteBuffer buffer = cleanable.buffer();
            assertTrue(buffer.isDirect());
            assertEquals(capacity, buffer.capacity());
            assertEquals(address, PlatformDependent.directBufferAddress(buffer));
            if (usedBefore >= 0) {
                assertEquals(usedBefore + capacity, PlatformDependent.usedDirectMemory(),
                        "accounted by capacity, like every other cleaner");
            }
            assertTrue(isMapped(address), "mapped after allocate");
            assertTrue(isMapped(address + 3 * pageSize), "the last, partial page is mapped too");
            for (int i = 0; i < capacity; i++) {
                assertEquals(0, buffer.get(i), "anonymous mappings are zero-filled");
                buffer.put(i, (byte) i);
            }
            for (int i = 0; i < capacity; i++) {
                assertEquals((byte) i, buffer.get(i));
            }
        } finally {
            cleanable.clean();
        }
        assertFalse(isMapped(address), "clean() must munmap");
        if (usedBefore >= 0) {
            assertEquals(usedBefore, PlatformDependent.usedDirectMemory());
        }
    }

    @Test
    void purgeZeroesOnlyWholePagesInsideTheRange() {
        int pageSize = (int) MmapCleaner.pageSize();
        int capacity = 4 * pageSize;
        CleanableDirectBuffer cleanable = PlatformDependent.allocateDirectMmap(capacity);
        try {
            ByteBuffer buffer = cleanable.buffer();
            for (int i = 0; i < capacity; i++) {
                buffer.put(i, (byte) 1);
            }
            // From the middle of page 0 to the middle of page 3: only pages 1 and 2 are wholly inside.
            int offset = pageSize / 2;
            assertTrue(cleanable.purge(offset, 3 * pageSize));
            for (int i = 0; i < capacity; i++) {
                boolean purged = i >= pageSize && i < 3 * pageSize;
                assertEquals(purged ? 0 : 1, buffer.get(i), "byte " + i);
            }
            // Still mapped and writable.
            buffer.put(pageSize, (byte) 2);
            assertEquals(2, buffer.get(pageSize));
            assertTrue(cleanable.purge(1, 1), "a range with no whole page is a no-op");
            assertThrows(IndexOutOfBoundsException.class, () -> cleanable.purge(pageSize, capacity));
        } finally {
            cleanable.clean();
        }
    }

    @Test
    void otherCleanersDoNotPurge() {
        CleanableDirectBuffer cleanable = PlatformDependent.allocateDirect(64);
        try {
            assertFalse(cleanable.purge(0, 64));
        } finally {
            cleanable.clean();
        }
    }

    /**
     * Whether any mapping of this process covers {@code address}. The kernel merges adjacent anonymous mappings, so
     * the mapping is looked up by containment, not by its bounds.
     */
    static boolean isMapped(long address) throws IOException {
        for (String line : Files.readAllLines(Paths.get("/proc/self/maps"), StandardCharsets.US_ASCII)) {
            int dash = line.indexOf('-');
            int space = line.indexOf(' ', dash);
            long start = Long.parseUnsignedLong(line.substring(0, dash), 16);
            long end = Long.parseUnsignedLong(line.substring(dash + 1, space), 16);
            if (Long.compareUnsigned(address, start) >= 0 && Long.compareUnsigned(address, end) < 0) {
                return true;
            }
        }
        return false;
    }
}
