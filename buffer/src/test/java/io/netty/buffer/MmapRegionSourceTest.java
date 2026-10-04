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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link MmapRegionSource}, where it is available (Java 22+ on Linux with native access, e.g. the buffer module's
 * Java 24+ native-access test run): aligned exact mappings, purges that zero the range and drop the process's
 * resident memory, and the unmap on release.
 */
@Isolated("Reads the process's resident memory, which concurrent tests would move")
final class MmapRegionSourceTest {
    private static final int MIB = 1024 * 1024;
    private static final int REGION_SIZE = 64 * MIB;
    private static final int ALIGNMENT = 2 * MIB;

    private MmapRegionSource source;

    @BeforeEach
    void setUp() {
        assumeTrue(MmapRegionSource.isAvailable(), "mmap(2) regions are not available here");
        source = new MmapRegionSource(UnpooledByteBufAllocator.DEFAULT);
    }

    @Test
    void regionsAreAlignedExactAndZero() {
        AbstractByteBuf region = source.allocateRegion(REGION_SIZE, ALIGNMENT);
        try {
            long address = MmapRegionSource.addressOf(region);
            assertEquals(0, address & ALIGNMENT - 1);
            assertEquals(REGION_SIZE, region.capacity());
            assertTrue(region.isDirect());
            assertEquals(0, region.getLong(0));
            assertEquals(0, region.getLong(REGION_SIZE - 8));
            region.setLong(REGION_SIZE - 8, 42);
            assertEquals(42, region.getLong(REGION_SIZE - 8));
        } finally {
            region.release();
        }
    }

    /** One call purges the whole range: it reads zero again, and the process's resident memory drops by it. */
    @Test
    void purgeZeroesAndGivesTheMemoryBack() throws IOException {
        AbstractByteBuf region = source.allocateRegion(REGION_SIZE, ALIGNMENT);
        try {
            int stride = 4096;
            for (int offset = 0; offset < REGION_SIZE; offset += stride) {
                region.setLong(offset, offset + 1L);
            }
            long touched = residentKiB();
            source.purge(region, 0, REGION_SIZE);
            long purged = residentKiB();
            long droppedKiB = touched - purged;
            assertTrue(droppedKiB >= REGION_SIZE / 1024 * 3 / 4,
                    "resident memory went from " + touched + " KiB to " + purged + " KiB");
            for (int offset = 0; offset < REGION_SIZE; offset += stride) {
                assertEquals(0, region.getLong(offset), "offset " + offset);
            }
        } finally {
            region.release();
        }
    }

    /** A purge of a part leaves the rest as it was; an out-of-bounds range is rejected, a misaligned address fails. */
    @Test
    void purgeOfAPartLeavesTheRest() {
        AbstractByteBuf region = source.allocateRegion(8 * MIB, ALIGNMENT);
        try {
            region.setLong(MIB - 8, 1);
            region.setLong(MIB, 2);
            region.setLong(3 * MIB, 3);
            source.purge(region, MIB, 2 * MIB);
            assertEquals(1, region.getLong(MIB - 8));
            assertEquals(0, region.getLong(MIB));
            assertEquals(3, region.getLong(3 * MIB));
            assertThrows(IllegalStateException.class, () -> source.purge(region, 1, MIB));
            assertThrows(IllegalArgumentException.class, () -> source.purge(region, 0, 16 * MIB));
        } finally {
            region.release();
        }
    }

    /**
     * A failed call says why with its errno: ENOMEM (12) for a mapping larger than the address space, EINVAL (22) for
     * an address that is not on a page.
     */
    @Test
    void failuresCarryTheErrno() {
        OutOfMemoryError tooLarge = assertThrows(OutOfMemoryError.class, () -> MmapRegionSource.mmap(1L << 62));
        assertTrue(tooLarge.getMessage().endsWith(": errno 12"), tooLarge.getMessage());
        IllegalStateException unmap = assertThrows(IllegalStateException.class,
                () -> MmapRegionSource.munmap(1, 4096));
        assertTrue(unmap.getMessage().endsWith(": errno 22"), unmap.getMessage());
        IllegalStateException advise = assertThrows(IllegalStateException.class,
                () -> MmapRegionSource.madviseDontNeed(1, 4096));
        assertTrue(advise.getMessage().endsWith(": errno 22"), advise.getMessage());
    }

    /** The region is mapped while it lives, and unmapped once released. */
    @Test
    void releaseUnmaps() throws IOException {
        AbstractByteBuf region = source.allocateRegion(REGION_SIZE, ALIGNMENT);
        long start = MmapRegionSource.addressOf(region);
        long end = start + REGION_SIZE;
        assertTrue(mapped(start, end), "mapped while it lives");
        region.release();
        assertFalse(overlapsAnyMapping(start, end), "unmapped once released");
    }

    /** {@code Rss} of {@code /proc/self/smaps_rollup}, else {@code VmRSS} of {@code /proc/self/status}, in KiB. */
    static long residentKiB() throws IOException {
        try {
            return field("/proc/self/smaps_rollup", "Rss:");
        } catch (IOException e) {
            return field("/proc/self/status", "VmRSS:");
        }
    }

    private static long field(String file, String name) throws IOException {
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            for (String line = in.readLine(); line != null; line = in.readLine()) {
                if (line.startsWith(name)) {
                    return Long.parseLong(line.substring(name.length()).trim().split("\\s+")[0]);
                }
            }
        }
        throw new IOException(name + " not in " + file);
    }

    /** Whether {@code [start, end)} lies inside one mapping of {@code /proc/self/maps}. */
    private static boolean mapped(long start, long end) throws IOException {
        for (long[] range : maps()) {
            if (range[0] <= start && end <= range[1]) {
                return true;
            }
        }
        return false;
    }

    private static boolean overlapsAnyMapping(long start, long end) throws IOException {
        for (long[] range : maps()) {
            if (range[0] < end && start < range[1]) {
                return true;
            }
        }
        return false;
    }

    private static List<long[]> maps() throws IOException {
        List<long[]> ranges = new ArrayList<long[]>();
        try (BufferedReader in = new BufferedReader(new FileReader("/proc/self/maps"))) {
            for (String line = in.readLine(); line != null; line = in.readLine()) {
                String span = line.substring(0, line.indexOf(' '));
                int dash = span.indexOf('-');
                ranges.add(new long[] {Long.parseUnsignedLong(span.substring(0, dash), 16),
                        Long.parseUnsignedLong(span.substring(dash + 1), 16)});
            }
        }
        return ranges;
    }
}
