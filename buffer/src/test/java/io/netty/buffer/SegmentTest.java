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

import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The slice bitmap of a {@link Segment}: first-fit claims, releases that merge with free neighbours, and span buffers.
 */
final class SegmentTest {
    /** The block of a region of one, whose releases arm a page store's purge. */
    private static Segment segment(int slices) {
        int size = slices * SLICE_SIZE_BYTES;
        CountingSegmentSource source = new CountingSegmentSource(true);
        PageStore store = newAllocator(source, size).pageStore;
        return new Region(store, source.allocateSegment(size), source.fallback, 1, source, store.config, true, 0)
                .blocks[0];
    }

    /** Every length and every start of a lone span in an empty segment, both segment sizes. */
    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    void claimsTheLowestRunOfEveryLength(int slices) {
        Segment segment = segment(slices);
        for (int n = 1; n <= 32; n++) {
            assertEquals(0, segment.claimRun(n), "n=" + n);
            assertEquals(slices - n, segment.freeSlices());
            segment.releaseRun(0, n, 0);
            assertTrue(segment.isWhollyFree());
            // Every start: occupy the slices below it, so the lowest run of n is there.
            for (int start = 1; start + n <= slices; start++) {
                assertEquals(0, segment.claimRun(start));
                assertEquals(start, segment.claimRun(n), "n=" + n + " start=" + start);
                segment.releaseRun(0, start, 0);
                segment.releaseRun(start, n, 0);
                assertTrue(segment.isWhollyFree());
            }
        }
    }

    /** A run never reaches past the last slice; a full segment and a too long request claim nothing. */
    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    void claimsNothingPastTheLastSlice(int slices) {
        Segment segment = segment(slices);
        assertEquals(0, segment.claimRun(slices - 3));
        assertEquals(-1, segment.claimRun(4), "3 slices left");
        assertEquals(slices - 3, segment.claimRun(3));
        assertEquals(0, segment.freeSlices());
        assertEquals(-1, segment.claimRun(1));
        segment.releaseRun(slices - 3, 3, 0);
        assertEquals(slices - 1, segment.claimRun(1) + 2, "lowest of the three at the top");
    }

    /** Freed spans merge with their free neighbours: a longer run fits where three spans were. */
    @Test
    void freedSpansCoalesce() {
        Segment segment = segment(64);
        assertEquals(0, segment.claimRun(2));
        assertEquals(2, segment.claimRun(3));
        assertEquals(5, segment.claimRun(2));
        assertEquals(7, segment.claimRun(57));
        assertEquals(-1, segment.claimRun(4));
        segment.releaseRun(0, 2, 0);
        assertEquals(-1, segment.claimRun(4), "two free slices only");
        segment.releaseRun(5, 2, 0);
        assertEquals(-1, segment.claimRun(4), "two runs of two, not adjacent");
        segment.releaseRun(2, 3, 0);
        assertEquals(0, segment.claimRun(7), "0..6 merged");
        segment.releaseRun(0, 7, 0);
        segment.releaseRun(7, 57, 0);
        assertTrue(segment.isWhollyFree());
    }

    @Test
    void releasingFreeSlicesThrows() {
        Segment segment = segment(64);
        assertEquals(0, segment.claimRun(4));
        segment.releaseRun(0, 4, 0);
        assertThrows(IllegalStateException.class, () -> segment.releaseRun(0, 4, 0));
        assertThrows(IllegalStateException.class, () -> segment.releaseRun(2, 1, 0));
    }

    @Test
    void usedAndFreeSlicesAddUp() {
        Segment segment = segment(64);
        segment.claimRun(9);
        segment.claimRun(2);
        assertEquals(11, segment.usedSlices());
        assertEquals(53, segment.freeSlices());
        assertEquals(64L * SLICE_SIZE_BYTES, segment.buffer.capacity());
    }

    @Test
    void firstFitFindsTheLowestRunLongEnough() {
        assertEquals(0, Segment.firstFit(-1L, 63));
        assertEquals(-1, Segment.firstFit(0L, 1));
        long free = 0b1111_0110L; // slices 1, 2 and 4 to 7
        assertEquals(1, Segment.firstFit(free, 1));
        assertEquals(1, Segment.firstFit(free, 2));
        assertEquals(4, Segment.firstFit(free, 3));
        assertEquals(4, Segment.firstFit(free, 4));
        assertEquals(-1, Segment.firstFit(free, 5));
        assertEquals(63, Segment.firstFit(1L << 63, 1));
    }

    /** Random claims and releases against a model: the bitmap is exactly the free slices, whatever the order. */
    @Test
    void randomClaimsAndReleasesKeepTheBitmapExact() {
        Segment segment = segment(64);
        boolean[] used = new boolean[64];
        int[] starts = new int[64];
        int[] lengths = new int[64];
        int live = 0;
        Random random = new Random(42);
        for (int op = 0; op < 100_000; op++) {
            if (live > 0 && random.nextBoolean()) {
                int k = random.nextInt(live);
                segment.releaseRun(starts[k], lengths[k], 0);
                mark(used, starts[k], lengths[k], false);
                live--;
                starts[k] = starts[live];
                lengths[k] = lengths[live];
            } else {
                int n = 1 + random.nextInt(9);
                int expected = lowestRun(used, n);
                assertEquals(expected, segment.claimRun(n), "op " + op);
                if (expected >= 0) {
                    mark(used, expected, n, true);
                    starts[live] = expected;
                    lengths[live] = n;
                    live++;
                }
            }
            long bits = 0;
            for (int i = 0; i < 64; i++) {
                bits |= used[i] ? 0 : 1L << i;
            }
            assertEquals(bits, segment.free, "op " + op);
            assertEquals(64 - Long.bitCount(bits), segment.usedSlices());
            assertEquals(bits == -1L, segment.isWhollyFree());
        }
    }

    private static void mark(boolean[] used, int start, int n, boolean value) {
        for (int i = start; i < start + n; i++) {
            used[i] = value;
        }
    }

    private static int lowestRun(boolean[] used, int n) {
        for (int start = 0; start + n <= used.length; start++) {
            int i = start;
            while (i < start + n && !used[i]) {
                i++;
            }
            if (i == start + n) {
                return start;
            }
        }
        return -1;
    }
}
