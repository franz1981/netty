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
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The slice bitmap of a {@link Segment}: first-fit claims, releases that merge with free neighbours, and span buffers.
 */
final class SegmentTest {
    private static Segment segment(int slices) {
        return new Segment((AbstractByteBuf) Unpooled.buffer(slices * SLICE_SIZE_BYTES), SLICE_SIZE_BYTES);
    }

    /** Every length and every start of a lone span in an empty segment, both segment sizes. */
    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    void claimsTheLowestRunOfEveryLength(int slices) {
        Segment segment = segment(slices);
        for (int n = 1; n <= 32; n++) {
            assertEquals(0, segment.claim(n), "n=" + n);
            assertEquals(slices - n, segment.freeSlices());
            segment.release(0, n);
            assertTrue(segment.isWhollyFree());
            // Every start: occupy the slices below it, so the lowest run of n is there.
            for (int start = 1; start + n <= slices; start++) {
                assertEquals(0, segment.claim(start));
                assertEquals(start, segment.claim(n), "n=" + n + " start=" + start);
                segment.release(0, start);
                segment.release(start, n);
                assertTrue(segment.isWhollyFree());
            }
        }
    }

    /** A run never reaches past the last slice; a full segment and a too long request claim nothing. */
    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    void claimsNothingPastTheLastSlice(int slices) {
        Segment segment = segment(slices);
        assertEquals(0, segment.claim(slices - 3));
        assertEquals(-1, segment.claim(4), "3 slices left");
        assertEquals(slices - 3, segment.claim(3));
        assertEquals(0, segment.freeSlices());
        assertEquals(-1, segment.claim(1));
        segment.release(slices - 3, 3);
        assertEquals(slices - 1, segment.claim(1) + 2, "lowest of the three at the top");
    }

    /** Freed spans merge with their free neighbours: a longer run fits where three spans were. */
    @Test
    void freedSpansCoalesce() {
        Segment segment = segment(64);
        assertEquals(0, segment.claim(2));
        assertEquals(2, segment.claim(3));
        assertEquals(5, segment.claim(2));
        assertEquals(7, segment.claim(57));
        assertEquals(-1, segment.claim(4));
        segment.release(0, 2);
        assertEquals(-1, segment.claim(4), "two free slices only");
        segment.release(5, 2);
        assertEquals(-1, segment.claim(4), "two runs of two, not adjacent");
        segment.release(2, 3);
        assertEquals(0, segment.claim(7), "0..6 merged");
        segment.release(0, 7);
        segment.release(7, 57);
        assertTrue(segment.isWhollyFree());
    }

    @Test
    void releasingFreeSlicesThrows() {
        Segment segment = segment(64);
        assertEquals(0, segment.claim(4));
        segment.release(0, 4);
        assertThrows(IllegalStateException.class, () -> segment.release(0, 4));
        assertThrows(IllegalStateException.class, () -> segment.release(2, 1));
    }

    @Test
    void usedAndFreeSlicesAddUp() {
        Segment segment = segment(64);
        segment.claim(9);
        segment.claim(2);
        assertEquals(11, segment.usedSlices());
        assertEquals(53, segment.freeSlices());
        assertEquals(64L * SLICE_SIZE_BYTES, segment.capacity());
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
                segment.release(starts[k], lengths[k]);
                mark(used, starts[k], lengths[k], false);
                live--;
                starts[k] = starts[live];
                lengths[k] = lengths[live];
            } else {
                int n = 1 + random.nextInt(9);
                int expected = lowestRun(used, n);
                assertEquals(expected, segment.claim(n), "op " + op);
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

    /** A span claimed again at the same place with the same length gets the same buffer, over the segment's memory. */
    @Test
    void aSpanOfTheSameLengthReusesItsBuffer() {
        CountingSegmentSource source = new CountingSegmentSource();
        Segment segment = new Segment(source.allocateSegment(SEGMENT_SIZE), SLICE_SIZE_BYTES);
        try {
            AbstractByteBuf two = segment.span(source, 0, 2);
            assertEquals(2 * SLICE_SIZE_BYTES, two.capacity());
            assertSame(two, segment.span(source, 0, 2));
            AbstractByteBuf three = segment.span(source, 0, 3);
            assertNotSame(two, three);
            assertSame(three, segment.span(source, 0, 3));
            AbstractByteBuf fifth = segment.span(source, 5, 1);
            fifth.setByte(7, 42);
            assertEquals(42, segment.buffer.getByte(5 * SLICE_SIZE_BYTES + 7));
        } finally {
            segment.buffer.release();
        }
    }

    /** A coloured span starts that many bytes into its first slice, ends with its last, and is kept per colour. */
    @Test
    void aColouredSpanStartsInsideItsFirstSlice() {
        CountingSegmentSource source = new CountingSegmentSource();
        Segment segment = new Segment(source.allocateSegment(SEGMENT_SIZE), SLICE_SIZE_BYTES);
        try {
            AbstractByteBuf plain = segment.span(source, 3, 2);
            AbstractByteBuf coloured = segment.span(source, 3, 2, 192);
            assertNotSame(plain, coloured);
            assertEquals(2 * SLICE_SIZE_BYTES - 192, coloured.capacity());
            assertSame(coloured, segment.span(source, 3, 2, 192));
            coloured.setByte(0, 42);
            assertEquals(42, segment.buffer.getByte(3 * SLICE_SIZE_BYTES + 192));
            assertNotSame(coloured, segment.span(source, 3, 2, 256));
        } finally {
            segment.buffer.release();
        }
    }
}
