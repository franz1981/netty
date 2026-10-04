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

import org.junit.jupiter.api.Test;

import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The geometry of the size-class chunks: the page kinds per block, and per size class its chunk's slices, buffers and
 * colours in the direct block (64 slices) and the heap block (63 slices).
 */
public class AdaptivePageKindsTest {

    /**
     * Per size class: the class, then slices, buffers and colours in a 64-slice block, then the same in a 63-slice
     * block.
     */
    private static final int[][] TABLE = {
            {32, 1, 2048, 1, 1, 2048, 1},
            {64, 1, 1023, 2, 1, 1023, 2},
            {128, 1, 511, 3, 1, 511, 3},
            {256, 1, 255, 5, 1, 255, 5},
            {320, 1, 204, 5, 1, 204, 5},
            {384, 1, 170, 5, 1, 170, 5},
            {448, 1, 146, 3, 1, 146, 3},
            {512, 1, 127, 9, 1, 127, 9},
            {640, 1, 102, 5, 1, 102, 5},
            {768, 1, 85, 5, 1, 85, 5},
            {896, 1, 73, 3, 1, 73, 3},
            {1024, 1, 63, 16, 1, 63, 16},
            {1152, 1, 56, 16, 1, 56, 16},
            {1280, 1, 51, 5, 1, 51, 5},
            {1536, 1, 42, 16, 1, 42, 16},
            {1792, 1, 36, 16, 1, 36, 16},
            {2048, 1, 31, 16, 1, 31, 16},
            {2304, 1, 28, 16, 1, 28, 16},
            {2560, 1, 25, 16, 1, 25, 16},
            {3072, 1, 21, 16, 1, 21, 16},
            {3584, 1, 18, 16, 1, 18, 16},
            {4096, 1, 15, 16, 1, 15, 16},
            {4352, 1, 15, 5, 1, 15, 5},
            {5120, 1, 12, 16, 1, 12, 16},
            {6144, 1, 10, 16, 1, 10, 16},
            {7168, 1, 9, 16, 1, 9, 16},
            {8192, 8, 63, 16, 7, 55, 16},
            {8704, 1, 7, 16, 1, 7, 16},
            {10240, 1, 6, 16, 1, 6, 16},
            {12288, 1, 5, 16, 1, 5, 16},
            {14336, 1, 4, 16, 1, 4, 16},
            {16384, 8, 31, 16, 7, 27, 16},
            {16896, 8, 31, 9, 7, 27, 16},
            {24576, 8, 21, 16, 7, 18, 16},
            {32768, 8, 15, 16, 7, 13, 16},
            {33792, 8, 15, 16, 7, 13, 16},
            {49152, 8, 10, 16, 7, 9, 16},
            {65536, 8, 7, 16, 7, 6, 16},
            {67584, 8, 7, 16, 7, 6, 16},
            {98304, 8, 5, 16, 21, 13, 16},
            {131072, 8, 3, 16, 21, 10, 16},
            {135168, 32, 15, 16, 21, 10, 16},
    };

    @Test
    void pageKindsPerBlock() {
        assertArrayEquals(new int[] {1, 8, 32}, SizeClassTable.pageKinds(64, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 7, 21}, SizeClassTable.pageKinds(63, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 4, 16}, SizeClassTable.pageKinds(32, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 7}, SizeClassTable.pageKinds(7, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 3}, SizeClassTable.pageKinds(3, SLICE_SIZE_BYTES));
    }

    @Test
    void everySizeClassIsInTheTable() {
        int[] sizeClasses = SizeClassTable.SIZES.clone();
        assertEquals(TABLE.length, sizeClasses.length);
        for (int i = 0; i < TABLE.length; i++) {
            assertEquals(TABLE[i][0], sizeClasses[i]);
        }
    }

    @Test
    void directBlockOf64Slices() {
        assertTable(64, 1);
    }

    @Test
    void heapBlockOf63Slices() {
        assertTable(63, 4);
    }

    /** Low memory: 32-slice blocks; the exact fits under 16 buffers take the 4-slice kind. */
    @Test
    void lowMemoryBlockOf32Slices() {
        int[] kinds = SizeClassTable.pageKinds(32, SLICE_SIZE_BYTES);
        assertChunk(kinds, 8192, 4, 31, 16);
        assertChunk(kinds, 16384, 4, 15, 16);
        assertChunk(kinds, 16896, 4, 15, 16);
    }

    /** Every chunk leaves at most an eighth of its page at its end, but an exact fit's dropped buffer. */
    @Test
    void endWasteIsAtMostAnEighthButADroppedBuffer() {
        for (int blockSlices : new int[] {64, 63, 32}) {
            int[] kinds = SizeClassTable.pageKinds(blockSlices, SLICE_SIZE_BYTES);
            for (int sizeClass : SizeClassTable.SIZES) {
                int page = SizeClassTable.chunkSlicesOf(sizeClass, kinds, SLICE_SIZE_BYTES)
                        * SLICE_SIZE_BYTES;
                int fit = page / sizeClass;
                int buffers = SizeClassTable.chunkBuffersOf(sizeClass, page);
                assertTrue(page - fit * sizeClass <= page >>> 3, blockSlices + " slices, class " + sizeClass);
                assertTrue(buffers == fit || buffers == fit - 1, "class " + sizeClass);
            }
        }
    }

    private static void assertTable(int blockSlices, int column) {
        int[] kinds = SizeClassTable.pageKinds(blockSlices, SLICE_SIZE_BYTES);
        for (int[] row : TABLE) {
            assertChunk(kinds, row[0], row[column], row[column + 1], row[column + 2]);
        }
    }

    private static void assertChunk(int[] kinds, int sizeClass, int slices, int buffers, int colours) {
        int chunkSlices = SizeClassTable.chunkSlicesOf(sizeClass, kinds, SLICE_SIZE_BYTES);
        assertEquals(slices, chunkSlices, "slices of class " + sizeClass);
        int chunkSize = chunkSlices * SLICE_SIZE_BYTES;
        assertEquals(buffers, SizeClassTable.chunkBuffersOf(sizeClass, chunkSize),
                "buffers of class " + sizeClass);
        assertEquals(colours, SizeClassTable.chunkColoursOf(sizeClass, chunkSize),
                "colours of class " + sizeClass);
    }
}
