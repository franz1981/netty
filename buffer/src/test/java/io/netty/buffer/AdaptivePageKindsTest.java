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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The geometry of the size-class chunks: the page kinds per block, and the bound on what a chunk leaves unused at its
 * end, for the direct block (64 slices), the heap block (63) and the low-memory block (32).
 */
public class AdaptivePageKindsTest {
    @Test
    void pageKindsPerBlock() {
        assertArrayEquals(new int[] {1, 8, 32}, SizeClassTable.pageKinds(64, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 7, 21}, SizeClassTable.pageKinds(63, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 4, 16}, SizeClassTable.pageKinds(32, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 7}, SizeClassTable.pageKinds(7, SLICE_SIZE_BYTES));
        assertArrayEquals(new int[] {1, 3}, SizeClassTable.pageKinds(3, SLICE_SIZE_BYTES));
    }

    /** Every chunk leaves at most an eighth of its page at its end, but an exact fit's dropped buffer. */
    @Test
    void endWasteIsAtMostAnEighthButADroppedBuffer() {
        for (int blockSlices : new int[] {64, 63, 32}) {
            int[] kinds = SizeClassTable.pageKinds(blockSlices, SLICE_SIZE_BYTES);
            for (int sizeClass : SizeClassTable.SIZES) {
                int page = SizeClassTable.slicesPerChunk(sizeClass, kinds, SLICE_SIZE_BYTES)
                        * SLICE_SIZE_BYTES;
                int fit = page / sizeClass;
                int buffers = SizeClassTable.slotsPerChunk(sizeClass, page);
                assertTrue(page - fit * sizeClass <= page >>> 3, blockSlices + " slices, class " + sizeClass);
                assertTrue(buffers == fit || buffers == fit - 1, "class " + sizeClass);
            }
        }
    }
}
