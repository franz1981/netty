/*
 * Copyright 2024 The Netty Project
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

import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptivePoolingAllocatorTest {
    @Test
    void sizeClassComputations() throws Exception {
        final int[] sizeClasses = SizeClassTable.SIZES.clone();
        for (int sizeClassIndex = 0; sizeClassIndex < sizeClasses.length; sizeClassIndex++) {
            final int previousSizeIncluded = sizeClassIndex == 0? 0 : sizeClasses[sizeClassIndex - 1] + 1;
            assertSizeClassOf(sizeClassIndex, previousSizeIncluded, sizeClasses[sizeClassIndex]);
        }
        // beyond the last size class, we return the size class array's length
        assertSizeClassOf(sizeClasses.length, sizeClasses[sizeClasses.length - 1] + 1,
                          sizeClasses[sizeClasses.length - 1] + 1);
    }

    /**
     * A released segment's offset becomes its index by a multiply and a shift: exact for every segment of every size
     * class, whatever the span's start in its block and the chunk's colour.
     */
    @Test
    void segmentIndexIsExactForEverySizeClassAndColour() {
        PageStoreConfig config = PageStoreConfig.directDefaults();
        int slice = config.sliceSize;
        int block = config.segmentSize;
        int mostSegments = 0;
        for (int sizeClass : SizeClassTable.SIZES) {
            int chunkSize = PageStoreTestSupport.chunkSize(sizeClass, config);
            int segments = chunkSize / sizeClass;
            mostSegments = Math.max(mostSegments, segments);
            long recip = SizeClassedChunk.indexReciprocal(sizeClass);
            int lastSpanStart = block / slice - (chunkSize + slice - 1) / slice;
            for (int colour = 0; colour < 16 * 64; colour += 64) {
                for (int spanStart : new int[] {0, lastSpanStart}) {
                    int base = spanStart * slice + colour;
                    for (int i = 0; i < segments; i++) {
                        int offset = base + i * sizeClass;
                        assertEquals(i, SizeClassedChunk.index(offset - base, recip),
                                "size class " + sizeClass + ", colour " + colour + ", segment " + i);
                    }
                }
            }
        }
        assertTrue(mostSegments <= SizeClassedChunk.MAX_SLOTS);
    }

    private static void assertSizeClassOf(int expectedSizeClass, int previousSizeIncluded, int maxSizeIncluded) {
        for (int size = previousSizeIncluded; size <= maxSizeIncluded; size++) {
            int sizeToTest = size;
            assertEquals(expectedSizeClass, SizeClassTable.sizeClassIndex(size),
                         () -> "size = " + sizeToTest);
        }
    }
}
