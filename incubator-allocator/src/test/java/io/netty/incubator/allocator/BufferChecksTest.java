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
package io.netty.incubator.allocator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** That the checks of {@link BufferChecks} fail on what they are there to find, and pass on what they let be. */
class BufferChecksTest {
    @Test
    void overlappingBuffersAreNotDisjoint() {
        ByteBuf direct = Unpooled.directBuffer(100);
        ByteBuf heap = Unpooled.buffer(100);
        try {
            ByteBuf directTail = direct.slice(50, 50);
            ByteBuf heapHead = heap.slice(0, 10);
            assertThrows(AssertionError.class, () -> BufferChecks.assertDisjoint(Arrays.asList(direct, directTail)));
            assertThrows(AssertionError.class, () -> BufferChecks.assertDisjoint(Arrays.asList(heap, heapHead)));
        } finally {
            direct.release();
            heap.release();
        }
    }

    @Test
    void buffersOfTheirOwnMemoryAreDisjoint() {
        ByteBuf first = Unpooled.buffer(100);
        ByteBuf second = Unpooled.buffer(100);
        ByteBuf direct = Unpooled.directBuffer(100);
        ByteBuf otherDirect = Unpooled.directBuffer(100);
        try {
            BufferChecks.assertDisjoint(Arrays.asList(first, second, direct, otherDirect));
            // Two halves of one buffer share none either.
            BufferChecks.assertDisjoint(Arrays.asList(first.slice(0, 50), first.slice(50, 50)));
        } finally {
            first.release();
            second.release();
            direct.release();
            otherDirect.release();
        }
    }

    @Test
    void aBufferVerifiesWithItsOwnPatternOnly() {
        for (int size : new int[] {1, 7, 8, 9, 100}) {
            ByteBuf buf = Unpooled.buffer(size);
            ByteBuf twoParts = Unpooled.buffer(size);
            try {
                BufferChecks.stamp(buf, 5);
                BufferChecks.verify(buf, 5);
                assertThrows(AssertionError.class, () -> BufferChecks.verify(buf, 6));
                // Stamped in two parts is stamped at once.
                BufferChecks.stamp(twoParts, 5, 0, size / 2);
                BufferChecks.stamp(twoParts, 5, size / 2, size);
                BufferChecks.verify(twoParts, 5);
                // One byte altered is found, at either end.
                buf.setByte(size - 1, buf.getByte(size - 1) + 1);
                assertThrows(AssertionError.class, () -> BufferChecks.verify(buf, 5));
                buf.setByte(size - 1, buf.getByte(size - 1) - 1);
                buf.setByte(0, buf.getByte(0) + 1);
                assertThrows(AssertionError.class, () -> BufferChecks.verify(buf, 5));
            } finally {
                buf.release();
                twoParts.release();
            }
        }
    }
}
