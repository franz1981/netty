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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code indexOf} reads the root's memory itself; the shared tests only reach buffers at the start of their chunk.
 * The answer (or the exception) of every range, forward and backward, in and out of bounds, must be the one of
 * the base implementation over a copy of the same bytes.
 */
class PageStoreIndexOfTest {
    private static final byte[] NEEDLES = {0, 1, (byte) 0x80, (byte) 0xFF};

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    @ParameterizedTest(name = "direct: {0}")
    @ValueSource(booleans = {true, false})
    void sameAnswerAsTheBaseImplementation(boolean direct) {
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(direct, false));
        Random random = new Random(42);
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        try {
            // the first buffers push the next ones off the start of their chunk
            for (int i = 0; i < 3; i++) {
                held.add(allocator.buffer(40));
            }
            for (int capacity : new int[] {1, 7, 8, 9, 40}) {
                ByteBuf buf = allocator.buffer(capacity, capacity);
                held.add(buf);
                for (int density = 1; density <= 4; density++) {
                    byte[] bytes = new byte[capacity];
                    for (int i = 0; i < capacity; i++) {
                        // few needles at density 1, up to a buffer full of them
                        bytes[i] = random.nextInt(5) < density ? NEEDLES[random.nextInt(NEEDLES.length)] : 2;
                    }
                    buf.setBytes(0, bytes);
                    ByteBuf expected = Unpooled.wrappedBuffer(bytes);
                    for (int from = -1; from <= capacity + 1; from++) {
                        for (int to = -1; to <= capacity + 1; to++) {
                            for (byte needle : NEEDLES) {
                                assertEquals(indexOf(expected, from, to, needle), indexOf(buf, from, to, needle),
                                        "capacity " + capacity + " from " + from + " to " + to + " needle " + needle);
                            }
                        }
                    }
                }
            }
        } finally {
            for (ByteBuf buf : held) {
                buf.release();
            }
        }
    }

    /** The index, or the class of the exception thrown. */
    private static Object indexOf(ByteBuf buf, int from, int to, byte needle) {
        try {
            return buf.indexOf(from, to, needle);
        } catch (RuntimeException e) {
            return e.getClass();
        }
    }
}
