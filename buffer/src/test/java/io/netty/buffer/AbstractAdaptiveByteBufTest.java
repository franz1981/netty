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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class AbstractAdaptiveByteBufTest extends AbstractPooledByteBufTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    /** Made by the first buffer: an instance whose test never runs makes none, and has none to close. */
    private AdaptiveByteBufAllocator allocator;

    @Override
    protected final ByteBuf alloc(int length, int maxCapacity) {
        if (allocator == null) {
            allocator = closer.add(new AdaptiveByteBufAllocator());
        }
        return alloc(allocator, length, maxCapacity);
    }

    protected abstract ByteBuf alloc(AdaptiveByteBufAllocator allocator, int length, int maxCapacity);

    @Disabled("Assumes the ByteBuf can be cast to PooledByteBuf")
    @Test
    @Override
    public void testMaxFastWritableBytes() {
    }

    @Disabled("Assumes the ByteBuf can be cast to PooledByteBuf")
    @Test
    @Override
    public void testEnsureWritableDoesntGrowTooMuch() {
    }
}
