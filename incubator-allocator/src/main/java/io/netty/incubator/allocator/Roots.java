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

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.CleanableDirectBuffer;
import io.netty.util.internal.PlatformDependent;

/**
 * Makes the root buffers the pooled buffers slice: every root is an {@link AbstractByteBuf} that is also a
 * {@link RootAccess}. The class follows {@link PlatformDependent#hasUnsafe()}, as {@code Unpooled} does.
 */
final class Roots {
    /** A heap root of {@code initialCapacity} bytes. */
    static AbstractByteBuf heap(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        return PlatformDependent.hasUnsafe() ?
                new RootUnsafeHeapByteBuf(alloc, initialCapacity, maxCapacity) :
                new RootHeapByteBuf(alloc, initialCapacity, maxCapacity);
    }

    /** A direct root over {@code memory}, which is cleaned once when the root is released. */
    static AbstractByteBuf direct(ByteBufAllocator alloc, CleanableDirectBuffer memory, int maxCapacity) {
        return PlatformDependent.hasUnsafe() ?
                new RootUnsafeDirectByteBuf(alloc, memory, maxCapacity) :
                new RootDirectByteBuf(alloc, memory, maxCapacity);
    }

    /** A direct root of {@code capacity} bytes from {@link PlatformDependent#allocateDirect(int, boolean)}. */
    static AbstractByteBuf direct(ByteBufAllocator alloc, int capacity, int maxCapacity) {
        return direct(alloc, PlatformDependent.allocateDirect(capacity, true), maxCapacity);
    }

    /** The address of {@code root}'s first byte, 0 for a heap root. */
    static long address(AbstractByteBuf root) {
        return ((RootAccess) root).address();
    }

    private Roots() { }
}
