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

import io.netty.buffer.AdaptivePoolingAllocator.Chunk;
import io.netty.util.internal.PlatformDependent;

/**
 * {@link AllocateBufferEvent} / {@link FreeBufferEvent} / {@link ReallocateBufferEvent}, fired by every adaptive
 * buffer's init, deallocate and capacity growth.
 */
final class BufferEvents {
    private BufferEvents() {
    }

    static void allocated(AdaptiveByteBuf buf, Chunk chunk) {
        if (PlatformDependent.isJfrEnabled() && AllocateBufferEvent.isEventEnabled()) {
            AllocateBufferEvent event = new AllocateBufferEvent();
            if (event.shouldCommit()) {
                event.fill(buf, AdaptiveByteBufAllocator.class);
                event.chunkPooled = chunk.pooled;
                event.chunkThreadLocal = chunk.inThreadLocalMagazine();
                event.commit();
            }
        }
    }

    static void freed(AdaptiveByteBuf buf) {
        if (PlatformDependent.isJfrEnabled() && FreeBufferEvent.isEventEnabled()) {
            FreeBufferEvent event = new FreeBufferEvent();
            if (event.shouldCommit()) {
                event.fill(buf, AdaptiveByteBufAllocator.class);
                event.commit();
            }
        }
    }

    static void reallocated(AdaptiveByteBuf buf, int newCapacity) {
        if (PlatformDependent.isJfrEnabled() && ReallocateBufferEvent.isEventEnabled()) {
            ReallocateBufferEvent event = new ReallocateBufferEvent();
            if (event.shouldCommit()) {
                event.fill(buf, AdaptiveByteBufAllocator.class);
                event.newCapacity = newCapacity;
                event.commit();
            }
        }
    }
}
