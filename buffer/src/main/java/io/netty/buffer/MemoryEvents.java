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

import io.netty.util.internal.PlatformDependent;

/** {@link AllocateChunkEvent} / {@link FreeChunkEvent}, fired by the adaptive allocator's commit and release paths. */
final class MemoryEvents {
    private MemoryEvents() {
    }

    static void allocated(long address, int bytes, boolean direct, boolean pooled, boolean threadLocal) {
        if (PlatformDependent.isJfrEnabled() && AllocateChunkEvent.isEventEnabled()) {
            AllocateChunkEvent event = new AllocateChunkEvent();
            if (event.shouldCommit()) {
                event.allocatorType = AdaptiveByteBufAllocator.class;
                event.capacity = bytes;
                event.direct = direct;
                event.address = address;
                event.pooled = pooled;
                event.threadLocal = threadLocal;
                event.commit();
            }
        }
    }

    static void freed(long address, int bytes, boolean direct, boolean pooled) {
        if (PlatformDependent.isJfrEnabled() && FreeChunkEvent.isEventEnabled()) {
            FreeChunkEvent event = new FreeChunkEvent();
            if (event.shouldCommit()) {
                event.allocatorType = AdaptiveByteBufAllocator.class;
                event.capacity = bytes;
                event.direct = direct;
                event.address = address;
                event.pooled = pooled;
                event.commit();
            }
        }
    }
}
