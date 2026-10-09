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

/**
 * Where an allocator's memory comes from when it is not carved out of an {@code mmap} region: the one-shot chunks
 * of single buffers, and the {@link PageStore}'s regions of one block where no {@link MmapRegionSource} is
 * available. libc {@code malloc} for the direct allocator, a {@code byte[]} for the heap allocator. Called from any
 * thread; implementations are the chunk allocators and test sources.
 */
interface MemorySource {
    /** The buffer of a one-shot chunk, or of a region of one block: of any class. */
    AbstractByteBuf allocate(int initialCapacity, int maxCapacity);
}
