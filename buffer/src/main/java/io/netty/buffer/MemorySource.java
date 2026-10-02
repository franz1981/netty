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
 * Where an allocator's memory comes from: its one-shot chunks, its page store's regions, and the views of their
 * blocks. libc {@code malloc} or {@code mmap} for the direct allocator, a {@code byte[]} for the heap allocator.
 * Implementations: the chunk allocators and test sources.
 */
interface MemorySource {
    /** The buffer of a one-shot chunk, or a block of {@link MallocRegionSource}: of any class. */
    AbstractByteBuf allocate(int initialCapacity, int maxCapacity);

    /**
     * A view of {@code block} (or of a region) that never frees memory, of the class of {@code block}, so that
     * the buffers reading a chunk see one class whatever the chunk. Or {@code block} itself, never released by its
     * spans, where a view cannot start at an offset (a heap buffer): a size-class chunk then hands out offsets from
     * its span's.
     */
    AbstractByteBuf view(AbstractByteBuf block, int offset, int length);

    /** Regions of many blocks ({@code mmap}), or {@code null}: none here. */
    default RegionSource regionSource() {
        return null;
    }

    /**
     * Regions of one block, one plain allocation each, for where {@link #regionSource()} has none or cannot map one;
     * {@code null}: none either.
     */
    default RegionSource mallocRegionSource() {
        return null;
    }
}
