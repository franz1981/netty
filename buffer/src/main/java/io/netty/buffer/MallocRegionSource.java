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
 * Regions of one block each, plain direct buffers of a {@link AdaptivePoolingAllocator.ChunkAllocator}: libc
 * {@code malloc} behind {@link UnsafeByteBufUtil#newDirectByteBuf}, charged to the direct memory limit by their
 * allocation, as mimalloc's Java port allocates its 4 MiB segments. No part of a block can go back; a wholly idle one
 * goes back by its release ({@code free}), and whether that returns memory to the OS is the libc allocator's business.
 * For the direct allocator where {@code mmap} regions cannot be had (before Java 22, without native access) or are
 * turned off.
 */
final class MallocRegionSource implements RegionSource {
    private final AdaptivePoolingAllocator.ChunkAllocator allocator;

    MallocRegionSource(AdaptivePoolingAllocator.ChunkAllocator allocator) {
        this.allocator = allocator;
    }

    /** {@code alignment} is not honoured: libc's own. */
    @Override
    public AbstractByteBuf allocateRegion(int size, int alignment) {
        return allocator.allocate(size, size);
    }

    @Override
    public boolean canPurgeSlices() {
        return false;
    }

    @Override
    public void purge(AbstractByteBuf region, int offset, int length) {
        throw new UnsupportedOperationException("a malloc'd region goes back whole only");
    }
}
