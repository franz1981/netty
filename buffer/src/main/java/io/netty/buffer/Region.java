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
 * A region: one allocation from the {@link RegionSource} that {@link #slots} segments are carved out of, one per
 * slot. It is what the allocator holds from its memory, so it is the unit of
 * {@link AdaptivePoolingAllocator#usedMemory()} and of the chunk events (its {@link #capacity()} is what was allocated
 * for it); the segments carved out of it fire none.
 * <p>
 * A slot is free while its segment is back in the region: neither in a heap nor in the {@link SegmentCache}.
 * Guarded by the {@link RegionPool}'s lock.
 */
final class Region implements ChunkInfo {
    final AbstractByteBuf buffer;
    final int allocatedBytes;
    final int slots;
    /** {@link #freeSlots} of a region whose segments are all back. */
    final long allFree;
    /** Bit {@code i} set when slot {@code i} is free. */
    long freeSlots;
    /** The segment of each slot, made when the slot is first taken and kept with the region. */
    private final Segment[] segments;

    Region(AbstractByteBuf buffer, int allocatedBytes, int slots) {
        assert slots > 0 && slots <= Long.SIZE;
        this.buffer = buffer;
        this.allocatedBytes = allocatedBytes;
        this.slots = slots;
        allFree = slots == Long.SIZE ? -1L : (1L << slots) - 1;
        freeSlots = allFree;
        segments = new Segment[slots];
    }

    /** Take the lowest free slot's segment. */
    Segment takeSlot(SegmentSource source, PageStoreConfig config) {
        assert freeSlots != 0;
        int slot = Long.numberOfTrailingZeros(freeSlots);
        freeSlots &= ~(1L << slot);
        Segment segment = segments[slot];
        if (segment == null) {
            int size = config.segmentSize;
            segment = new Segment(source.span(buffer, slot * size, size), config.sliceSize, this, slot);
            segments[slot] = segment;
        }
        return segment;
    }

    int freeSlotCount() {
        return Long.bitCount(freeSlots);
    }

    @Override
    public int capacity() {
        return allocatedBytes;
    }

    @Override
    public boolean isDirect() {
        return buffer.isDirect();
    }

    @Override
    public long memoryAddress() {
        return buffer._memoryAddress();
    }

    @Override
    public String toString() {
        return "Region[slots: " + slots + ", free: " + Long.bitCount(freeSlots) + ']';
    }
}
