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
 * One allocation from a {@link RegionSource}, carved into {@link #slots} segments, one per slot: the unit the
 * allocator accounts ({@link #capacity()} is what was allocated for it, alignment included). A slot is free while its
 * segment is back in the region: in no heap and not in the cache. Guarded by the {@link RegionPool}'s monitor.
 */
final class Region implements ChunkInfo {
    final AbstractByteBuf buffer;
    final int allocatedBytes;
    final int slots;
    final long allFree;
    /** Bit {@code i} set when slot {@code i} is free. */
    long freeSlots;
    /** Made when a slot is first taken, and reused. */
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

    /** The lowest free slot's segment. */
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
