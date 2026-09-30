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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * One mapping from a {@link RegionSource}, cut into {@link #slots} segments at fixed offsets. Its slots are taken and
 * given back from any thread by a CAS on the {@link #free} bitmap: a cleared bit is owned by exactly one thread (the
 * heap that took the slot, or its releaser), which alone touches the slot's {@link #segments} and
 * {@link #committed} entries; the CAS that gives the bit back publishes them to the next owner. A region lives as
 * long as its {@link PageStore}.
 */
final class Region {
    private static final AtomicLongFieldUpdater<Region> FREE = AtomicLongFieldUpdater.newUpdater(Region.class, "free");

    final AbstractByteBuf buffer;
    final int slots;
    final long allSlots;
    /** Bit {@code i} set when slot {@code i} is free: in no heap. */
    volatile long free;
    /** The segment of each slot, made the first time it is taken, then reused. Slot owner only. */
    private final Segment[] segments;
    /** Whether the slot's memory was touched since the region was mapped: counted in the used memory. Slot owner. */
    final boolean[] committed;

    Region(AbstractByteBuf buffer, int slots) {
        assert slots > 0 && slots <= Long.SIZE;
        this.buffer = buffer;
        this.slots = slots;
        allSlots = slots == Long.SIZE ? -1L : (1L << slots) - 1;
        free = allSlots;
        segments = new Segment[slots];
        committed = new boolean[slots];
    }

    /** Takes the lowest free slot: returns it, or -1 when none is free. Lock-free. */
    int takeSlot() {
        for (;;) {
            long current = free;
            if (current == 0) {
                return -1;
            }
            int slot = Long.numberOfTrailingZeros(current);
            if (FREE.compareAndSet(this, current, current & ~(1L << slot))) {
                return slot;
            }
        }
    }

    /** Frees {@code slot}, which the caller owns. Lock-free. */
    void giveBack(int slot) {
        long bit = 1L << slot;
        for (;;) {
            long current = free;
            if ((current & bit) != 0) {
                throw new IllegalStateException("slot " + slot + " is already free: " + Long.toHexString(current));
            }
            if (FREE.compareAndSet(this, current, current | bit)) {
                return;
            }
        }
    }

    /** Slot owner only. The slot's segment, made on first use as a view of the region. */
    Segment segment(int slot, SegmentSource source, PageStoreConfig config) {
        Segment segment = segments[slot];
        if (segment == null) {
            int size = config.segmentSize;
            segment = new Segment(source.span(buffer, slot * size, size), config.sliceSize, this, slot);
            segments[slot] = segment;
        }
        return segment;
    }

    /** For the close, when no slot has an owner any more. */
    Segment segmentOrNull(int slot) {
        return segments[slot];
    }

    int freeSlotCount() {
        return Long.bitCount(free);
    }

    @Override
    public String toString() {
        return "Region[slots: " + slots + ", free: " + freeSlotCount() + ']';
    }
}
