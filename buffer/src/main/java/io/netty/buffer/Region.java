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
 * One piece of memory cut into {@link #slots} blocks: {@code mmap}d, or one plain block, charged and purged
 * whole, if {@link #source} is {@code null}.
 */
final class Region {
    final PageStore store;
    final AbstractByteBuf buffer;
    final MmapRegionSource source;
    final int slots;
    final int length;
    final Segment[] blocks;
    final int index;
    /** Set under the store's lock; every block stays claimed once set. */
    volatile boolean released;

    Region(PageStore store, AbstractByteBuf buffer, MmapRegionSource source, int slots,
           PageStoreConfig config, boolean committed, long committedAt, int index) {
        assert slots > 0 && slots <= Long.SIZE;
        this.store = store;
        this.buffer = buffer;
        this.source = source;
        this.slots = slots;
        this.index = index;
        int size = config.segmentSize;
        length = slots * size;
        blocks = new Segment[slots];
        for (int slot = 0; slot < slots; slot++) {
            blocks[slot] = new Segment(buffer, slot * size, size, config.sliceSize, this, slot, committed, committedAt);
        }
    }

    int claimBlocks(int n) {
        Segment[] blocks = this.blocks;
        int first = 0;
        while (first + n <= blocks.length) {
            int end = first;
            while (end < first + n && blocks[end].isEmpty()) {
                end++;
            }
            if (end < first + n) {
                first = end + 1;
                continue;
            }
            int claimed = 0;
            while (claimed < n && blocks[first + claimed].claimWhole()) {
                claimed++;
            }
            if (claimed == n) {
                return first;
            }
            unclaimBlocks(first, claimed);
            store.armPurge(System.nanoTime());
            first += claimed + 1;
        }
        return -1;
    }

    void unclaimBlocks(int first, int n) {
        for (int i = first; i < first + n; i++) {
            Segment block = blocks[i];
            block.unclaim(block.allFree);
        }
    }

    int claimAll() {
        int claimed = 0;
        while (claimed < slots && blocks[claimed].claimWhole()) {
            claimed++;
        }
        return claimed;
    }

    boolean isEmpty() {
        for (Segment block : blocks) {
            if (!block.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /** How long, at {@code now}, the slice freed last has been free. Racy unless the caller claimed every block. */
    long shortestWait(long now) {
        long shortest = Long.MAX_VALUE;
        for (Segment block : blocks) {
            shortest = Math.min(shortest, block.shortestWait(now));
        }
        return shortest;
    }

    void release() {
        released = true;
        buffer.release();
    }

    @Override
    public String toString() {
        return "Region[" + index + ", blocks: " + slots + (released ? ", released]" : "]");
    }
}
