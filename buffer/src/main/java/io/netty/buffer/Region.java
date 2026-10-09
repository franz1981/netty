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
 * One piece of a {@link PageStore}'s memory, cut into {@link #slots} blocks ({@link Segment}). Either mapped by
 * {@link #source}, many blocks whose free slices the purger gives back in place; or, with {@link #source}
 * {@code null}, one block from the allocator's {@link MemorySource}, charged whole when added and given back whole
 * once every slice is idle. Claims and releases touch only the blocks; this class is touched under the store's
 * lock, or racily by the purger before it claims every block.
 */
final class Region {
    final PageStore store;
    final AbstractByteBuf buffer;
    final MmapRegionSource source;
    final int slots;
    final int length;
    final Segment[] blocks;
    /** Its place in the store's regions, and the high half of its blocks' ids. */
    final int index;
    /** Set under the store's lock by whoever gives the region back, holding every block: nothing claims in it after. */
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

    /**
     * Any thread: claims {@code n} consecutive empty blocks whole, each by CAS; returns the first's slot, or -1.
     * Blocks claimed before one that is no longer empty are freed again, and the search goes on past it.
     */
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

    /** The holder of blocks {@code first} to {@code first + n - 1}, claimed whole: frees them. */
    void unclaimBlocks(int first, int n) {
        for (int i = first; i < first + n; i++) {
            Segment block = blocks[i];
            block.unclaim(block.allFree);
        }
    }

    /** Purger: claims the blocks whole from the first, stopping at the first that is not empty; returns how many. */
    int claimAll() {
        int claimed = 0;
        while (claimed < slots && blocks[claimed].claimWhole()) {
            claimed++;
        }
        return claimed;
    }

    /** Racy. */
    boolean isEmpty() {
        for (Segment block : blocks) {
            if (!block.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /** How long ago the slice freed last was freed. Racy unless the caller holds every block. */
    long shortestWait(long now) {
        long shortest = Long.MAX_VALUE;
        for (Segment block : blocks) {
            shortest = Math.min(shortest, block.shortestWait(now));
        }
        return shortest;
    }

    /** Under the store's lock, holding every block: unmaps the mapping, or frees the one block. */
    void release() {
        released = true;
        buffer.release();
    }

    @Override
    public String toString() {
        return "Region[" + index + ", blocks: " + slots + (released ? ", released]" : "]");
    }
}
