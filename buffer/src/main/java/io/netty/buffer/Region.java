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
 * One piece of memory, cut into {@link #slots} blocks at fixed offsets, all made with the region: {@code mmap}d by
 * {@link #source}, or, where it is {@code null}, one plain block from the store's {@link MemorySource}. Each block's
 * {@link Segment#free} is the free bitmap of its slices, claimed in runs and released by CAS from any thread. A
 * region lives as long as its {@link PageStore}, or until it goes back whole ({@link #released}).
 */
final class Region {
    final PageStore store;
    final AbstractByteBuf buffer;
    /**
     * Where {@link #buffer} comes from, and goes back to; {@code null}: one plain block, goes back whole. Whether it
     * purges idle free slices in place is {@code source != null}; else the region is charged and counted whole, and
     * goes back whole once idle.
     */
    final MmapRegionSource source;
    final int slots;
    /** {@link #slots} blocks, in bytes. */
    final int length;
    final Segment[] blocks;
    /** Its index in {@link PageStore#regions}, published with the region. */
    final int index;
    /** Given back to its source whole: every block stays claimed. Set by the purger under the store's monitor. */
    volatile boolean released;

    /** Every block made now, all slices free; committed and freed at {@code committedAt} if {@code committed}. */
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
     * Any thread: claims {@code n} contiguous empty blocks and returns the first, or -1: from the start, whole
     * blocks only, one CAS per block, and the blocks claimed so far go back when one is taken meanwhile.
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

    /** Gives {@code n} blocks from {@code first}, claimed whole and never committed, back unclaimed. */
    void unclaimBlocks(int first, int n) {
        for (int i = first; i < first + n; i++) {
            Segment block = blocks[i];
            block.unclaim(block.allFree);
        }
    }

    /** Claims every block whole, one CAS at a time; returns how many, stopping at the first already claimed. */
    int claimAll() {
        int claimed = 0;
        while (claimed < slots && blocks[claimed].claimWhole()) {
            claimed++;
        }
        return claimed;
    }

    /** Racy: whether every slice is free. */
    boolean isEmpty() {
        for (Segment block : blocks) {
            if (!block.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * How long, at {@code now}, the slice freed last has been free: the region is idle once this reaches the purge
     * delay. Racy unless the caller claimed every block.
     */
    long shortestWait(long now) {
        long shortest = Long.MAX_VALUE;
        for (Segment block : blocks) {
            shortest = Math.min(shortest, block.shortestWait(now));
        }
        return shortest;
    }

    /** Marks the region released and gives its buffer back. */
    void release() {
        released = true;
        buffer.release();
    }

    @Override
    public String toString() {
        return "Region[" + index + ", blocks: " + slots + (released ? ", released]" : "]");
    }
}
