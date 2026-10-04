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

import java.util.Arrays;

/**
 * One piece of memory, cut into {@link #slots} blocks at fixed offsets, all made with the region: {@code mmap}d by
 * {@link #source}, or, where it is {@code null}, one plain block from the store's {@link MemorySource}. Each block's
 * {@link Segment#free} is the free bitmap of its slices, claimed in runs and released by CAS from any thread. A
 * region lives as long as its {@link PageStore}, or until it goes back whole ({@link #released}).
 */
final class Region {
    final PageStore store;
    final AbstractByteBuf buffer;
    /** Where {@link #buffer} comes from, and goes back to; {@code null}: one plain block, goes back whole. */
    final MmapRegionSource source;
    /**
     * Whether {@link #source} purges idle free slices in place; else the region is charged and counted whole, and
     * goes back whole once wholly idle.
     */
    final boolean purgesSlices;
    final int slots;
    /** {@link #slots} blocks, in bytes. */
    final int length;
    final Segment[] blocks;
    /**
     * Per slice of the region, slice owner only: whether memory was ever behind it, so that a slice with none now was
     * purged.
     */
    final boolean[] sliceEverCommitted;
    /** Its index in {@link PageStore#regions}, set before it is published there. */
    int index = -1;
    /** Given back to its source whole: every block stays claimed. Set by the purger under the store's monitor. */
    volatile boolean released;

    /** Every block made now, all slices free; committed and freed at {@code committedAt} if {@code committed}. */
    Region(PageStore store, AbstractByteBuf buffer, MmapRegionSource source, int slots,
           PageStoreConfig config, boolean committed, long committedAt) {
        assert slots > 0 && slots <= Long.SIZE;
        this.store = store;
        this.buffer = buffer;
        this.source = source;
        purgesSlices = source != null;
        this.slots = slots;
        int size = config.segmentSize;
        length = slots * size;
        blocks = new Segment[slots];
        for (int slot = 0; slot < slots; slot++) {
            Segment block = new Segment(buffer, slot * size, size, config.sliceSize, this, slot);
            if (committed) {
                block.committed = block.allFree;
                Arrays.fill(block.freedAt, committedAt);
            }
            blocks[slot] = block;
        }
        sliceEverCommitted = new boolean[slots * config.slicesPerSegment()];
        if (committed) {
            Arrays.fill(sliceEverCommitted, true);
        }
    }

    /**
     * Any thread: claims {@code n} contiguous wholly free blocks and returns the first, or -1. As
     * mimalloc v3's {@code mi_bbitmap_try_find_and_clearN_} for objects above a chunk
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L1950-L1997): from the
     * start, whole blocks only, one CAS per block, and the blocks claimed so far go back when one is taken meanwhile.
     */
    int claimBlocks(int n) {
        Segment[] blocks = this.blocks;
        int first = 0;
        while (first + n <= blocks.length) {
            int end = first;
            while (end < first + n && blocks[end].isWhollyFree()) {
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
            for (int i = 0; i < claimed; i++) {
                Segment block = blocks[first + i];
                block.giveBack(block.allFree);
            }
            store.armPurge(System.nanoTime());
            first += claimed + 1;
        }
        return -1;
    }

    @Override
    public String toString() {
        return "Region[" + index + ", blocks: " + slots + (released ? ", released]" : "]");
    }
}
