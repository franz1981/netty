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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * One piece of memory from a {@link RegionSource}, cut into {@link #slots} blocks at fixed offsets, all made with the
 * region. Each block's {@link Segment#free} is the free bitmap of its slices, claimed in runs and released by CAS from
 * any thread. A region lives as long as its {@link PageStore}, or until it goes back whole ({@link #released}).
 */
final class Region {
    private static final AtomicIntegerFieldUpdater<Region> MAX_ACCESSED =
            AtomicIntegerFieldUpdater.newUpdater(Region.class, "maxAccessed");

    /** No memory behind the slice: never claimed since the region was mapped, or purged since. */
    static final long UNCOMMITTED = Long.MIN_VALUE;

    final AbstractByteBuf buffer;
    /** Where {@link #buffer} comes from, and goes back to. */
    final RegionSource source;
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
    /** The highest block a run was claimed in, -1 before the first. */
    private volatile int maxAccessed = -1;

    /** Every block made now, all slices free; uncommitted unless {@code committedAt}, their release time, is set. */
    Region(AbstractByteBuf buffer, RegionSource source, int slots, SegmentSource views, PageStoreConfig config,
           boolean committed, long committedAt) {
        assert slots > 0 && slots <= Long.SIZE;
        this.buffer = buffer;
        this.source = source;
        purgesSlices = source.canPurgeSlices();
        this.slots = slots;
        int size = config.segmentSize;
        length = slots * size;
        blocks = new Segment[slots];
        for (int slot = 0; slot < slots; slot++) {
            Segment block = new Segment(views.span(buffer, slot * size, size), config.sliceSize, this, slot);
            Arrays.fill(block.freedAt, committed ? committedAt : UNCOMMITTED);
            blocks[slot] = block;
        }
        sliceEverCommitted = new boolean[slots * config.slicesPerSegment()];
        if (committed) {
            Arrays.fill(sliceEverCommitted, true);
        }
    }

    /**
     * Any thread: claims the lowest run of {@code n} free slices of one block, at most a block, and
     * returns its first slice in the region, or -1 when no block has such a run. Lock-free: one CAS on the block's
     * bitmap, again only if another thread changed it meanwhile.
     * <p>
     * As mimalloc v3's {@code mi_bbitmap_try_find_and_clear_generic}
     * (https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L1801-L1884): the blocks
     * claimed in so far are visited from {@code seq} modulo their count, wrapping around, so that heaps with different
     * sequences start in different blocks, then the blocks never claimed in, in order; within a block, the lowest
     * fitting run (first fit, as {@code mi_bchunk_try_find_and_clearNX},
     * https://github.com/microsoft/mimalloc/blob/31d034d/src/bitmap.c#L793-L849). A run never
     * crosses a block.
     */
    int claimSlices(int n, int seq) {
        Segment[] blocks = this.blocks;
        int count = blocks.length;
        int cycle = maxAccessed + 1;
        int start = cycle == 0 ? 0 : seq % cycle;
        for (int k = 0; k < count; k++) {
            int slot = k >= cycle ? k : start + k < cycle ? start + k : start + k - cycle;
            Segment block = blocks[slot];
            if (Long.bitCount(block.free) >= n) {
                int first = block.claimRun(n);
                if (first >= 0) {
                    accessed(slot);
                    return slot * block.slices + first;
                }
            }
        }
        return -1;
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
                accessed(first + n - 1);
                return first;
            }
            for (int i = 0; i < claimed; i++) {
                Segment block = blocks[first + i];
                block.giveBack(block.allFree);
            }
            first += claimed + 1;
        }
        return -1;
    }

    private void accessed(int slot) {
        for (;;) {
            int max = maxAccessed;
            if (slot <= max || MAX_ACCESSED.compareAndSet(this, max, slot)) {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "Region[" + index + ", blocks: " + slots + (released ? ", released]" : "]");
    }
}
