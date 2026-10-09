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
 * The size classes every {@link AdaptivePoolingAllocator} serves, the same for every allocator, and the chunk
 * geometry they take under one allocator's {@link PageStoreConfig}, built once from it: the page kinds (the span
 * lengths, in slices, a chunk may have) and, per size class, its chunk's slices, slots and leftover room. Immutable,
 * read by every heap and by the store's bins ({@link PageStore#binByLength}).
 */
final class SizeClassTable {
    /** A size class's page holds at least this many slots, unless only the largest page kind fits it. */
    private static final int MIN_PAGE_SLOTS = 4;
    /** At most this fraction of a page, as a right shift, stays unused at its end: an eighth. */
    private static final int MAX_PAGE_WASTE_SHIFT = 3;
    /** An exact fit with fewer slots takes the next page kind, for room to colour, unless that is the largest. */
    private static final int MIN_EXACT_FIT_SLOTS = 16;
    /** Colours are this many bytes apart, as a shift: one cache line. */
    private static final int COLOUR_SHIFT = 6;
    static final int MAX_CHUNK_COLOURS = 16;

    /**
     * Steps are a quarter of a doubling apart up to 16 KiB (256, 320, 384, 448, ...): a buffer leaves at most a
     * fifth of its class unused, where power-of-2 steps alone leave up to half, and most classes take one-slice
     * chunks. Above 16896 the steps are half a doubling apart: those classes take 8 slices or more per chunk, so a
     * finer step there would cost more in chunks than it saves in rounding.
     */
    static final int[] SIZES = {
            32,
            64,
            128,
            256,
            320,
            384,
            448,
            512,
            640, // 512 + 128
            768,
            896,
            1024,
            1152, // 1024 + 128
            1280,
            1536,
            1792,
            2048,
            2304, // 2048 + 256
            2560,
            3072,
            3584,
            4096,
            4352, // 4096 + 256
            5120,
            6144,
            7168,
            8192,
            8704, // 8192 + 512
            10240,
            12288,
            14336,
            16384,
            16896, // 16384 + 512
            24576,
            32768,
            33792, // 32768 + 1024
            49152,
            65536,
            67584, // 65536 + 2048
            98304,
            131072,
            135168, // 131072 + 4096
    };

    static final int SIZES_COUNT = SIZES.length;
    private static final byte[] SIZE_INDEXES = new byte[SIZES[SIZES_COUNT - 1] / 32 + 1];

    static {
        int lastStep = 0;
        for (int i = 0; i < SIZES_COUNT; i++) {
            int sizeClass = SIZES[i];
            //noinspection ConstantValue
            assert (sizeClass & 31) == 0 : "Size class must be a multiple of 32";
            int step = sizeStep(sizeClass);
            Arrays.fill(SIZE_INDEXES, lastStep + 1, step + 1, (byte) i);
            lastStep = step;
        }
    }

    /** {@code size} in steps of 32 bytes, rounded up: its index in {@link #SIZE_INDEXES}. */
    private static int sizeStep(final int size) {
        return size + 31 >> 5;
    }

    /** The size class of {@code size}, or {@link #SIZES_COUNT} above the largest size class. */
    static int sizeClassIndex(int size) {
        int step = sizeStep(size);
        if (step < SIZE_INDEXES.length) {
            return SIZE_INDEXES[step];
        }
        return SIZES_COUNT;
    }

    /**
     * The page kinds of a block of {@code blockSlices} slices of {@code sliceSize}, smallest first: the span lengths,
     * in slices, a size class's chunk may have. One slice; the largest divisor of the block up to an eighth of it;
     * the largest up to half of it; and the block itself if none of these holds the largest size class. Each divides
     * the block, so chunks of one kind tile it with no tail, and each kind is a bin of the store's blocks
     * ({@link PageStore#binByLength}). 64 slices: 1, 8, 32; 63: 1, 7, 21; 32: 1, 4, 16; 7: 1, 7.
     */
    static int[] pageKinds(int blockSlices, int sliceSize) {
        int[] kinds = new int[4];
        int count = 0;
        kinds[count++] = 1;
        count = addKind(kinds, count, largestDivisorUpTo(blockSlices, blockSlices / 8));
        count = addKind(kinds, count, largestDivisorUpTo(blockSlices, blockSlices / 2));
        if ((long) kinds[count - 1] * sliceSize < SIZES[SIZES_COUNT - 1]) {
            count = addKind(kinds, count, blockSlices);
        }
        return Arrays.copyOf(kinds, count);
    }

    private static int largestDivisorUpTo(int n, int max) {
        for (int d = max; d > 1; d--) {
            if (n % d == 0) {
                return d;
            }
        }
        return 1;
    }

    private static int addKind(int[] kinds, int count, int kind) {
        if (kind > kinds[count - 1]) {
            kinds[count++] = kind;
        }
        return count;
    }

    /**
     * The page kind, in slices, of a size class: the smallest of {@code kinds} with {@link #MIN_PAGE_SLOTS} slots or
     * more that leaves at most an eighth of the page unused at its end, else the largest.
     */
    private static int pageSlices(int sizeClass, int[] kinds, int sliceSize) {
        for (int kind : kinds) {
            int page = kind * sliceSize;
            int slots = page / sizeClass;
            if (slots >= MIN_PAGE_SLOTS && page - slots * sizeClass <= page >>> MAX_PAGE_WASTE_SHIFT) {
                return kind;
            }
        }
        return kinds[kinds.length - 1];
    }

    /**
     * The slices of a size class's chunks: {@link #pageSlices}, or for an exact fit of fewer than
     * {@link #MIN_EXACT_FIT_SLOTS} slots the next kind, unless that is the largest, so that giving up one slot for the
     * colours costs little (see {@link #colourOffset}).
     */
    static int slicesPerChunk(int sizeClass, int[] kinds, int sliceSize) {
        int kind = pageSlices(sizeClass, kinds, sliceSize);
        int k = Arrays.binarySearch(kinds, kind);
        for (;;) {
            int page = kinds[k] * sliceSize;
            int slots = page / sizeClass;
            if (page - slots * sizeClass >= 1 << COLOUR_SHIFT || slots <= 1 || slots >= MIN_EXACT_FIT_SLOTS
                    || k + 2 >= kinds.length) {
                return kinds[k];
            }
            k++;
        }
    }

    /**
     * The buffers a chunk of {@code chunkSize} bytes hands out: those that fit, less one an exact fit gives up for a
     * second colour when that buffer makes room for it (not the 32-byte class).
     */
    static int slotsPerChunk(int sizeClass, int chunkSize) {
        int slots = chunkSize / sizeClass;
        int room = chunkSize - slots * sizeClass;
        return room < 1 << COLOUR_SHIFT && slots > 1 && room + sizeClass >= 1 << COLOUR_SHIFT ? slots - 1 : slots;
    }

    /**
     * Round robin over the colours {@code room} bytes leave, {@code maxColours} at most, 64 bytes apart: the start
     * offset of the {@code sequence}'th chunk's or span's buffers, past the first one. Slab colouring, for the size
     * classes' chunks and for the spans of large buffers, as mimalloc's large allocations
     * (https://github.com/microsoft/mimalloc/pull/1339).
     */
    static int colourOffset(int sequence, int room, int maxColours) {
        int colours = Math.min(maxColours, (room >>> COLOUR_SHIFT) + 1);
        return colours == 1 ? 0 : (sequence & Integer.MAX_VALUE) % colours << COLOUR_SHIFT;
    }

    /** The page kinds of {@code config}'s blocks: see {@link #pageKinds}. */
    final int[] pageKinds;
    /** Per size class, its chunk's slices: {@link #slicesPerChunk} under {@link #pageKinds}. */
    final int[] chunkSlices;
    /** Per size class, the buffers its chunk hands out: {@link #slotsPerChunk}. */
    final int[] slots;
    /** Per size class, its chunk size's tail its slots leave unused: the room {@link #colourOffset} rotates in. */
    final int[] room;

    SizeClassTable(PageStoreConfig config) {
        pageKinds = pageKinds(config.slicesPerSegment(), config.sliceSize);
        chunkSlices = new int[SIZES_COUNT];
        slots = new int[SIZES_COUNT];
        room = new int[SIZES_COUNT];
        for (int i = 0; i < SIZES_COUNT; i++) {
            int sizeClass = SIZES[i];
            int slices = slicesPerChunk(sizeClass, pageKinds, config.sliceSize);
            int chunkSize = slices * config.sliceSize;
            int classSlots = slotsPerChunk(sizeClass, chunkSize);
            chunkSlices[i] = slices;
            slots[i] = classSlots;
            room[i] = chunkSize - classSlots * sizeClass;
        }
    }
}
