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
package io.netty.incubator.allocator;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What the integrity tests check of buffers through the {@link ByteBuf} API alone: every byte of a buffer is set to
 * a value of its own and read back, and the writable ranges of the live buffers do not overlap.
 */
final class BufferChecks {
    private BufferChecks() {
    }

    /** The value of byte {@code index} of the pattern of {@code seed}: a byte of a mix of both, 8 bytes per long. */
    private static long word(long seed, long wordIndex) {
        long x = seed * 0x9E3779B97F4A7C15L + wordIndex * 0xC2B2AE3D27D4EB4FL;
        x ^= x >>> 29;
        x *= 0xBF58476D1CE4E5B9L;
        return x ^ x >>> 32;
    }

    /** Bytes {@code from} to {@code to} of the pattern of {@code seed}, as {@link ByteBuf#setLong} lays them out. */
    private static byte[] pattern(long seed, int from, int to) {
        byte[] bytes = new byte[to - from];
        long w = 0;
        for (int j = from; j < to; j++) {
            if (j == from || (j & 7) == 0) {
                w = word(seed, j >>> 3);
            }
            bytes[j - from] = (byte) (w >>> (7 - (j & 7)) * 8);
        }
        return bytes;
    }

    /**
     * Sets bytes {@code from} to {@code to} of {@code buf} to the pattern of {@code seed}; the indices are kept. One
     * access per call, as {@link #verify} reads: with paranoid leak detection every access keeps a record with a
     * stack trace for as long as the buffer lives.
     */
    static void stamp(ByteBuf buf, long seed, int from, int to) {
        buf.setBytes(from, pattern(seed, from, to));
    }

    static void stamp(ByteBuf buf, long seed) {
        stamp(buf, seed, 0, buf.capacity());
    }

    /** Asserts that bytes 0 to {@code to} of {@code buf} are the pattern of {@code seed}, read in one access. */
    static void verify(ByteBuf buf, long seed, int to) {
        byte[] expected = pattern(seed, 0, to);
        byte[] actual = new byte[to];
        buf.getBytes(0, actual);
        for (int j = 0; j < to; j++) {
            if (expected[j] != actual[j]) {
                assertEquals(expected[j], actual[j], "byte " + j + " of a buffer of " + buf.capacity()
                        + " bytes with the pattern " + seed);
            }
        }
    }

    static void verify(ByteBuf buf, long seed) {
        verify(buf, seed, buf.capacity());
    }

    /** The bytes {@code buf} may be written to without moving: what it has, and what it can grow into in place. */
    static int slotLength(ByteBuf buf) {
        return buf.writerIndex() + buf.maxFastWritableBytes();
    }

    /**
     * Asserts that no two of {@code live} share a byte of the memory they may write: from their address or their
     * array offset for {@link #slotLength}, a buffer's capacity or the fast growth it has.
     */
    static void assertDisjoint(List<? extends ByteBuf> live) {
        Map<Object, List<long[]>> byMemory = new IdentityHashMap<Object, List<long[]>>();
        List<long[]> direct = new ArrayList<long[]>();
        for (ByteBuf buf : live) {
            if (buf.hasArray()) {
                byMemory.computeIfAbsent(buf.array(), k -> new ArrayList<long[]>())
                        .add(new long[] {buf.arrayOffset(), slotLength(buf)});
            } else {
                assumeTrue(buf.hasMemoryAddress(), "a direct buffer without an address cannot be checked for overlap");
                direct.add(new long[] {buf.memoryAddress(), slotLength(buf)});
            }
        }
        assertDisjointRanges(direct);
        for (List<long[]> ranges : byMemory.values()) {
            assertDisjointRanges(ranges);
        }
    }

    private static void assertDisjointRanges(List<long[]> ranges) {
        long[][] sorted = ranges.toArray(new long[0][]);
        Arrays.sort(sorted, Comparator.comparingLong(r -> r[0]));
        for (int i = 1; i < sorted.length; i++) {
            long previousEnd = sorted[i - 1][0] + sorted[i - 1][1];
            assertTrue(previousEnd <= sorted[i][0], "two buffers share memory: 0x" + Long.toHexString(sorted[i - 1][0])
                    + " + " + sorted[i - 1][1] + " reaches 0x" + Long.toHexString(sorted[i][0]));
        }
    }

    /** Releases every buffer of {@code buffers} and empties the list. */
    static void releaseAll(List<? extends ByteBuf> buffers) {
        for (ByteBuf buf : buffers) {
            buf.release();
        }
        buffers.clear();
    }

    /** Whether the range from {@code start} of {@code length} shares a byte with the one of the other. */
    static boolean overlaps(long start, long length, long otherStart, long otherLength) {
        return start < otherStart + otherLength && otherStart < start + length;
    }
}
