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

import io.netty.buffer.AdaptivePoolingAllocator.SegmentBitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveSegmentBitSetTest {

    /**
     * A new set has every segment free, hands each one out exactly once, and then has none. The counts are the
     * segments per chunk of the size classes, plus the word boundaries.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16, 32, 56, 63, 64, 65, 113, 128, 204, 256, 512, 1024, 2048, 4096})
    void handsOutEverySegmentOnce(int segments) {
        SegmentBitSet set = new SegmentBitSet(segments);
        assertEquals(segments, set.freeCount());
        assertTrue(set.isWhollyFree());
        BitSet seen = new BitSet(segments);
        for (int i = 0; i < segments; i++) {
            int index = set.acquire();
            assertTrue(index >= 0 && index < segments, "index " + index);
            assertFalse(seen.get(index), "segment " + index + " handed out twice");
            seen.set(index);
        }
        assertEquals(0, set.freeCount());
        assertFalse(set.hasFree());
        assertEquals(-1, set.acquire());
    }

    /**
     * The lowest free segment is handed out first, so which segment a chunk gives next depends only on which are
     * free, not on the order they were released in.
     */
    @Test
    void acquiresTheLowestFreeSegment() {
        SegmentBitSet set = new SegmentBitSet(4096);
        for (int i = 0; i < 4096; i++) {
            assertEquals(i, set.acquire());
        }
        set.release(3000);
        set.release(10);
        set.release(64);
        assertEquals(10, set.acquire());
        assertEquals(64, set.acquire());
        assertEquals(3000, set.acquire());
        assertEquals(-1, set.acquire());
    }

    /**
     * Seeded random acquires and releases, by the owner and as another thread would, against a plain model: a
     * segment in use is never handed out, and none is lost.
     */
    @ParameterizedTest
    @ValueSource(ints = {8, 64, 113, 4096})
    void neverHandsOutASegmentInUse(int segments) {
        SplittableRandom rng = new SplittableRandom(segments);
        SegmentBitSet set = new SegmentBitSet(segments);
        BitSet inUse = new BitSet(segments);
        List<Integer> held = new ArrayList<Integer>();
        for (int step = 0; step < 200000; step++) {
            if (held.isEmpty() || rng.nextInt(100) < 55) {
                int index = set.acquire();
                if (index == -1) {
                    assertEquals(segments, held.size(), "no segment although some are free");
                } else {
                    assertFalse(inUse.get(index), "segment " + index + " handed out twice");
                    inUse.set(index);
                    held.add(index);
                }
            } else {
                int index = held.remove(rng.nextInt(held.size()));
                inUse.clear(index);
                if (rng.nextInt(4) == 0) {
                    set.releaseExternal(index);
                } else {
                    set.release(index);
                }
            }
            assertEquals(segments - held.size(), set.freeCount());
            assertEquals(held.isEmpty(), set.isWhollyFree());
        }
    }

    /**
     * Segments released by other threads count as free at once, and the owner takes them when it runs dry.
     */
    @Test
    void takesExternalReleasesWhenItRunsDry() {
        SegmentBitSet set = new SegmentBitSet(204);
        for (int i = 0; i < 204; i++) {
            set.acquire();
        }
        set.releaseExternal(5);
        set.releaseExternal(70);
        set.releaseExternal(203);
        assertEquals(3, set.freeCount());
        assertTrue(set.hasFree());
        assertFalse(set.isWhollyFree());
        assertEquals(5, set.acquire());
        assertEquals(70, set.acquire());
        assertEquals(203, set.acquire());
        assertEquals(-1, set.acquire());
    }

    @Test
    void isWhollyFreeCountsBothSides() {
        SegmentBitSet set = new SegmentBitSet(32);
        assertTrue(set.isWhollyFree());
        int a = set.acquire();
        int b = set.acquire();
        assertFalse(set.isWhollyFree());
        set.release(a);
        assertFalse(set.isWhollyFree());
        set.releaseExternal(b);
        assertTrue(set.isWhollyFree());
    }

    /**
     * Other threads release every segment while the owner keeps acquiring: each segment reaches the owner exactly
     * once, whatever the interleaving.
     */
    @Test
    void concurrentReleasesReachTheOwnerExactlyOnce() throws Exception {
        final int segments = 4096;
        final int releasers = 4;
        final SegmentBitSet set = new SegmentBitSet(segments);
        for (int i = 0; i < segments; i++) {
            set.acquire();
        }
        final CountDownLatch start = new CountDownLatch(1);
        Thread[] threads = new Thread[releasers];
        for (int t = 0; t < releasers; t++) {
            final int id = t;
            threads[t] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    // Interleaved indices, so that the threads hit the same words.
                    for (int i = id; i < segments; i += releasers) {
                        set.releaseExternal(i);
                    }
                }
            });
            threads[t].start();
        }
        start.countDown();
        BitSet seen = new BitSet(segments);
        int count = 0;
        while (count < segments) {
            int index = set.acquire();
            if (index == -1) {
                Thread.yield();
                continue;
            }
            assertFalse(seen.get(index), "segment " + index + " reached the owner twice");
            seen.set(index);
            count++;
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(-1, set.acquire());
        assertEquals(0, set.freeCount());
    }
}
