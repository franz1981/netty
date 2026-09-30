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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The slot bitmap of a {@link Region}: lowest free slot first, a slot given back exactly once, and, under contention,
 * every slot owned by one thread at a time with takes and give-backs balancing out.
 */
final class RegionTest {
    /** The bitmap alone: no memory behind it. */
    private static Region region(int slots) {
        return new Region(null, slots);
    }

    @Test
    void takesTheLowestFreeSlot() {
        Region region = region(9);
        for (int slot = 0; slot < 9; slot++) {
            assertEquals(slot, region.takeSlot());
        }
        assertEquals(-1, region.takeSlot());
        region.giveBack(4);
        region.giveBack(2);
        assertEquals(2, region.freeSlotCount());
        assertEquals(2, region.takeSlot());
        assertEquals(4, region.takeSlot());
        assertThrows(IllegalStateException.class, () -> {
            region.giveBack(7);
            region.giveBack(7);
        }, "a slot is given back once");
    }

    @Test
    void sixtyFourSlotsFillTheWord() {
        Region region = region(Long.SIZE);
        assertEquals(-1L, region.free);
        for (int slot = 0; slot < Long.SIZE; slot++) {
            assertEquals(slot, region.takeSlot());
        }
        assertEquals(0, region.free);
        region.giveBack(63);
        assertEquals(Long.MIN_VALUE, region.free);
    }

    /**
     * Eight threads take and give back slots of one 9-slot region at random, holding a few at a time: a slot taken is
     * never owned by another thread until given back, and at the end every slot is free.
     */
    @Test
    void contendedSlotsAreOwnedOnce() throws Exception {
        final int slots = 9;
        final int threads = 8;
        final int rounds = 200_000;
        final Region region = region(slots);
        final AtomicIntegerArray owners = new AtomicIntegerArray(slots);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final long[] takes = new long[threads];
        final long[] givesBack = new long[threads];
        final CyclicBarrier start = new CyclicBarrier(threads);
        List<Thread> workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            final int id = t + 1;
            Thread worker = new Thread(() -> {
                try {
                    Random random = new Random(id);
                    int[] held = new int[2];
                    int count = 0;
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        if (count < held.length && random.nextBoolean()) {
                            int slot = region.takeSlot();
                            if (slot < 0) {
                                continue;
                            }
                            if (!owners.compareAndSet(slot, 0, id)) {
                                throw new AssertionError("slot " + slot + " taken by " + id + " is owned by "
                                        + owners.get(slot));
                            }
                            held[count++] = slot;
                            takes[id - 1]++;
                        } else if (count > 0) {
                            int k = random.nextInt(count);
                            int slot = held[k];
                            held[k] = held[--count];
                            if (!owners.compareAndSet(slot, id, 0)) {
                                throw new AssertionError("slot " + slot + " given back by " + id + " is owned by "
                                        + owners.get(slot));
                            }
                            region.giveBack(slot);
                            givesBack[id - 1]++;
                        }
                    }
                    while (count > 0) {
                        int slot = held[--count];
                        owners.set(slot, 0);
                        region.giveBack(slot);
                        givesBack[id - 1]++;
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertNull(failure.get());
        long taken = 0;
        long given = 0;
        for (int t = 0; t < threads; t++) {
            taken += takes[t];
            given += givesBack[t];
        }
        assertEquals(taken, given);
        assertEquals(region.allSlots, region.free);
    }
}
