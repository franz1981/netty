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

import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassMagazine;
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.AdaptivePoolingAllocator.IS_LOW_MEM;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.asOwner;
import static io.netty.buffer.PageStoreTestSupport.chunk;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * A size-class chunk object outlives its span: given up, it waits on its magazine's spare list and serves the
 * magazine's next chunk on another span (an incarnation). A release from another thread touches the chunk twice, the
 * CAS that pushes its slot and then the note for the owner; between the two the owner may give the chunk up and make
 * it again.
 */
public class ChunkReuseTest {
    private static final int SIZE = 4096;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    /**
     * The remote release of the last buffer of incarnation C is cut in two: its CAS, then, after the owner gave C up
     * and the chunk went on as {@code variant} says, its note. The note must be harmless whatever the chunk is by then:
     * <ul>
     *   <li>{@code idle}: given up, never made again;</li>
     *   <li>{@code active}: made again as C', the magazine's chunk, a slot allocated and released on it;</li>
     *   <li>{@code refiled}: made again as C', run out and filed full.</li>
     * </ul>
     * Every slot of C' is handed out once, its span is claimed while it lives and free once it is given up again.
     * {@code stripe}: the same on a stripe, whose owner is the lock holder.
     */
    @ParameterizedTest
    @CsvSource({"idle, false", "active, false", "refiled, false", "idle, true", "active, true", "refiled, true"})
    void aNoteLeftForAnEarlierIncarnationIsHarmless(String variant, boolean stripe) throws Throwable {
        assumeFalse(!stripe && IS_LOW_MEM, "low-memory mode has no thread-local heaps");
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(new CountingMemorySource(true),
                SEGMENT_SIZE));
        onOwner(stripe, () -> noteOfAnEarlierIncarnation(allocator, variant));
    }

    private static void noteOfAnEarlierIncarnation(AdaptivePoolingAllocator allocator, String variant)
            throws Exception {
        // Full chunks above the retention floor, kept: a note that wrongly took an idle chunk for a reusable one
        // would give it up again.
        List<ByteBuf> padding = new ArrayList<ByteBuf>();
        padding.add(allocator.allocate(SIZE, SIZE));
        int slots = chunk(padding.get(0)).magazine.slots;
        while (padding.size() < (SizeClassMagazine.FLOOR + 1) * slots) {
            padding.add(allocator.allocate(SIZE, SIZE));
        }
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        held.add(allocator.allocate(SIZE, SIZE));
        final SizeClassedChunk chunk = chunk(held.get(0));
        final SizeClassMagazine magazine = chunk.magazine;
        assertEquals(SizeClassMagazine.FLOOR + 1, magazine.full.size);
        for (int i = 1; i < slots; i++) {
            held.add(allocator.allocate(SIZE, SIZE));
        }
        // The next allocation takes another chunk, D; C is filed with no free slot.
        ByteBuf firstOfD = allocator.allocate(SIZE, SIZE);
        assertNotSame(chunk, chunk(firstOfD));
        Segment block = chunk.segment;
        int start = chunk.spanStart;
        int slices = magazine.slices;

        // All of C but one buffer comes back on the owner's side.
        ByteBuf last = held.remove(held.size() - 1);
        for (ByteBuf buf : held) {
            buf.release();
        }
        held.clear();
        // The other thread's release of the last one, first half: the CAS. Its buffer is never released again.
        final int offset = slotOffset(last);
        runOnAnotherThread(() -> chunk.pushRemoteFree(offset));

        // The owner gives C up: every slot is back.
        asOwner(magazine.heap, () -> magazine.returnFreeSpans(false));
        assertTrue(spanFree(block, start, slices), "given up: its span is free");

        List<ByteBuf> ofNewIncarnation = new ArrayList<ByteBuf>();
        List<ByteBuf> ofD = new ArrayList<ByteBuf>();
        ofD.add(firstOfD);
        boolean filed = "refiled".equals(variant);
        if (!"idle".equals(variant)) {
            // Run D out: the next chunk is the same object, C'.
            for (int i = 1; i < slots; i++) {
                ofD.add(allocator.allocate(SIZE, SIZE));
            }
            ByteBuf buf = allocator.allocate(SIZE, SIZE);
            assertSame(chunk, chunk(buf), "the spare chunk object serves the next chunk");
            ofNewIncarnation.add(buf);
            allocator.allocate(SIZE, SIZE).release();
            if (filed) {
                while (ofNewIncarnation.size() < slots) {
                    ofNewIncarnation.add(allocator.allocate(SIZE, SIZE));
                }
                ByteBuf next = allocator.allocate(SIZE, SIZE);
                assertNotSame(chunk, chunk(next));
                ofD.add(next);
            }
        }

        // Second half: the note, on whatever the chunk is now; the owner drains it.
        runOnAnotherThread(() -> magazine.heap.notes.push(chunk));
        asOwner(magazine.heap, magazine.heap::applyNotes);

        if ("idle".equals(variant)) {
            assertTrue(spanFree(block, start, slices), "a spare chunk stays spare: its span stays free");
        } else {
            Segment newBlock = chunk.segment;
            assertFalse(spanFree(newBlock, chunk.spanStart, slices), "C' holds its span");
            assertEachSlotHandedOutOnce(allocator, chunk, ofNewIncarnation, slots, filed);
            int newStart = chunk.spanStart;
            for (ByteBuf buf : ofNewIncarnation) {
                buf.release();
            }
            assertTrue(chunk.allFree(), "every slot of C' is back, and counted once");
            assertTrue(spanFree(newBlock, newStart, slices), "above the floor: its span given back, once");
        }
        for (ByteBuf buf : ofD) {
            buf.release();
        }
        for (ByteBuf buf : padding) {
            buf.release();
        }
    }

    /**
     * The slots of {@code chunk}: those of {@code live} and, unless it is filed full, those the owner hands out
     * until the next buffer comes from another chunk; together every slot, each once. The buffers handed out are
     * added to {@code live}.
     */
    private static void assertEachSlotHandedOutOnce(AdaptivePoolingAllocator allocator, SizeClassedChunk chunk,
                                                    List<ByteBuf> live, int slots, boolean filed) {
        Set<Integer> offsets = new HashSet<Integer>();
        for (ByteBuf buf : live) {
            assertTrue(offsets.add(slotOffset(buf)));
        }
        if (filed) {
            // Filed full: nothing allocates from it.
            assertEquals(slots, live.size(), "every slot of C' is out");
            assertFalse(chunk.hasFreeSlot());
            return;
        }
        for (;;) {
            ByteBuf buf = allocator.allocate(SIZE, SIZE);
            live.add(buf);
            if (chunk(buf) != chunk) {
                break;
            }
            assertTrue(offsets.add(slotOffset(buf)), "slot handed out twice");
        }
        assertEquals(slots, offsets.size(), "every slot of C' handed out");
    }

    /**
     * Steady churn makes no chunk object once the magazine has had its peak: a burst of several chunks, all
     * released, so that the chunks above the retention floor are given up, again and again, and every round's
     * chunks are the objects of the first round.
     */
    @ParameterizedTest
    @CsvSource({"false", "true"})
    void churnMakesNoMoreChunkObjectsThanItsPeak(boolean stripe) throws Throwable {
        assumeFalse(!stripe && IS_LOW_MEM, "low-memory mode has no thread-local heaps");
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(new CountingMemorySource(true),
                SEGMENT_SIZE));
        onOwner(stripe, () -> {
            final int chunks = SizeClassMagazine.FLOOR + 4;
            Set<SizeClassedChunk> peak = new HashSet<SizeClassedChunk>();
            for (int round = 0; round < 50; round++) {
                List<ByteBuf> burst = new ArrayList<ByteBuf>();
                Set<SizeClassedChunk> seen = new HashSet<SizeClassedChunk>();
                for (int i = 0; i < chunks * 31; i++) {
                    ByteBuf buf = allocator.allocate(SIZE, SIZE);
                    burst.add(buf);
                    seen.add(chunk(buf));
                }
                if (round == 0) {
                    peak.addAll(seen);
                    assertTrue(peak.size() >= chunks, "the burst must take several chunks: " + peak.size());
                } else {
                    assertTrue(peak.containsAll(seen), "round " + round + " made a chunk object");
                }
                for (ByteBuf buf : burst) {
                    buf.release();
                }
            }
        });
    }

    // --- helpers ---

    private interface Body {
        void run() throws Exception;
    }

    /** Runs {@code body} on a thread with its own heap, or on a plain thread that allocates from a stripe. */
    private static void onOwner(boolean stripe, final Body body) throws Throwable {
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Runnable task = () -> {
            try {
                body.run();
            } catch (Throwable t) {
                failure.set(t);
            }
        };
        Thread thread = stripe ? new Thread(task) : new FastThreadLocalThread(task);
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    private static void runOnAnotherThread(Runnable action) throws InterruptedException {
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread thread = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }

    private static boolean spanFree(Segment block, int start, int slices) {
        long bits = block.bits(start, slices);
        return (block.free & bits) == bits;
    }

    /** Where {@code buf}'s slot starts in its block's {@code byte[]}: the offset the chunk knows the slot by. */
    private static int slotOffset(ByteBuf buf) {
        return buf.arrayOffset();
    }
}
