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
import io.netty.incubator.allocator.AdaptivePoolingAllocator.Heap;
import io.netty.incubator.allocator.AdaptivePoolingAllocator.SizeClassMagazine;
import io.netty.incubator.allocator.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.incubator.allocator.PageStoreAllocators.Geometry;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

import static io.netty.incubator.allocator.AdaptivePoolingAllocator.THREAD_LOCAL_HEAPS;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.TestThreads.runOnThread;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The one interleaving no sequence of calls can place. A size-class chunk object outlives its span: given up, it waits
 * on its magazine's spare list and serves the magazine's next chunk on another span (an incarnation). A release from
 * another thread touches the chunk twice, the CAS that pushes its slot and then the note for the owner; between the
 * two the owner may give the chunk up and make it again. The test drives the two halves apart and observes what a user
 * would: the buffers of the new incarnation are disjoint and keep their bytes, the memory used does not change, and
 * the allocator closes. The reads of the allocator's internals only show that the variant was reached.
 */
public class ChunkReuseTest {
    private static final int SIZE = 4096;

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    /** The buffers of the scenario that are live, each with the pattern its bytes carry. */
    private static final class Live {
        private final AdaptivePoolingAllocator alloc;
        private final List<ByteBuf> buffers = new ArrayList<ByteBuf>();
        private final List<Long> seeds = new ArrayList<Long>();
        private long next = 1;

        /**
         * The buffers come from the heap allocator behind {@code alloc} itself, with no leak-aware wrapper: one of them
         * is never released, its slot is given back by the driver.
         */
        Live(AdaptiveByteBufAllocator alloc) {
            ByteBuf probe = alloc.heapBuffer(SIZE);
            this.alloc = chunk(probe).allocator;
            probe.release();
        }

        ByteBuf allocate() {
            ByteBuf buf = alloc.allocate(SIZE, SIZE);
            long seed = next++;
            BufferChecks.stamp(buf, seed);
            buffers.add(buf);
            seeds.add(seed);
            return buf;
        }

        void release(ByteBuf buf) {
            forget(buf);
            buf.release();
        }

        /** Buffers are equal when their readable bytes are: this is the identity. */
        private int indexOf(ByteBuf buf) {
            for (int i = 0; i < buffers.size(); i++) {
                if (buffers.get(i) == buf) {
                    return i;
                }
            }
            throw new AssertionError("not a live buffer");
        }

        /** Stops tracking {@code buf}, whose slot something else gives back, after its bytes were checked. */
        void forget(ByteBuf buf) {
            int i = indexOf(buf);
            BufferChecks.verify(buf, seeds.get(i));
            buffers.remove(i);
            seeds.remove(i);
        }

        void verifyAll() {
            BufferChecks.assertDisjoint(buffers);
            for (int i = 0; i < buffers.size(); i++) {
                BufferChecks.verify(buffers.get(i), seeds.get(i));
            }
        }

        void releaseAll() {
            for (ByteBuf buf : new ArrayList<ByteBuf>(buffers)) {
                release(buf);
            }
        }
    }

    /**
     * The remote release of the last buffer of incarnation C is cut in two: its CAS, then, after the owner gave C up
     * and the chunk went on as {@code variant} says, its note. The note must be harmless whatever the chunk is by then:
     * <ul>
     *   <li>{@code idle}: given up, never made again;</li>
     *   <li>{@code active}: made again as C', the magazine's chunk, a slot allocated and released on it;</li>
     *   <li>{@code refiled}: made again as C', run out and filed full.</li>
     * </ul>
     * {@code stripe}: the same on a stripe, whose owner is the lock holder.
     */
    @ParameterizedTest
    @CsvSource({"idle, false", "active, false", "refiled, false", "idle, true", "active, true", "refiled, true"})
    void aNoteLeftForAnEarlierIncarnationIsHarmless(String variant, boolean stripe) throws Throwable {
        assumeFalse(!stripe && !THREAD_LOCAL_HEAPS, "no thread-local heaps in this mode");
        final AdaptiveByteBufAllocator allocator = newAllocator(closer, ticker, Geometry.DEFAULTS, null);
        runOnThread(!stripe, () -> noteOfAnEarlierIncarnation(allocator, variant));
        allocator.close();
        assertEquals(0, allocator.usedHeapMemory(), "the close returns everything");
    }

    private static void noteOfAnEarlierIncarnation(AdaptiveByteBufAllocator allocator, String variant)
            throws Exception {
        Live live = new Live(allocator);
        // Full chunks above the retention floor, kept: a note that wrongly took an idle chunk for a reusable one
        // would give it up again.
        List<ByteBuf> padding = new ArrayList<ByteBuf>();
        padding.add(live.allocate());
        int slots = chunk(padding.get(0)).magazine.slots;
        while (padding.size() < (SizeClassMagazine.FLOOR + 1) * slots) {
            padding.add(live.allocate());
        }
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        held.add(live.allocate());
        final SizeClassedChunk chunk = chunk(held.get(0));
        final SizeClassMagazine magazine = chunk.magazine;
        assertEquals(SizeClassMagazine.FLOOR + 1, magazine.full.size, "the variant's chunk is above the floor");
        for (int i = 1; i < slots; i++) {
            held.add(live.allocate());
        }
        // The next allocation takes another chunk, D; C is filed with no free slot.
        ByteBuf firstOfD = live.allocate();
        assertNotSame(chunk, chunk(firstOfD));

        // All of C but one buffer comes back on the owner's side.
        ByteBuf last = held.remove(held.size() - 1);
        for (ByteBuf buf : held) {
            live.release(buf);
        }
        held.clear();
        // The other thread's release of the last one, first half: the CAS. Its buffer is never released again.
        live.forget(last);
        final int offset = last.arrayOffset();
        runOnThread(false, () -> chunk.pushRemoteFree(offset));

        // The owner gives C up: every slot is back.
        asOwner(magazine.heap, () -> magazine.returnFreeSpans(false));

        List<ByteBuf> ofNewIncarnation = new ArrayList<ByteBuf>();
        List<ByteBuf> ofD = new ArrayList<ByteBuf>();
        ofD.add(firstOfD);
        boolean filed = "refiled".equals(variant);
        if (!"idle".equals(variant)) {
            // Run D out: the next chunk is the same object, C'.
            for (int i = 1; i < slots; i++) {
                ofD.add(live.allocate());
            }
            ByteBuf buf = live.allocate();
            assertSame(chunk, chunk(buf), "the spare chunk object serves the next chunk");
            ofNewIncarnation.add(buf);
            live.release(live.allocate());
            if (filed) {
                while (ofNewIncarnation.size() < slots) {
                    ofNewIncarnation.add(live.allocate());
                }
                ByteBuf next = live.allocate();
                assertNotSame(chunk, chunk(next), "C' is full: the next buffer is of another chunk");
                ofD.add(next);
            }
        }
        live.verifyAll();
        long used = allocator.usedHeapMemory();

        // Second half: the note, on whatever the chunk is now; the owner drains it.
        runOnThread(false, () -> magazine.heap.notes.push(chunk));
        asOwner(magazine.heap, magazine.heap::applyNotes);

        assertEquals(used, allocator.usedHeapMemory(), "the note changed the memory used");
        live.verifyAll();
        if (!"idle".equals(variant) && !filed) {
            // The new incarnation's slots are all handed out, once each: until the next buffer is of another chunk.
            for (;;) {
                ByteBuf buf = live.allocate();
                ofNewIncarnation.add(buf);
                if (chunk(buf) != chunk) {
                    break;
                }
            }
            live.verifyAll();
        } else if ("idle".equals(variant)) {
            // The spare chunk serves the next chunk, on a span of its own.
            for (int i = 0; i < 2 * slots; i++) {
                ofNewIncarnation.add(live.allocate());
            }
            live.verifyAll();
        }
        live.releaseAll();
    }

    // --- helpers ---

    /** Runs {@code action} as {@code heap}'s owner: under its lock if it is a stripe's. */
    private static void asOwner(Heap heap, Runnable action) {
        StampedLock lock = heap.lock;
        long stamp = lock == null ? 0 : lock.writeLock();
        try {
            action.run();
        } finally {
            if (lock != null) {
                lock.unlockWrite(stamp);
            }
        }
    }

    /** The size-class chunk {@code buf} is a slot of, under a leak-aware wrapper or not. */
    private static SizeClassedChunk chunk(ByteBuf buf) {
        while (!(buf instanceof AdaptiveByteBuf)) {
            buf = buf.unwrap();
        }
        return (SizeClassedChunk) ((AdaptiveByteBuf) buf).chunk;
    }
}
