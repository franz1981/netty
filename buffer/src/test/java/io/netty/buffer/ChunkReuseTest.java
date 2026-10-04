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

import io.netty.buffer.AdaptivePoolingAllocator.AdaptiveByteBuf;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunkCache;
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * A size-class chunk object outlives its span: given up, it waits on its cache's idle list and serves the cache's next
 * chunk on another span (an incarnation). A release from another thread touches the chunk twice, the CAS that pushes
 * its segment and then the note for the owner; between the two the owner may give the chunk up and make it again.
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
     *   <li>{@code active}: made again as C', the magazine's chunk, a segment allocated and released on it;</li>
     *   <li>{@code refiled}: made again as C', run out and filed exhausted.</li>
     * </ul>
     * Every segment of C' is handed out once, its span is claimed while it lives and free once it is given up again,
     * and the drain files it by its own capacity. {@code stripe}: the same on a stripe, whose owner is the lock holder.
     */
    @ParameterizedTest
    @CsvSource({"idle, false", "active, false", "refiled, false", "idle, true", "active, true", "refiled, true"})
    void aNoteLeftForAnEarlierIncarnationIsHarmless(String variant, boolean stripe) throws Throwable {
        assumeFalse(!stripe && isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(new CountingMemorySource(true),
                SEGMENT_SIZE));
        onOwner(stripe, () -> noteOfAnEarlierIncarnation(allocator, variant, stripe));
    }

    private static void noteOfAnEarlierIncarnation(AdaptivePoolingAllocator allocator, String variant,
                                                   boolean stripe) throws Exception {
        // Full chunks above the retention floor, kept: a note that wrongly took an idle chunk for a reusable one
        // would give it up again.
        List<ByteBuf> padding = new ArrayList<ByteBuf>();
        padding.add(allocator.allocate(SIZE, SIZE));
        int segments = segmentsOf(chunkOf(padding.get(0)));
        while (padding.size() < (SizeClassedChunkCache.FLOOR + 1) * segments) {
            padding.add(allocator.allocate(SIZE, SIZE));
        }
        List<ByteBuf> held = new ArrayList<ByteBuf>();
        held.add(allocator.allocate(SIZE, SIZE));
        final SizeClassedChunk chunk = chunkOf(held.get(0));
        final SizeClassedChunkCache cache = chunk.owningCache;
        assertEquals(SizeClassedChunkCache.FLOOR + 1, cache.exhausted.size());
        for (int i = 1; i < segments; i++) {
            held.add(allocator.allocate(SIZE, SIZE));
        }
        // The next allocation takes another chunk, D; C is filed with no free segment.
        ByteBuf firstOfD = allocator.allocate(SIZE, SIZE);
        assertNotSame(chunk, chunkOf(firstOfD));
        assertSame(cache.exhausted, chunk.queue);
        Segment block = chunk.segment;
        int start = chunk.spanStart;
        int slices = chunk.spanSlices();

        // All of C but one buffer comes back on the owner's side.
        ByteBuf last = held.remove(held.size() - 1);
        for (ByteBuf buf : held) {
            buf.release();
        }
        held.clear();
        // The other thread's release of the last one, first half: the CAS. Its buffer is never released again.
        final int offset = startIndexOf(last);
        runOnAnotherThread(() -> chunk.pushExternalFree(offset));

        // The owner gives C up: every segment is back.
        asOwner(allocator, stripe, cache::evictWhollyFree);
        assertSame(cache.idle, chunk.queue, "given up to the cache's idle chunks");
        assertTrue(spanFree(block, start, slices));

        List<ByteBuf> ofNewIncarnation = new ArrayList<ByteBuf>();
        List<ByteBuf> ofD = new ArrayList<ByteBuf>();
        ofD.add(firstOfD);
        if (!"idle".equals(variant)) {
            // Run D out: the next chunk is the same object, C'.
            for (int i = 1; i < segments; i++) {
                ofD.add(allocator.allocate(SIZE, SIZE));
            }
            ByteBuf buf = allocator.allocate(SIZE, SIZE);
            assertSame(chunk, chunkOf(buf), "the idle chunk object serves the next chunk");
            assertNull(chunk.queue);
            ofNewIncarnation.add(buf);
            allocator.allocate(SIZE, SIZE).release();
            if ("refiled".equals(variant)) {
                while (ofNewIncarnation.size() < segments) {
                    ofNewIncarnation.add(allocator.allocate(SIZE, SIZE));
                }
                ByteBuf next = allocator.allocate(SIZE, SIZE);
                assertNotSame(chunk, chunkOf(next));
                ofD.add(next);
                assertSame(cache.exhausted, chunk.queue);
            }
        }

        // Second half: the note, on whatever the chunk is now; the owner drains it.
        runOnAnotherThread(() -> cache.notifyHasCapacity(chunk));
        assertEquals(1, cache.pendingCount());
        asOwner(allocator, stripe, cache::drainPending);
        assertEquals(0, cache.pendingCount());

        if ("idle".equals(variant)) {
            assertSame(cache.idle, chunk.queue, "an idle chunk stays idle");
            assertTrue(spanFree(block, start, slices), "and its span free");
        } else {
            Segment newBlock = chunk.segment;
            assertFalse(spanFree(newBlock, chunk.spanStart, chunk.spanSlices()), "C' holds its span");
            if ("refiled".equals(variant)) {
                assertSame(cache.exhausted, chunk.queue, "no free segment: it stays exhausted");
            } else {
                assertNull(chunk.queue, "the magazine's chunk stays on no list");
            }
            assertEachSegmentHandedOutOnce(allocator, chunk, ofNewIncarnation, segments);
            int newStart = chunk.spanStart;
            for (ByteBuf buf : ofNewIncarnation) {
                buf.release();
            }
            assertTrue(chunk.hasFullCapacity(), "every segment of C' is back, and counted once");
            assertSame(cache.idle, chunk.queue, "above the floor: given up again at its last release");
            assertTrue(spanFree(newBlock, newStart, slices), "its span given back, once");
        }
        for (ByteBuf buf : ofD) {
            buf.release();
        }
        for (ByteBuf buf : padding) {
            buf.release();
        }
    }

    /**
     * The free segments of {@code chunk}, the magazine's or a filed one, as the owner hands them out: together with
     * {@code live} they are every segment, each once.
     */
    private static void assertEachSegmentHandedOutOnce(AdaptivePoolingAllocator allocator, SizeClassedChunk chunk,
                                                       List<ByteBuf> live, int segments) throws Exception {
        Set<Integer> offsets = new HashSet<Integer>();
        for (ByteBuf buf : live) {
            assertTrue(offsets.add(startIndexOf(buf)));
        }
        int free = chunk.freeSegmentCount();
        assertEquals(segments, free + live.size(), "free and live segments of C'");
        if (chunk.queue != null) {
            // Filed: nothing allocates from it, so the count is all there is to check.
            return;
        }
        List<ByteBuf> taken = new ArrayList<ByteBuf>();
        for (int i = 0; i < free; i++) {
            ByteBuf buf = allocator.allocate(SIZE, SIZE);
            assertSame(chunk, chunkOf(buf));
            assertTrue(offsets.add(startIndexOf(buf)), "segment handed out twice");
            taken.add(buf);
        }
        live.addAll(taken);
        assertEquals(segments, offsets.size());
    }

    /**
     * Steady churn makes no chunk object once the cache has had its peak: a burst of several chunks, all released, so
     * that the chunks above the retention floor are given up, again and again.
     */
    @ParameterizedTest
    @CsvSource({"false", "true"})
    void churnMakesNoMoreChunkObjectsThanItsPeak(boolean stripe) throws Throwable {
        assumeFalse(!stripe && isLowMemory(), "low-memory mode has no thread-local heaps");
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(new CountingMemorySource(true),
                SEGMENT_SIZE));
        onOwner(stripe, () -> {
            final int chunks = SizeClassedChunkCache.FLOOR + 4;
            SizeClassedChunkCache cache = null;
            int peak = 0;
            for (int round = 0; round < 50; round++) {
                List<ByteBuf> burst = new ArrayList<ByteBuf>();
                Set<SizeClassedChunk> used = new HashSet<SizeClassedChunk>();
                for (int i = 0; i < chunks * 31; i++) {
                    ByteBuf buf = allocator.allocate(SIZE, SIZE);
                    used.add(chunkOf(buf));
                    burst.add(buf);
                }
                cache = chunkOf(burst.get(0)).owningCache;
                peak = Math.max(peak, used.size());
                for (ByteBuf buf : burst) {
                    buf.release();
                }
                assertTrue(cache.idle.size() > 0, "round " + round + ": chunks given up to the idle list");
            }
            assertTrue(cache.chunksMade <= peak, cache.chunksMade + " chunk objects made, peak " + peak);
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

    /** As the cache's owner: the calling thread on a thread-local heap, under the stripe lock on a stripe. */
    private static void asOwner(AdaptivePoolingAllocator allocator, boolean stripe, Runnable action)
            throws Exception {
        if (!stripe) {
            action.run();
            return;
        }
        List<StampedLock> locks = new ArrayList<StampedLock>();
        for (Object heap : (Object[]) AdaptiveSegmentsTest.field(allocator, "stripedHeaps")) {
            locks.add((StampedLock) AdaptiveSegmentsTest.field(heap, "lock"));
        }
        List<Long> stamps = new ArrayList<Long>();
        for (StampedLock lock : locks) {
            stamps.add(lock.writeLock());
        }
        try {
            action.run();
        } finally {
            for (int i = 0; i < locks.size(); i++) {
                locks.get(i).unlockWrite(stamps.get(i));
            }
        }
    }

    private static boolean spanFree(Segment block, int start, int slices) {
        long bits = block.bits(start, slices);
        return (block.free & bits) == bits;
    }

    private static SizeClassedChunk chunkOf(ByteBuf buf) {
        return (SizeClassedChunk) ((AdaptiveByteBuf) buf).chunk;
    }

    private static int startIndexOf(ByteBuf buf) throws Exception {
        Field f = AdaptiveByteBuf.class.getDeclaredField("startIndex");
        f.setAccessible(true);
        return f.getInt(buf);
    }

    private static int segmentsOf(SizeClassedChunk chunk) throws Exception {
        Field f = SizeClassedChunk.class.getDeclaredField("segments");
        f.setAccessible(true);
        return f.getInt(chunk);
    }

    private static boolean isLowMemory() throws Exception {
        Field f = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        f.setAccessible(true);
        return f.getBoolean(null);
    }
}
