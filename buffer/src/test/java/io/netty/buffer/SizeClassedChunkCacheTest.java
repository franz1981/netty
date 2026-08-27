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
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunkCache;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SizeClassedChunkCacheTest {

    private static final int CHUNK_SIZE = 128 * 1024;

    /**
     * Mockito instantiates mocks without running constructors (Objenesis), so instance field
     * initializers do not run and {@code cacheIndex} would default to 0 -- a valid slot. Every
     * mock has to be told it is not cached.
     */
    private static SizeClassedChunk newMock() {
        SizeClassedChunk chunk = mock(SizeClassedChunk.class);
        chunk.cacheIndex = SizeClassedChunkCache.NOT_CACHED;
        return chunk;
    }

    private static SizeClassedChunkCache newCache() {
        return new SizeClassedChunkCache(CHUNK_SIZE, null, 0);
    }

    /** Has at least one free segment, but not all of them. */
    private static SizeClassedChunk reusableChunk() {
        SizeClassedChunk chunk = newMock();
        when(chunk.remainingCapacity()).thenReturn(512);
        when(chunk.capacity()).thenReturn(CHUNK_SIZE);
        when(chunk.hasRemainingCapacity()).thenReturn(true);
        when(chunk.hasFullCapacity()).thenReturn(false);
        return chunk;
    }

    /** No free segments at all. */
    private static SizeClassedChunk exhaustedChunk() {
        SizeClassedChunk chunk = newMock();
        when(chunk.remainingCapacity()).thenReturn(0);
        when(chunk.capacity()).thenReturn(CHUNK_SIZE);
        when(chunk.hasRemainingCapacity()).thenReturn(false);
        when(chunk.hasFullCapacity()).thenReturn(false);
        return chunk;
    }

    /** Every segment back: eviction candidate. */
    private static SizeClassedChunk fullyFreeChunk() {
        SizeClassedChunk chunk = newMock();
        when(chunk.remainingCapacity()).thenReturn(CHUNK_SIZE);
        when(chunk.capacity()).thenReturn(CHUNK_SIZE);
        when(chunk.hasRemainingCapacity()).thenReturn(true);
        when(chunk.hasFullCapacity()).thenReturn(true);
        return chunk;
    }

    /** Simulates a segment coming back into an exhausted chunk. */
    private static void gainCapacity(SizeClassedChunk chunk) {
        when(chunk.remainingCapacity()).thenReturn(512);
        when(chunk.hasRemainingCapacity()).thenReturn(true);
    }

    // --- admission: no ceiling ---

    @Test
    void offerChunkNeverRefuses() {
        SizeClassedChunkCache cache = newCache();
        // Far past both the old maxCachedChunks (8 MiB / 128 KiB = 64) and the initial array size.
        int many = 4096;
        for (int i = 0; i < many; i++) {
            assertTrue(cache.offerChunk(exhaustedChunk()), "offer " + i + " must be accepted");
        }
        assertEquals(many, cache.count);
    }

    @Test
    void offerChunkClassifiesByCapacity() {
        SizeClassedChunkCache cache = newCache();
        cache.offerChunk(exhaustedChunk());
        cache.offerChunk(reusableChunk());
        cache.offerChunk(exhaustedChunk());
        assertEquals(3, cache.count);
        assertEquals(1, cache.reusableCount);
    }

    // --- array invariant: chunks[c.cacheIndex] == c after every mutation ---

    private static void assertIndicesConsistent(SizeClassedChunkCache cache) {
        for (int i = 0; i < cache.count; i++) {
            SizeClassedChunk c = cache.chunks[i];
            assertNotNull(c, "slot " + i + " is null below count=" + cache.count);
            assertEquals(i, c.cacheIndex, "chunk at slot " + i + " disagrees about its index");
        }
        for (int i = cache.count; i < cache.chunks.length; i++) {
            assertNull(cache.chunks[i], "stale reference at slot " + i);
        }
    }

    @Test
    void swapPartitionKeepsEveryIndexValid() {
        SizeClassedChunkCache cache = newCache();
        List<SizeClassedChunk> exhausted = new ArrayList<SizeClassedChunk>();
        // Interleave so the regions are not trivially ordered, and grow past the initial array.
        for (int i = 0; i < 40; i++) {
            if ((i & 1) == 0) {
                SizeClassedChunk c = exhaustedChunk();
                exhausted.add(c);
                cache.offerChunk(c);
            } else {
                cache.offerChunk(reusableChunk());
            }
            assertIndicesConsistent(cache);
        }
        // Move exhausted chunks to reusable from the middle out, in a scrambled order.
        for (int i = exhausted.size() - 1; i >= 0; i -= 3) {
            SizeClassedChunk c = exhausted.get(i);
            gainCapacity(c);
            cache.moveToReusable(c);
            assertIndicesConsistent(cache);
        }
        // Drain the reusable region.
        while (cache.pollChunk(256) != null) {
            assertIndicesConsistent(cache);
        }
    }

    @Test
    void growthPreservesIndices() {
        SizeClassedChunkCache cache = newCache();
        int initialLength = cache.chunks.length;
        for (int i = 0; i < initialLength * 4 + 1; i++) {
            cache.offerChunk(reusableChunk());
        }
        assertTrue(cache.chunks.length > initialLength);
        assertIndicesConsistent(cache);
    }

    // --- poll ---

    @Test
    void pollTakesFromTheReusableRegion() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk noCap = exhaustedChunk();
        SizeClassedChunk cap = reusableChunk();
        cache.offerChunk(noCap);
        cache.offerChunk(cap);
        assertSame(cap, cache.pollChunk(256));
        assertEquals(SizeClassedChunkCache.NOT_CACHED, cap.cacheIndex);
        assertEquals(1, cache.count);
    }

    @Test
    void pollReturnsNullWhenEmpty() {
        assertNull(newCache().pollChunk(256));
    }

    @Test
    void pollProbesTheExhaustedRegionForAnUndrainedCapacityGain() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk chunk = exhaustedChunk();
        cache.offerChunk(chunk);
        assertNull(cache.pollChunk(256));

        // A segment came back but no note was left (or it has not been drained).
        gainCapacity(chunk);
        assertSame(chunk, cache.pollChunk(256));
    }

    @Test
    void pollProbeIsBounded() {
        SizeClassedChunkCache cache = newCache();
        // The probe visits at most 8 exhausted chunks; hide the usable one behind more than that.
        for (int i = 0; i < 32; i++) {
            cache.offerChunk(exhaustedChunk());
        }
        SizeClassedChunk hidden = exhaustedChunk();
        cache.offerChunk(hidden);
        gainCapacity(hidden);
        assertNull(cache.pollChunk(256), "probe must not walk the whole exhausted region");
    }

    // --- eviction: the floor is the only bound ---

    @Test
    void tickPurgeEvictsFullyFreeChunksAboveFloor() {
        SizeClassedChunkCache cache = newCache();
        int floor = cache.purgeRetentionFloor;
        for (int i = 0; i < floor; i++) {
            cache.offerChunk(exhaustedChunk());
        }
        SizeClassedChunk[] excess = new SizeClassedChunk[7];
        for (int i = 0; i < excess.length; i++) {
            excess[i] = fullyFreeChunk();
            cache.offerChunk(excess[i]);
        }
        cache.tickPurge();
        for (SizeClassedChunk c : excess) {
            verify(c, atLeastOnce()).recycleOrDeallocate(null, 0);
        }
        assertEquals(floor, cache.count);
        assertIndicesConsistent(cache);
    }

    @Test
    void tickPurgeStopsAtTheFloor() {
        SizeClassedChunkCache cache = newCache();
        int floor = cache.purgeRetentionFloor;
        SizeClassedChunk[] all = new SizeClassedChunk[floor];
        for (int i = 0; i < floor; i++) {
            all[i] = fullyFreeChunk();
            cache.offerChunk(all[i]);
        }
        cache.tickPurge();
        assertEquals(floor, cache.count);
        for (SizeClassedChunk c : all) {
            verify(c, never()).recycleOrDeallocate(null, 0);
        }
    }

    @Test
    void tickPurgeDoesNotEvictChunksThatStillHoldSegments() {
        SizeClassedChunkCache cache = newCache();
        for (int i = 0; i < cache.purgeRetentionFloor; i++) {
            cache.offerChunk(exhaustedChunk());
        }
        SizeClassedChunk active = reusableChunk();
        cache.offerChunk(active);
        cache.tickPurge();
        verify(active, never()).recycleOrDeallocate(null, 0);
        assertSame(active, cache.pollChunk(256));
    }

    // --- release-path transitions (no scan involved) ---

    @Test
    void transitionAfterReleaseMovesExhaustedToReusable() {
        SizeClassedChunkCache cache = newCache();
        cache.offerChunk(exhaustedChunk());
        SizeClassedChunk chunk = exhaustedChunk();
        cache.offerChunk(chunk);
        cache.offerChunk(exhaustedChunk());
        assertEquals(0, cache.reusableCount);

        gainCapacity(chunk);
        cache.transitionAfterRelease(chunk);

        assertEquals(1, cache.reusableCount);
        assertIndicesConsistent(cache);
        assertSame(chunk, cache.pollChunk(256));
    }

    @Test
    void transitionAfterReleaseEvictsWhenTheLastSegmentComesBack() {
        SizeClassedChunkCache cache = newCache();
        for (int i = 0; i < cache.purgeRetentionFloor; i++) {
            cache.offerChunk(exhaustedChunk());
        }
        SizeClassedChunk chunk = exhaustedChunk();
        cache.offerChunk(chunk);
        int before = cache.count;

        when(chunk.remainingCapacity()).thenReturn(CHUNK_SIZE);
        when(chunk.hasRemainingCapacity()).thenReturn(true);
        when(chunk.hasFullCapacity()).thenReturn(true);
        cache.transitionAfterRelease(chunk);

        verify(chunk).recycleOrDeallocate(null, 0);
        assertEquals(before - 1, cache.count);
        assertEquals(SizeClassedChunkCache.NOT_CACHED, chunk.cacheIndex);
        assertIndicesConsistent(cache);
    }

    @Test
    void transitionAfterReleaseKeepsTheChunkAtTheFloor() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk chunk = fullyFreeChunk();
        cache.offerChunk(chunk);
        cache.transitionAfterRelease(chunk);
        verify(chunk, never()).recycleOrDeallocate(null, 0);
        assertEquals(1, cache.count);
    }

    // --- notification protocol ---

    @Test
    void drainMovesANotifiedExhaustedChunkToReusable() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk chunk = exhaustedChunk();
        cache.offerChunk(chunk);

        gainCapacity(chunk);
        cache.notifyHasCapacity(chunk);
        assertEquals(1, cache.pendingCount());

        assertSame(chunk, cache.pollChunk(256)); // pollChunk drains first
        assertEquals(0, cache.pendingCount());
    }

    @Test
    void drainEvictsANotifiedChunkThatBecameFullyFree() {
        SizeClassedChunkCache cache = newCache();
        for (int i = 0; i < cache.purgeRetentionFloor; i++) {
            cache.offerChunk(exhaustedChunk());
        }
        SizeClassedChunk chunk = exhaustedChunk();
        cache.offerChunk(chunk);

        when(chunk.remainingCapacity()).thenReturn(CHUNK_SIZE);
        when(chunk.hasRemainingCapacity()).thenReturn(true);
        when(chunk.hasFullCapacity()).thenReturn(true);
        cache.notifyHasCapacity(chunk);

        cache.tickPurge();
        verify(chunk, atLeastOnce()).recycleOrDeallocate(null, 0);
        assertEquals(SizeClassedChunkCache.NOT_CACHED, chunk.cacheIndex);
    }

    @Test
    void drainIgnoresChunksThatLeftTheCache() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk chunk = reusableChunk();
        cache.offerChunk(chunk);
        assertSame(chunk, cache.pollChunk(256));

        cache.notifyHasCapacity(chunk);
        cache.tickPurge();
        verify(chunk, never()).recycleOrDeallocate(null, 0);
        assertEquals(0, cache.count);
    }

    // Invariant N property 4: a chunk classified as exhausted while it already had capacity is
    // fixed by the note, which cannot be consumed between the classification read and the insert.
    @Test
    void aChunkMisclassifiedAtOfferTimeStillBecomesReusable() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk chunk = exhaustedChunk();
        // The segment is already in the free list, but offerChunk's capacity read misses it --
        // the return landed right after the read and right before the insert. Only the first
        // read (the one offerChunk does) reports no capacity.
        final AtomicBoolean firstRead = new AtomicBoolean(true);
        when(chunk.hasRemainingCapacity()).thenAnswer(invocation -> !firstRead.getAndSet(false));
        cache.notifyHasCapacity(chunk);
        cache.offerChunk(chunk);
        assertEquals(0, cache.reusableCount, "offer must have read the stale capacity");

        // Nothing scans; the note is the only thing that can fix this.
        assertSame(chunk, cache.pollChunk(256));
    }

    // Invariant N property 2: one outstanding note covers any number of later returns.
    @Test
    void concurrentReturnsOnOneChunkQueueItAtMostOncePerDrain() throws Exception {
        final SizeClassedChunkCache cache = newCache();
        final SizeClassedChunk chunk = exhaustedChunk();
        cache.offerChunk(chunk);

        int threads = 8;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicInteger errors = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        for (int j = 0; j < 1000; j++) {
                            cache.notifyHasCapacity(chunk);
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertEquals(0, errors.get());
        assertEquals(1, cache.pendingCount(), "the dedup claim must admit exactly one note");
    }

    // Invariant N property 3: re-arm before processing, so a return landing during processing is
    // not lost. Modelled by re-notifying from inside the capacity read that processPending does.
    @Test
    void aReturnLandingDuringProcessingRequeuesTheChunk() {
        final SizeClassedChunkCache cache = newCache();
        final SizeClassedChunk chunk = newMock();
        when(chunk.capacity()).thenReturn(CHUNK_SIZE);
        when(chunk.remainingCapacity()).thenReturn(0);
        when(chunk.hasFullCapacity()).thenReturn(false);
        // First read (offerChunk) says exhausted. The read done by processPending re-notifies,
        // standing in for a segment return that lands while the drain is inside processPending.
        when(chunk.hasRemainingCapacity()).thenAnswer(invocation -> {
            cache.notifyHasCapacity(chunk);
            return false;
        });
        cache.offerChunk(chunk);
        cache.notifyHasCapacity(chunk);

        cache.drainPending();
        assertEquals(1, cache.pendingCount(), "the re-notification must have been accepted");
    }

    // --- free ---

    @Test
    void freeDrainsEveryChunkIncludingExhaustedOnes() {
        SizeClassedChunkCache cache = newCache();
        SizeClassedChunk cap1 = reusableChunk();
        SizeClassedChunk cap2 = reusableChunk();
        SizeClassedChunk noCap1 = exhaustedChunk();
        SizeClassedChunk noCap2 = exhaustedChunk();
        cache.offerChunk(cap1);
        cache.offerChunk(noCap1);
        cache.offerChunk(cap2);
        cache.offerChunk(noCap2);

        cache.free();

        assertTrue(cache.isEmpty());
        assertEquals(0, cache.reusableCount);
        assertIndicesConsistent(cache);
        verify(cap1, atLeastOnce()).markToDeallocate();
        verify(cap2, atLeastOnce()).markToDeallocate();
        verify(noCap1, atLeastOnce()).markToDeallocate();
        verify(noCap2, atLeastOnce()).markToDeallocate();
    }

    @Test
    void pollCannotDrainExhaustedChunks() {
        SizeClassedChunkCache cache = newCache();
        cache.offerChunk(reusableChunk());
        cache.offerChunk(exhaustedChunk());
        cache.offerChunk(reusableChunk());
        cache.offerChunk(exhaustedChunk());

        int drained = 0;
        while (cache.pollChunk(0) != null && drained < 100) {
            drained++;
        }
        assertEquals(2, drained);
        // The exhausted ones are still held: this is why free() exists.
        assertEquals(2, cache.count);
    }
}
