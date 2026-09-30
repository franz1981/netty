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

import io.netty.buffer.PageStoreTestSupport.CountingRegionSource;
import io.netty.buffer.PageStoreTestSupport.CountingSegmentSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.committedSlots;
import static io.netty.buffer.PageStoreTestSupport.giveBack;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link PageStore}: where a heap's segments come from (the fullest region, the heap's offset on a tie, a new region),
 * segments as views of committed region slots, slots owned once under contention, the fallback of one allocation per
 * segment, and the accounting through a random workload and the close.
 */
final class PageStoreTest {
    private static final int SLOTS = REGION_SIZE / SEGMENT_SIZE;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();

    /** A free slot of the fullest region, its lowest; then a new region. */
    @Test
    void takesFromTheFullestRegionThenANewRegion() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        Segment a = heap.claim(63);
        heap.claim(63);
        giveBack(heap, a, 0, 63);
        Segment again = store.take(heap);
        assertSame(a, again, "slot 0 is free again: the lowest");
        assertSame(heap, again.owner);
        Segment fromRegion = store.take(heap);
        assertSame(a.region, fromRegion.region);
        assertEquals(2, fromRegion.slot, "the lowest free slot of the fullest region");
        for (int slot = 3; slot < SLOTS; slot++) {
            assertEquals(slot, store.take(heap).slot);
        }
        assertEquals(1, store.regionCount());
        Segment fresh = store.take(heap);
        assertNotSame(a.region, fresh.region, "no free slot left: a new region");
        assertEquals(0, fresh.slot);
        assertEquals(2, store.regionCount());
        store.free(releaseOwnership(again));
        assertAccounted(segments, regions, allocator);
    }

    /** A segment taken by take() and never claimed from is given back as {@link HeapSegments#afterFree} would. */
    private static Segment releaseOwnership(Segment segment) {
        assertTrue(segment.isWhollyFree());
        assertTrue(Segment.OWNER.compareAndSet(segment, segment.owner, null));
        return segment;
    }

    /**
     * Segments are views of their region at their slot's offset, and no segment is allocated on its own. A slot counts
     * in the used memory from its first take on, back in the region or not; taking it again adds nothing.
     */
    @Test
    void segmentsAreViewsOfCommittedSlots() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < SLOTS; i++) {
            Segment segment = heap.claim(63);
            taken.add(segment);
            assertEquals(i, segment.slot);
            assertSame(taken.get(0).region, segment.region);
            assertEquals((i + 1L) * SEGMENT_SIZE, allocator.usedMemory());
        }
        assertEquals(0, segments.segmentsAllocated(), "no segment of its own");
        assertEquals(1, regions.regions.size());
        assertEquals(SLOTS, store.segmentsCommitted);
        Region region = taken.get(0).region;
        long base = region.buffer.memoryAddress();
        for (Segment segment : taken) {
            assertEquals(base + (long) segment.slot * SEGMENT_SIZE, segment.memoryAddress());
            segment.buffer.setLong(SEGMENT_SIZE - 8, segment.slot);
        }
        for (Segment segment : taken) {
            assertEquals(segment.slot, region.buffer.getLong(segment.slot * SEGMENT_SIZE + SEGMENT_SIZE - 8));
        }
        giveBack(heap, taken.get(3), 0, 63);
        assertEquals((long) SLOTS * SEGMENT_SIZE, allocator.usedMemory(), "back in its region, still committed");
        assertSame(taken.get(3), heap.claim(63), "the same segment again");
        assertEquals(SLOTS, store.segmentsCommitted, "no new commit");
        assertEquals((long) SLOTS * SEGMENT_SIZE, allocator.usedMemory());
        assertAccounted(segments, regions, allocator);
    }

    /** Where regions are mapped, a region starts at a multiple of 2 MiB, and so does each of its (4 MiB) segments. */
    @Test
    void regionsAreAligned() {
        assumeTrue(regions.mmap != null, "aligned regions need mmap");
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        for (int i = 0; i < 3; i++) {
            Segment segment = heap.claim(63);
            assertEquals(0, segment.memoryAddress() & REGION_ALIGNMENT - 1, "segment " + i);
        }
        assertEquals(0, allocator.pageStore.regions[0].buffer.memoryAddress() & REGION_ALIGNMENT - 1);
    }

    /** Equally full regions: each heap starts from its own offset, so two heaps take from different regions. */
    @Test
    void equallyFullRegionsSpreadByTheHeapsOffset() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments filler = new HeapSegments(store, null, Thread.currentThread());
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < 2 * SLOTS; i++) {
            taken.add(store.take(filler));
        }
        assertEquals(2, store.regionCount());
        store.free(releaseOwnership(taken.get(1)));
        store.free(releaseOwnership(taken.get(SLOTS + 1)));
        HeapSegments first = new HeapSegments(store, null, Thread.currentThread());
        first.regionOffset = 0;
        HeapSegments second = new HeapSegments(store, null, Thread.currentThread());
        second.regionOffset = 1;
        Segment fromFirst = store.take(first);
        assertSame(store.regions[0], fromFirst.region);
        store.free(releaseOwnership(fromFirst));
        assertSame(store.regions[1], store.take(second).region);
        // A fuller region wins whatever the offset.
        store.free(releaseOwnership(taken.get(2)));
        store.free(releaseOwnership(taken.get(3)));
        store.free(releaseOwnership(taken.get(SLOTS + 2)));
        assertSame(store.regions[1], store.take(first).region);
    }

    /**
     * Eight threads take and give back segments of one store, two at most each: a segment is held by one thread at a
     * time, and since 16 fit in two regions of 9 slots, no third region is ever mapped.
     */
    @Test
    void contendedSegmentsAreOwnedOnce() throws Exception {
        final AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        final PageStore store = allocator.pageStore;
        final int threads = 8;
        final ConcurrentHashMap<Segment, Integer> holders = new ConcurrentHashMap<Segment, Integer>();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final CyclicBarrier start = new CyclicBarrier(threads);
        List<Thread> workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            Thread worker = new Thread(() -> {
                try {
                    HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
                    Random random = new Random(id);
                    Segment[] held = new Segment[2];
                    int count = 0;
                    start.await();
                    for (int r = 0; r < 50_000; r++) {
                        if (count < held.length && random.nextBoolean()) {
                            Segment segment = store.take(heap);
                            Integer previous = holders.putIfAbsent(segment, id);
                            if (previous != null) {
                                throw new AssertionError(segment + " taken by " + id + " is held by " + previous);
                            }
                            held[count++] = segment;
                        } else if (count > 0) {
                            int k = random.nextInt(count);
                            Segment segment = held[k];
                            held[k] = held[--count];
                            holders.remove(segment);
                            store.free(releaseOwnership(segment));
                        }
                    }
                    while (count > 0) {
                        Segment segment = held[--count];
                        holders.remove(segment);
                        store.free(releaseOwnership(segment));
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
        assertTrue(store.regionCount() <= 2, store.regionCount() + " regions");
        for (Region region : store.regions) {
            assertEquals(region.allSlots, region.free);
        }
        assertAccounted(segments, regions, allocator);
    }

    /**
     * Several heaps claiming and releasing at random, with their spares ageing and idle slots purged: at every step
     * the allocator's used memory is exactly the segments allocated on their own and not freed plus the committed
     * slots; once the heaps are freed and the store closed, nothing is left.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void accountingMatchesTheSourcesThroughARandomWorkload(boolean withRegions) {
        AdaptivePoolingAllocator allocator = withRegions ?
                newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT) :
                newAllocator(segments, SEGMENT_SIZE);
        PageStore store = allocator.pageStore;
        HeapSegments[] heaps = new HeapSegments[3];
        for (int i = 0; i < heaps.length; i++) {
            heaps[i] = new HeapSegments(store, null, Thread.currentThread());
        }
        int capacity = 48;
        HeapSegments[] owners = new HeapSegments[capacity];
        Segment[] spans = new Segment[capacity];
        int[] starts = new int[capacity];
        int[] lengths = new int[capacity];
        int live = 0;
        long now = System.nanoTime();
        Random random = new Random(11);
        for (int op = 0; op < 10_000; op++) {
            int dice = random.nextInt(16);
            if (dice == 0) {
                heaps[random.nextInt(heaps.length)].decay(now += INTERVAL / 2);
            } else if (live == capacity || live > 0 && dice < 8) {
                int k = random.nextInt(live);
                owners[k].release(spans[k], starts[k], lengths[k]);
                live--;
                owners[k] = owners[live];
                spans[k] = spans[live];
                starts[k] = starts[live];
                lengths[k] = lengths[live];
            } else {
                HeapSegments heap = heaps[random.nextInt(heaps.length)];
                int n = 1 + random.nextInt(16);
                owners[live] = heap;
                spans[live] = heap.claim(n);
                starts[live] = heap.claimedStart();
                lengths[live] = n;
                live++;
            }
            assertAccounted(segments, regions, allocator);
        }
        while (live > 0) {
            live--;
            owners[live].release(spans[live], starts[live], lengths[live]);
        }
        for (HeapSegments heap : heaps) {
            assertEquals(0, heap.count, "no span out: a spare at most");
            heap.markFreed();
            heap.afterFree();
        }
        assertEquals(0, segments.segmentsLive());
        assertEquals(withRegions, !regions.regions.isEmpty());
        assertEquals(withRegions, store.segmentsPurged > 0, "the decays purged idle slots");
        assertAccounted(segments, regions, allocator);
        store.close();
        assertEquals(0, allocator.usedMemory());
        assertEquals(0, regions.live(), "the close unmaps every region");
        assertEquals(0, store.regionCount());
    }

    /** The close unmaps every region, with the segments still in heaps, and accounts all of them as freed. */
    @Test
    void closeUnmapsEveryRegion() {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        PageStore store = allocator.pageStore;
        HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
        for (int i = 0; i < SLOTS + 2; i++) {
            heap.claim(63);
        }
        assertEquals(2, regions.live());
        assertEquals((SLOTS + 2L) * SEGMENT_SIZE, allocator.usedMemory());
        store.close();
        assertEquals(0, regions.live());
        assertEquals(0, allocator.usedMemory());
        assertEquals(0, committedSlots(store));
        assertThrows(IllegalStateException.class, () -> store.take(heap));
    }

    /**
     * A segment of a freed heap emptied by another thread's release goes straight back to its region's free slots, from
     * that thread.
     */
    @Test
    void foreignReleaseGivesTheSlotBack() throws Exception {
        AdaptivePoolingAllocator allocator = newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
        final HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
        final Segment segment = heap.claim(10);
        heap.markFreed();
        heap.afterFree();
        assertEquals(SLOTS - 1, segment.region.freeSlotCount());
        Thread releaser = new Thread(() -> heap.release(segment, 0, 10));
        releaser.start();
        releaser.join();
        assertNull(segment.owner);
        assertEquals(SLOTS, segment.region.freeSlotCount());
        assertAccounted(segments, regions, allocator);
    }

    /**
     * Regions off, by a config without them or without a region source: one allocation per segment, freed when it is
     * given back.
     */
    @Test
    void withoutRegionsSegmentsAreAllocatedOneByOne() {
        AdaptivePoolingAllocator noRegions = newAllocator(segments, regions, 0, 0);
        assertNull(noRegions.pageStore.regionSource);
        AdaptivePoolingAllocator noSource = new AdaptivePoolingAllocator(segments, true, segments, null,
                new PageStoreConfig(SEGMENT_SIZE, SLICE_SIZE_BYTES, INTERVAL, REGION_SIZE, REGION_ALIGNMENT));
        assertNull(noSource.pageStore.regionSource);
        for (AdaptivePoolingAllocator allocator : new AdaptivePoolingAllocator[] {noRegions, noSource}) {
            HeapSegments heap = new HeapSegments(allocator.pageStore, null, Thread.currentThread());
            Segment segment = heap.claim(10);
            assertNull(segment.region);
            assertNotNull(segment.buffer);
            assertEquals(SEGMENT_SIZE, allocator.usedMemory());
            giveBack(heap, segment, 0, 10);
            assertEquals(0, allocator.usedMemory());
        }
        assertEquals(2, segments.segmentsAllocated());
        assertEquals(0, segments.segmentsLive());
        assertEquals(0, regions.regions.size());
    }
}
