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
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.REGION_ALIGNMENT;
import static io.netty.buffer.PageStoreTestSupport.REGION_SIZE;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The purge of idle free slots ({@link PageStore#purgeIfDue}): only slots free through a whole interval, one call per
 * run of contiguous slots, one purger at a time, slots taken again after a purge, and the accounting and resident
 * memory that follow. Decays are forced with made-up clocks, one interval apart: nothing sleeps.
 */
@Isolated("Reads the process's resident memory, which concurrent tests would move")
final class RegionPurgeTest {
    private static final int SLOTS = REGION_SIZE / SEGMENT_SIZE;

    private final CountingSegmentSource segments = new CountingSegmentSource();
    private final CountingRegionSource regions = new CountingRegionSource();
    private final AdaptivePoolingAllocator allocator =
            newAllocator(segments, regions, REGION_SIZE, REGION_ALIGNMENT);
    private final PageStore store = allocator.pageStore;
    private final HeapSegments heap = new HeapSegments(store, null, Thread.currentThread());
    private static final long DELAY = INTERVAL; // the allocator's purge delay, see newAllocator
    private static final long CHECK = DELAY / 4;
    /** The made-up time of the last {@link #purgeAt}; no earlier than the store's creation. */
    private long lastPurgeAt = System.nanoTime();

    /** A pass of the store's purge at {@code at}, or a check interval after the previous one when that is later. */
    private void purgeAt(long at) {
        lastPurgeAt = Math.max(at, lastPurgeAt + CHECK);
        store.purgeIfDue(lastPurgeAt);
    }

    private List<Segment> takeAll(int n) {
        List<Segment> taken = new ArrayList<Segment>();
        for (int i = 0; i < n; i++) {
            taken.add(store.take(heap));
        }
        return taken;
    }

    /** As {@link HeapSegments#afterFree} gives back a segment it took and never claimed from. */
    private void giveBack(Segment segment) {
        assertTrue(Segment.OWNER.compareAndSet(segment, segment.owner, null));
        store.free(segment);
    }

    /** Slots 1-3, 5-6 and 8 free: three runs, three calls; the used memory drops by the six segments. */
    @Test
    void oneCallPerRunOfContiguousSlots() {
        List<Segment> taken = takeAll(SLOTS);
        long before = System.nanoTime();
        for (int slot : new int[] {1, 2, 3, 5, 6, 8}) {
            giveBack(taken.get(slot));
        }
        long after = System.nanoTime();
        purgeAt(before + DELAY - CHECK);
        assertEquals(0, regions.purgeCalls(), "free for less than the delay");
        purgeAt(after + DELAY);
        assertEquals(3, regions.purgeCalls());
        assertArrayEquals(new int[] {SEGMENT_SIZE, 3 * SEGMENT_SIZE}, regions.purges.get(0));
        assertArrayEquals(new int[] {5 * SEGMENT_SIZE, 2 * SEGMENT_SIZE}, regions.purges.get(1));
        assertArrayEquals(new int[] {8 * SEGMENT_SIZE, SEGMENT_SIZE}, regions.purges.get(2));
        assertEquals(3, store.purgeCalls);
        assertEquals(6, store.segmentsPurged);
        assertEquals(6L * SEGMENT_SIZE, store.bytesPurged);
        assertEquals(3L * SEGMENT_SIZE, allocator.usedMemory());
        assertArrayEquals(new int[] {3, 0, 6}, store.slotCounts());
        assertAccounted(segments, regions, allocator);

        // The rest back: all nine free, one run, one call, for the three not purged yet.
        for (int slot : new int[] {0, 4, 7}) {
            giveBack(taken.get(slot));
        }
        purgeAt(System.nanoTime() + DELAY);
        assertEquals(6, regions.purgeCalls(), "0, 4 and 7 are not contiguous");
        assertEquals(0, allocator.usedMemory());
        assertArrayEquals(new int[] {0, 0, SLOTS}, store.slotCounts());
    }

    /** A purge claims one run at a time: while one run is purged, the free slots of the others can be taken. */
    @Test
    void aPurgeHoldsOneRunAtATime() {
        List<Segment> taken = takeAll(SLOTS);
        for (int slot : new int[] {1, 2, 3, 5, 6}) {
            giveBack(taken.get(slot));
        }
        final Region region = taken.get(0).region;
        final List<Long> freeDuringPurge = new ArrayList<Long>();
        regions.onPurge = () -> freeDuringPurge.add(region.free);
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now += INTERVAL);
        assertEquals(2, freeDuringPurge.size());
        assertEquals(0b1100000L, (long) freeDuringPurge.get(0), "1-3 held, 5-6 free");
        assertEquals(0b0001110L, (long) freeDuringPurge.get(1), "5-6 held, 1-3 free again");
        assertEquals(0b1101110L, region.free);
        assertEquals(5, store.segmentsPurged);
        assertAccounted(segments, regions, allocator);
    }

    @Test
    void aWholeRegionFreeIsOneCall() {
        List<Segment> taken = takeAll(SLOTS);
        for (Segment segment : taken) {
            giveBack(segment);
        }
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now += INTERVAL);
        assertEquals(1, regions.purgeCalls());
        assertArrayEquals(new int[] {0, REGION_SIZE}, regions.purges.get(0));
        assertEquals(0, allocator.usedMemory());
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A slot is purged by the first pass that finds it free for the delay at least. A pass runs at most once per
     * check interval, however often it is asked.
     */
    @Test
    void onlySlotsFreeForTheDelay() {
        List<Segment> taken = takeAll(3);
        giveBack(taken.get(0));
        long freed0 = System.nanoTime();
        store.purgeIfDue(lastPurgeAt + CHECK / 2);
        assertEquals(0, store.purges, "not due: less than a check interval since the store began");
        purgeAt(freed0 + DELAY - CHECK);
        assertEquals(1, store.purges);
        assertEquals(0, regions.purgeCalls(), "slot 0 is free for less than the delay");
        giveBack(taken.get(1));
        long freed1 = System.nanoTime();
        store.purgeIfDue(lastPurgeAt + CHECK / 2);
        assertEquals(1, store.purges, "at most one pass per check interval");
        purgeAt(freed0 + DELAY);
        assertEquals(2, store.purges);
        assertEquals(1, regions.purgeCalls(), "slot 0 only: slot 1 is free for less than the delay");
        assertArrayEquals(new int[] {0, SEGMENT_SIZE}, regions.purges.get(0));
        purgeAt(freed1 + DELAY);
        assertEquals(2, regions.purgeCalls());
        assertArrayEquals(new int[] {SEGMENT_SIZE, SEGMENT_SIZE}, regions.purges.get(1));
        assertEquals((long) SEGMENT_SIZE, allocator.usedMemory(), "slot 2 is still in the heap");
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A purge call that fails leaves its slots committed, free and purgeable: nothing is thrown to the caller, which
     * may be an allocation, and the next pass tries them again.
     */
    @Test
    void aFailedPurgeCallIsSwallowedAndRetried() {
        List<Segment> taken = takeAll(3);
        giveBack(taken.get(1));
        long freed = System.nanoTime();
        final AtomicBoolean fail = new AtomicBoolean(true);
        regions.onPurge = () -> {
            if (fail.get()) {
                throw new IllegalStateException("madvise(MADV_DONTNEED) failed: errno 22");
            }
        };
        purgeAt(freed + DELAY);
        assertEquals(1, store.purgeFailures);
        assertEquals(0, store.segmentsPurged);
        assertEquals(3L * SEGMENT_SIZE, allocator.usedMemory(), "still committed");
        assertArrayEquals(new int[] {2, 1, SLOTS - 3}, store.slotCounts());
        purgeAt(0);
        assertEquals(2, store.purgeFailures);
        fail.set(false);
        purgeAt(0);
        assertEquals(1, store.segmentsPurged);
        assertEquals(2L * SEGMENT_SIZE, allocator.usedMemory());
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A heap's decay drives the purge too: the reserved segment goes back to its region, stamped with the clock, and
     * a pass in the same decay purges it once the decay's time is a delay past that.
     */
    @Test
    void heapDecaysDriveThePurge() {
        Segment segment = heap.claim(63);
        heap.release(segment, 0, 63);
        assertSame(segment, heap.reserve[0]);
        long now = System.nanoTime();
        heap.decay(now + CHECK); // the reserved one is seen; a pass runs, with nothing to purge
        assertEquals(1, store.purges);
        assertEquals(0, regions.purgeCalls());
        heap.decay(now + 2 * CHECK); // it goes back to its region, stamped now; a pass runs, too early for it
        long freed = System.nanoTime();
        assertEquals(0, heap.reserved);
        assertEquals(2, store.purges);
        assertEquals(0, regions.purgeCalls());
        heap.decay(freed + DELAY);
        assertEquals(1, regions.purgeCalls());
        assertEquals(3, store.purges);
        assertEquals(0, allocator.usedMemory());
    }

    /**
     * A heap's purge ticks purge the free slices of its region segments released a delay ago or earlier, one call
     * per run, once until they are claimed again; the slices in use keep their memory, and the segment stays in the
     * used memory.
     */
    @Test
    void heapPurgeTicksPurgeTheIdleSlicesOfTheirSegments() {
        int slice = PageStoreConfig.SLICE_SIZE_BYTES;
        Segment segment = heap.claim(8);
        assertSame(segment, heap.claim(8));
        assertEquals(8, heap.claimedStart());
        heap.claim(4); // 16 to 19
        int base = segment.slot * SEGMENT_SIZE;
        segment.buffer.setLong(0, 0x0123456789ABCDEFL);
        segment.buffer.setLong(8 * slice, 0x0123456789ABCDEFL);
        long before = System.nanoTime();
        heap.release(segment, 8, 8);
        long released = System.nanoTime();
        heap.purgeTick(before + DELAY - CHECK);
        assertEquals(0, store.slicePurgeCalls, "released less than a delay ago");
        heap.purgeTick(before + DELAY - CHECK / 2);
        assertEquals(0, store.slicePurgeCalls, "less than a check interval since the previous tick");
        heap.purgeTick(released + DELAY);
        assertEquals(1, store.slicePurgeCalls, "8 to 15; 20 to 63 were never claimed: no memory behind them");
        assertArrayEquals(new int[] {base + 8 * slice, 8 * slice}, regions.purges.get(regions.purgeCalls() - 1));
        assertEquals(8L * slice, store.slicePurgeBytes);
        assertEquals(0, segment.buffer.getLong(8 * slice), "purged memory reads zero");
        assertEquals(0x0123456789ABCDEFL, segment.buffer.getLong(0), "slices in use keep theirs");
        heap.purgeTick(released + DELAY + CHECK);
        assertEquals(1, store.slicePurgeCalls, "purged once");
        assertEquals((long) SEGMENT_SIZE, allocator.usedMemory(), "the heap still holds the segment");

        // Claimed and released again: a delay after the new release.
        assertSame(segment, heap.claim(8));
        assertEquals(8, heap.claimedStart());
        heap.release(segment, 8, 8);
        released = System.nanoTime();
        heap.purgeTick(released + DELAY + 2 * CHECK);
        assertEquals(2, store.slicePurgeCalls);
        heap.release(segment, 0, 8);
        heap.release(segment, 16, 4);
        assertAccounted(segments, regions, allocator);
    }

    /**
     * With a delay of 50 ms, allocations alone, without any decay, purge the slices a chunk freed: the ticks of the
     * size classes read the clock for the heap's purge tick.
     */
    @Test
    void allocationTicksHonourTheDelay() throws Exception {
        long delay = TimeUnit.MILLISECONDS.toNanos(50);
        AdaptivePoolingAllocator allocator = new AdaptivePoolingAllocator(segments, true, segments, regions,
                new PageStoreConfig(SEGMENT_SIZE, PageStoreConfig.SLICE_SIZE_BYTES, delay, REGION_SIZE,
                        REGION_ALIGNMENT));
        PageStore store = allocator.pageStore;
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 6 * 32; i++) { // six 8-slice chunks of 16 KiB buffers
            bufs.add(allocator.allocate(16384, 16384));
        }
        for (ByteBuf buf : bufs) {
            buf.release(); // two of the chunks empty above the class's floor of four: their spans go back
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5); // half a decay interval
        while (store.slicePurgeCalls == 0 && System.nanoTime() < deadline) {
            allocator.allocate(1024, 1024).release();
        }
        assertTrue(store.slicePurgeCalls > 0, "purged within 5 s of allocations");
        assertEquals(0, store.purgeFailures + store.slicePurgeFailures);
    }

    /** A pass that finds another one running returns at once, without purging nor counting a pass. */
    @Test
    void onePurgerAtATime() throws Exception {
        List<Segment> taken = takeAll(3);
        giveBack(taken.get(0));
        giveBack(taken.get(2));
        final long freed = System.nanoTime();
        final CountDownLatch inPurge = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean blocked = new AtomicBoolean();
        regions.onPurge = () -> {
            if (blocked.compareAndSet(false, true)) {
                inPurge.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        Thread first = new Thread(() -> store.purgeIfDue(freed + DELAY));
        first.start();
        assertTrue(inPurge.await(10, TimeUnit.SECONDS));
        store.purgeIfDue(freed + 10 * DELAY);
        assertEquals(1, store.purges, "the second caller did not purge");
        release.countDown();
        first.join();
        assertEquals(2, regions.purgeCalls(), "slots 0 and 2, by the first caller");
        assertAccounted(segments, regions, allocator);
    }

    /**
     * A purged slot is taken again as any free slot, the lowest first: its memory reads zero, and it counts in the
     * used memory again from that take.
     */
    @Test
    void aPurgedSlotIsTakenAgainAndReadsZero() {
        List<Segment> taken = takeAll(2);
        Segment first = taken.get(0);
        for (int offset = 0; offset < SEGMENT_SIZE; offset += PageStoreConfig.PAGE_SIZE_BYTES) {
            first.buffer.setLong(offset, offset + 1L);
        }
        giveBack(first);
        long now = System.nanoTime();
        store.purgeIfDue(now += INTERVAL);
        store.purgeIfDue(now += INTERVAL);
        assertEquals(SEGMENT_SIZE, allocator.usedMemory());
        assertEquals(2, store.segmentsCommitted);
        Segment again = store.take(heap);
        assertSame(first, again, "slot 0, the lowest free one");
        assertEquals(3, store.segmentsCommitted, "taken with no memory behind it");
        assertEquals(2L * SEGMENT_SIZE, allocator.usedMemory());
        for (int offset = 0; offset < SEGMENT_SIZE; offset += PageStoreConfig.PAGE_SIZE_BYTES) {
            assertEquals(0, again.buffer.getLong(offset), "offset " + offset);
        }
        assertAccounted(segments, regions, allocator);
    }

    /** With mmap, a purge through the store gives the memory back: the process's resident memory drops by it. */
    @Test
    void purgedSegmentsLeaveTheResidentMemory() throws Exception {
        assumeTrue(regions.mmap != null, "purges are real only with mmap regions");
        List<Segment> taken = takeAll(SLOTS);
        for (Segment segment : taken) {
            for (int offset = 0; offset < SEGMENT_SIZE; offset += PageStoreConfig.PAGE_SIZE_BYTES) {
                segment.buffer.setLong(offset, offset + 1L);
            }
            giveBack(segment);
        }
        long freed = System.nanoTime();
        long touched = MmapRegionSourceTest.residentKiB();
        purgeAt(freed + DELAY);
        long purged = MmapRegionSourceTest.residentKiB();
        assertEquals(1, regions.purgeCalls());
        assertTrue(touched - purged >= REGION_SIZE / 1024 * 3 / 4,
                "resident memory went from " + touched + " KiB to " + purged + " KiB");
    }

    /**
     * Four threads take and give back segments while another forces purges: a segment is still held by one thread at
     * a time, slots purged under them are taken again, and the used memory is the committed slots at the end.
     */
    @Test
    void purgesRaceWithTakes() throws Exception {
        final int threads = 4;
        final ConcurrentHashMap<Segment, Integer> holders = new ConcurrentHashMap<Segment, Integer>();
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final AtomicBoolean done = new AtomicBoolean();
        List<Thread> workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            Thread worker = new Thread(() -> {
                try {
                    HeapSegments own = new HeapSegments(store, null, Thread.currentThread());
                    Random random = new Random(id);
                    Segment[] held = new Segment[2];
                    int count = 0;
                    for (int r = 0; r < 20_000; r++) {
                        if (count < held.length && random.nextBoolean()) {
                            Segment segment = store.take(own);
                            Integer previous = holders.putIfAbsent(segment, id);
                            if (previous != null) {
                                throw new AssertionError(segment + " taken by " + id + " is held by " + previous);
                            }
                            segment.buffer.setLong(0, id);
                            held[count++] = segment;
                        } else if (count > 0) {
                            int k = random.nextInt(count);
                            Segment segment = held[k];
                            held[k] = held[--count];
                            if (segment.buffer.getLong(0) != id) {
                                throw new AssertionError(segment + " was purged under " + id);
                            }
                            holders.remove(segment);
                            assertTrue(Segment.OWNER.compareAndSet(segment, own, null));
                            store.free(segment);
                        }
                    }
                    while (count > 0) {
                        Segment segment = held[--count];
                        holders.remove(segment);
                        assertTrue(Segment.OWNER.compareAndSet(segment, own, null));
                        store.free(segment);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        Thread purger = new Thread(() -> {
            long now = System.nanoTime();
            while (!done.get()) {
                store.purgeIfDue(now += INTERVAL);
            }
        });
        purger.start();
        for (Thread worker : workers) {
            worker.join();
        }
        done.set(true);
        purger.join();
        assertNull(failure.get());
        assertTrue(store.segmentsPurged > 0, "the purges ran under the takes");
        assertTrue(store.segmentsCommitted > SLOTS, "purged slots were taken again");
        for (Region region : store.regions) {
            assertEquals(region.allSlots, region.free);
        }
        assertAccounted(segments, regions, allocator);
    }
}
