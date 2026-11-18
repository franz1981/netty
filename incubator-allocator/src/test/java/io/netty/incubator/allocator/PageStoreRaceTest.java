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
import io.netty.incubator.allocator.PageStoreAllocators.Geometry;
import io.netty.util.concurrent.MockTicker;
import io.netty.util.concurrent.Ticker;
import io.netty.util.internal.PlatformDependent;
import jdk.jfr.consumer.RecordedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.incubator.allocator.BufferChecks.assertDisjoint;
import static io.netty.incubator.allocator.BufferChecks.overlaps;
import static io.netty.incubator.allocator.BufferChecks.releaseAll;
import static io.netty.incubator.allocator.BufferChecks.slotLength;
import static io.netty.incubator.allocator.BufferChecks.stamp;
import static io.netty.incubator.allocator.BufferChecks.verify;
import static io.netty.incubator.allocator.PageStoreAllocators.assumeThreadLocalHeaps;
import static io.netty.incubator.allocator.PageStoreAllocators.pooledBuffers;
import static io.netty.incubator.allocator.PageStoreAllocators.useUntilQuiet;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.PageStoreAllocators.wholeBlocks;
import static io.netty.incubator.allocator.TestThreads.runOnThread;
import static io.netty.incubator.allocator.PageStoreAllocators.directRegionsAreMapped;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The races of the purge and of the addition of a region, made to happen once each, at the same point every time: a
 * region source of the test runs a callback inside the purge call and inside the mapping of a region, and the callback
 * starts the racing thread and joins it, so the two sides meet there on any number of cores. What each outcome is
 * asserted on is the buffers, the metrics and the JFR events.
 */
// A hang guard only. What the tests check is that an allocation or a release does not wait for a purge or for a region
// being added: if one did, it would wait for the thread that joins it, and this is how the test would end.
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@EnabledForJreRange(min = JRE.JAVA_11)
@Isolated("A recording takes the page store events of every thread, and on JDK 11 Flight Recorder loses a recording's "
        + "data when other tests allocate meanwhile")
class PageStoreRaceTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    @TempDir
    Path dir;

    private final AtomicReference<HookedRegionSource> source = new AtomicReference<HookedRegionSource>();

    @AfterEach
    void noHookFailed() {
        HookedRegionSource hooked = source.get();
        if (hooked != null) {
            hooked.assertNoHookFailure();
        }
    }

    private AdaptiveByteBufAllocator allocator(Geometry geometry) {
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        return newAllocator(closer, ticker, geometry, a -> {
            source.set(new HookedRegionSource(a));
            return source.get();
        });
    }

    private static void assumeJfr() {
        assumeTrue(PlatformDependent.isJfrEnabled(), "JFR is not available");
    }

    /** What a hook does when it is run: once. */
    private static Runnable once(final Runnable action) {
        final AtomicBoolean done = new AtomicBoolean();
        return new Runnable() {
            @Override
            public void run() {
                if (done.compareAndSet(false, true)) {
                    action.run();
                }
            }
        };
    }

    private static void unchecked(TestThreads.Body body, JfrEvents jfr, boolean fastThreadLocal) {
        try {
            jfr.runOnThread(fastThreadLocal, body);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A buffer of the same size allocated while a released one is being purged takes other memory, keeps its bytes
     * when the release is over, and is the only memory in use then.
     */
    @Test
    void anAllocationRacingTheReleaseOfALargeBufferKeepsItsBytes() throws Exception {
        final AdaptiveByteBufAllocator alloc = allocator(Geometry.TWO_MIB_BLOCKS);
        final int size = Geometry.TWO_MIB_BLOCKS.direct.regionSize;
        ByteBuf released = alloc.directBuffer(size);
        long address = released.memoryAddress();
        final AtomicReference<ByteBuf> racing = new AtomicReference<ByteBuf>();
        source.get().onPurge(once(() -> {
            try {
                TestThreads.runOnThread(false, () -> {
                    ByteBuf buf = alloc.directBuffer(size);
                    racing.set(buf);
                    stamp(buf, 7, 0, 4096);
                    buf.setLong(size - 8, 7);
                });
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }));
        released.release();
        source.get().onPurge(null);
        ByteBuf racer = racing.get();
        assertNotNull(racer, "the release purged nothing");
        verify(racer, 7, 4096);
        assertEquals(7, racer.getLong(size - 8));
        assertFalse(overlaps(address, size, racer.memoryAddress(), size), "the racing buffer is in the purged range");
        assertEquals(size, alloc.usedDirectMemory());
        // Released only once the assertions passed: after a failure its release would be accounted a second time and
        // hide the failure.
        racer.release();
    }

    /**
     * A release whose purge fails does not throw; the memory stays counted, the same buffer again is not charged a
     * second time and keeps its bytes, and the Purge event of the release says it failed.
     */
    @Test
    void aFailedPurgeAtReleaseIsNotThrownAndKeepsTheMemoryCounted() throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            AdaptiveByteBufAllocator alloc = allocator(Geometry.TWO_MIB_BLOCKS);
            int size = Geometry.TWO_MIB_BLOCKS.direct.regionSize;
            ByteBuf buf = alloc.directBuffer(size);
            long address = buf.memoryAddress();
            source.get().failNextPurge();
            buf.release();
            assertEquals(size, alloc.usedDirectMemory(), "memory that was not given back is still in use");
            ByteBuf again = alloc.directBuffer(size);
            try {
                assertEquals(size, alloc.usedDirectMemory(), "charged twice");
                stamp(again, 3, 0, 4096);
                verify(again, 3, 4096);
            } finally {
                again.release();
            }
            assertEquals(0, alloc.usedDirectMemory());
            jfr.stop();
            List<RecordedEvent> purges = jfr.events(PageStorePurgeEvent.NAME);
            assertEquals(2, purges.size());
            assertTrue(purges.get(0).getBoolean("failed"));
            assertEquals(address, purges.get(0).getLong("address"));
            assertEquals(size, purges.get(0).getLong("length"));
            assertFalse(purges.get(1).getBoolean("failed"));
        }
    }

    /**
     * An allocation of a thread with a heap of its own, made while the purger is inside a purge call, neither waits for
     * the purge nor takes a range it purges: the buffers are outside every range purged in the pass, keep their bytes
     * when the pass is over, and cost at most one region.
     */
    @Test
    void anAllocationRacingAPurgeNeitherWaitsNorTakesThePurgedRange() throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(Geometry.DEFAULTS);
            final int block = Geometry.DEFAULTS.direct.segmentSize;
            final List<ByteBuf> blocks = new ArrayList<ByteBuf>();
            jfr.runOnThread(false, () -> blocks.addAll(wholeBlocks(alloc, block, 4)));
            ByteBuf kept = blocks.remove(0);
            releaseAll(blocks);

            final List<ByteBuf> racing = new ArrayList<ByteBuf>();
            final List<Thread> racers = new ArrayList<Thread>();
            source.get().onPurge(once(() -> unchecked(() -> {
                for (int i = 0; i < 200; i++) {
                    final ByteBuf buf = alloc.directBuffer(1024);
                    stamp(buf, 100 + i);
                    racing.add(buf);
                }
                final ByteBuf big = alloc.directBuffer(block);
                stamp(big, 1000);
                racing.add(big);
            }, jfr, true)));
            jfr.runOnThread(false, () -> useUntilQuiet(alloc, ticker, pooledBuffers(alloc, true)));
            source.get().onPurge(null);
            try {
                for (int i = 0; i < 200; i++) {
                    verify(racing.get(i), 100 + i);
                }
                verify(racing.get(200), 1000);
                jfr.stop();

                List<RecordedEvent> purges = jfr.events(PageStorePurgeEvent.NAME);
                assertFalse(purges.isEmpty());
                for (RecordedEvent purge : purges) {
                    for (ByteBuf buf : racing) {
                        assertFalse(overlaps(purge.getLong("address"), purge.getLong("length"), buf.memoryAddress(),
                                slotLength(buf)), "a purge covers a buffer allocated meanwhile");
                    }
                    assertFalse(overlaps(purge.getLong("address"), purge.getLong("length"), kept.memoryAddress(),
                            slotLength(kept)));
                }
                assertDisjoint(concat(racing, kept));
                assertTrue(jfr.events(PageStoreMapEvent.NAME).size() <= 2, "at most a region for the racing thread");
            } finally {
                releaseAll(racing);
                kept.release();
            }
        }
    }

    private static List<ByteBuf> concat(List<ByteBuf> buffers, ByteBuf more) {
        List<ByteBuf> all = new ArrayList<ByteBuf>(buffers);
        all.add(more);
        return all;
    }

    /**
     * A buffer released while a purge pass is running, in a block the pass already went by, is purged later: the
     * clock moves and a thread allocates until the allocator gives nothing more back, and the Purge events cover the
     * block, once, none failed. Which call purges it is the allocator's policy.
     */
    @Test
    void aReleaseDuringAPurgePassIsPurgedLater() throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(Geometry.DEFAULTS);
            final int block = Geometry.DEFAULTS.direct.segmentSize;
            final List<ByteBuf> blocks = new ArrayList<ByteBuf>();
            jfr.runOnThread(false, () -> blocks.addAll(wholeBlocks(alloc, block, 4)));
            // The first block is held while the first pass goes by, and released from inside its first purge call.
            final ByteBuf held = blocks.remove(0);
            final long address = held.memoryAddress();
            releaseAll(blocks);
            source.get().onPurge(once(held::release));
            jfr.runOnThread(false, () -> useUntilQuiet(alloc, ticker, pooledBuffers(alloc, true)));
            source.get().onPurge(null);
            jfr.stop();

            long covered = 0;
            for (RecordedEvent purge : jfr.events(PageStorePurgeEvent.NAME)) {
                assertFalse(purge.getBoolean("failed"));
                if (overlaps(purge.getLong("address"), purge.getLong("length"), address, block)) {
                    covered += Math.min(purge.getLong("address") + purge.getLong("length"), address + block)
                            - Math.max(purge.getLong("address"), address);
                }
            }
            assertEquals(block, covered, "the buffer released during a pass was not purged, once, by the next ones");
        }
    }

    /**
     * An allocation that does not need a region goes on while another thread is adding one, holding the store's lock:
     * the buffers do not overlap, keep their bytes, the racing thread maps nothing, and the regions are the two that
     * were added.
     */
    @Test
    void allocationsProceedWhileARegionIsBeingAdded() throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(Geometry.SMALL_REGIONS);
            final int block = Geometry.SMALL_REGIONS.direct.segmentSize;
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            final List<ByteBuf> meanwhile = new ArrayList<ByteBuf>();
            final List<Long> seeds = new ArrayList<Long>();
            // The first region holds two blocks: one is taken, one is free for the thread that races.
            jfr.runOnThread(false, () -> {
                final ByteBuf first = alloc.directBuffer(block);
                stamp(first, 1);
                live.add(first);
            });
            final AtomicReference<Thread> racer = new AtomicReference<Thread>();
            source.get().onMap(once(() -> {
                try {
                    racer.set(jfr.runOnThread(false, () -> {
                        for (int i = 0; i < 50; i++) {
                            final ByteBuf buf = alloc.directBuffer(1024);
                            stamp(buf, 10 + i);
                            meanwhile.add(buf);
                            seeds.add(10L + i);
                        }
                        for (int i = 0; i < 50; i += 2) {
                            verify(meanwhile.get(i), seeds.get(i));
                            meanwhile.get(i).release();
                        }
                    }));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }));
            // A run of two blocks does not fit the one that is free: a second region.
            jfr.runOnThread(false, () -> {
                final ByteBuf run = alloc.directBuffer(2 * block);
                stamp(run, 2);
                live.add(run);
            });
            source.get().onMap(null);
            List<ByteBuf> kept = new ArrayList<ByteBuf>();
            try {
                for (int i = 1; i < 50; i += 2) {
                    kept.add(meanwhile.get(i));
                }
                live.addAll(kept);
                assertDisjoint(live);
                verify(live.get(0), 1);
                verify(live.get(1), 2);
                for (int i = 0; i < kept.size(); i++) {
                    verify(kept.get(i), seeds.get(2 * i + 1));
                }
                jfr.stop();
                assertEquals(0, jfr.eventsOf(PageStoreMapEvent.NAME, racer.get()).size());
                assertEquals(2, jfr.events(PageStoreMapEvent.NAME).size(), "the regions mapped");
            } finally {
                releaseAll(live);
            }
        }
    }

    /**
     * Two threads with heaps of their own find no room in the only region: the first maps a second one, the second,
     * which waits for it, uses that region and maps none. One region is mapped, and the buffers keep their bytes and
     * share no memory.
     */
    @Test
    void aThreadThatWaitedForARegionUsesTheOneMapped() throws Exception {
        assumeJfr();
        assumeThreadLocalHeaps();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(Geometry.SMALL_REGIONS);
            final int block = Geometry.SMALL_REGIONS.direct.segmentSize;
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            // The first region holds two blocks, and both are taken.
            jfr.runOnThread(true, () -> {
                for (int i = 0; i < 2; i++) {
                    final ByteBuf buf = alloc.directBuffer(block);
                    stamp(buf, 1 + i);
                    live.add(buf);
                }
            });
            final AtomicReference<TestThreads.Started> second = new AtomicReference<TestThreads.Started>();
            final AtomicReference<ByteBuf> other = new AtomicReference<ByteBuf>();
            source.get().onMap(once(() -> {
                // This thread holds the store's lock, inside the mapping of the second region. The other thread finds
                // no room too, and parks on the lock until this one is done.
                second.set(jfr.start(true, () -> {
                    final ByteBuf buf = alloc.directBuffer(block);
                    stamp(buf, 4);
                    other.set(buf);
                }));
                final Thread thread = second.get().thread();
                // The one wait of this test, on a state and not on time: the thread is parked by the store, on its
                // lock, or is over. Parked elsewhere (the executor's queue, say) is not that.
                while (!parkedByTheStore(thread) && thread.getState() != Thread.State.TERMINATED) {
                    Thread.yield();
                }
            }));
            Thread first = jfr.runOnThread(true, () -> {
                final ByteBuf buf = alloc.directBuffer(block);
                stamp(buf, 3);
                live.add(buf);
            });
            source.get().onMap(null);
            second.get().join();
            live.add(other.get());
            try {
                jfr.stop();
                assertEquals(1, jfr.eventsOf(PageStoreMapEvent.NAME, first).size(), "the region the first mapped");
                assertEquals(0, jfr.eventsOf(PageStoreMapEvent.NAME, second.get().thread()).size(),
                        "the second thread mapped a region of its own");
                assertEquals(2, jfr.events(PageStoreMapEvent.NAME).size(), "the regions mapped");
                assertEquals(4L * block, alloc.usedDirectMemory());
                assertDisjoint(live);
                verify(live.get(0), 1);
                verify(live.get(1), 2);
                verify(live.get(2), 3);
                verify(live.get(3), 4);
            } finally {
                releaseAll(live);
            }
        }
    }

    /**
     * A release that finds its stripe busy, because the owner is adding a region, leaves a note and does not wait: when
     * the owner allocates again it reuses what was released, over the buffers it kept, which keep their bytes, and a
     * full round once everything is released needs no more memory than the first did.
     */
    @Test
    void aReleaseToABusyStripeIsAppliedLater() throws Exception {
        final AdaptiveByteBufAllocator alloc = allocator(Geometry.SMALL_REGIONS);
        final PageStoreConfig config = Geometry.SMALL_REGIONS.direct;
        SizeClassTable table = new SizeClassTable(config);
        int index = SizeClassTable.sizeClassIndex(1024);
        // Two blocks fill the first region exactly, with chunks of one slice.
        final int count = 2 * table.slots[index] * config.slicesPerSegment();
        final List<ByteBuf> live = new ArrayList<ByteBuf>();
        final List<Long> seeds = new ArrayList<Long>();
        final long[] used = new long[1];
        final AtomicReference<ByteBuf> extra = new AtomicReference<ByteBuf>();
        final Runnable releasing = once(() -> {
            // The owner is inside the mapping of a second region, holding its stripe: the releases find it busy.
            // The invariant this test stands on: while a region is mapped for a stripe's allocation, the stripe's
            // lock is held, so every tryWriteLock of a release fails and the release leaves a note. If a later
            // change maps outside the lock, this fails here and the test does not go on testing nothing.
            for (int i = 0; i < count; i += 2) {
                assertTrue(stripeOf(live.get(i)).lock.isWriteLocked(),
                        "the owner's stripe is not held while its region is mapped");
            }
            try {
                runOnThread(false, () -> {
                    for (int i = 0; i < count; i += 2) {
                        verify(live.get(i), seeds.get(i));
                        live.get(i).release();
                    }
                });
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        runOnThread(false, () -> {
            for (int i = 0; i < count; i++) {
                final ByteBuf buf = alloc.directBuffer(1024);
                stamp(buf, i + 1);
                live.add(buf);
                seeds.add(i + 1L);
                if (i == 0) {
                    // The first region is there: the next one's mapping is where the others meet the owner.
                    source.get().onMap(releasing);
                }
            }
            assertEquals(2L * config.segmentSize, alloc.usedDirectMemory(), "the first region is full");
            final ByteBuf more = alloc.directBuffer(1024);
            extra.set(more);
            used[0] = alloc.usedDirectMemory();
        });
        source.get().onMap(null);
        List<ByteBuf> kept = new ArrayList<ByteBuf>();
        for (int i = 1; i < count; i += 2) {
            kept.add(live.get(i));
        }
        live.clear();
        final List<ByteBuf> again = new ArrayList<ByteBuf>();
        try {
            runOnThread(false, () -> {
                for (int i = 0; i < count / 2; i++) {
                    final ByteBuf buf = alloc.directBuffer(1024);
                    stamp(buf, 100_000 + i);
                    again.add(buf);
                }
            });
            assertEquals(used[0], alloc.usedDirectMemory(), "the released slots were not reused");
            List<ByteBuf> all = new ArrayList<ByteBuf>(kept);
            all.addAll(again);
            all.add(extra.get());
            assertDisjoint(all);
            for (int i = 0; i < kept.size(); i++) {
                verify(kept.get(i), seeds.get(2 * i + 1));
            }
        } finally {
            releaseAll(kept);
            releaseAll(again);
            extra.get().release();
        }
        final List<ByteBuf> round = new ArrayList<ByteBuf>();
        try {
            runOnThread(false, () -> {
                for (int i = 0; i < count; i++) {
                    round.add(alloc.directBuffer(1024));
                }
            });
            assertEquals(used[0], alloc.usedDirectMemory(), "a full round took memory");
        } finally {
            releaseAll(round);
        }
    }

    /**
     * The heap that owns the chunk of {@code buf}, a size class's buffer. A precondition of the test above only, never
     * what it asserts on: see the invariant it checks.
     */
    private static AdaptivePoolingAllocator.Heap stripeOf(ByteBuf buf) {
        while (!(buf instanceof AdaptiveByteBuf)) {
            buf = buf.unwrap();
        }
        return ((AdaptivePoolingAllocator.SizeClassedChunk) ((AdaptiveByteBuf) buf).chunk).magazine.heap;
    }

    /** Whether {@code thread} waits, and the first frame of its stack that is not the JDK's is the store's. */
    private static boolean parkedByTheStore(Thread thread) {
        if (thread.getState() != Thread.State.WAITING) {
            return false;
        }
        for (StackTraceElement frame : thread.getStackTrace()) {
            String name = frame.getClassName();
            if (!name.startsWith("java.") && !name.startsWith("jdk.") && !name.startsWith("sun.")) {
                return name.equals(PageStore.class.getName());
            }
        }
        return false;
    }
}
