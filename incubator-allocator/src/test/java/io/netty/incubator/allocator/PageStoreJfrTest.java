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
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.incubator.allocator.BufferChecks.overlaps;
import static io.netty.incubator.allocator.BufferChecks.releaseAll;
import static io.netty.incubator.allocator.BufferChecks.slotLength;
import static io.netty.incubator.allocator.PageStoreAllocators.assumeThreadLocalHeaps;
import static io.netty.incubator.allocator.PageStoreAllocators.aboveABlock;
import static io.netty.incubator.allocator.PageStoreAllocators.pooledBuffers;
import static io.netty.incubator.allocator.PageStoreAllocators.useUntilQuiet;
import static io.netty.incubator.allocator.PageStoreAllocators.mmapWhereLinked;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.PageStoreAllocators.wholeBlocks;
import static io.netty.incubator.allocator.PageStoreAllocators.directRegionsAreMapped;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What the page store reports about its memory, read back from a stopped recording: the regions it maps and unmaps,
 * the free slices it purges, where its memory is, the chunks and the buffers. Each test records the events of its
 * own threads, and the allocator is built after the recording starts. Purges and decays happen when a test moves the
 * clock and a thread allocates, at a time it picks. Whether mapped regions are used is the execution's
 * ({@code -Dio.netty.allocator.pagestore.mmap}); the tests that need them are skipped where they are not.
 */
@EnabledForJreRange(min = JRE.JAVA_11)
@Isolated("A recording takes the page store events of every thread, and on JDK 11 Flight Recorder loses a recording's "
        + "data when other tests allocate meanwhile")
class PageStoreJfrTest {
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

    private static void assumeJfr() {
        assumeTrue(PlatformDependent.isJfrEnabled(), "JFR is not available");
    }

    private AdaptiveByteBufAllocator allocator(boolean mapped, Geometry geometry) {
        if (mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
            return newAllocator(closer, ticker, geometry, mmapWhereLinked());
        }
        return newAllocator(closer, ticker, geometry, null);
    }

    private static boolean inside(long address, RecordedEvent range) {
        return address >= range.getLong("address") && address < range.getLong("address") + range.getLong("length");
    }

    /**
     * One Map event per region added, whole-block for {@code malloc}'d regions and region-sized for mapped ones, each
     * buffer inside one of them, and none for a second round that the freed memory serves.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void everyRegionIsAnnouncedOnceAndHoldsItsBuffers(boolean mapped) throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(mapped, Geometry.SMALL_REGIONS);
            PageStoreConfig config = Geometry.SMALL_REGIONS.direct;
            final int block = config.segmentSize;
            int regionBlocks = mapped ? config.regionSize / block : 1;
            final int buffers = 3;
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            final List<Long> addresses = new ArrayList<Long>();
            Thread first = jfr.runOnThread(false, () -> {
                live.addAll(wholeBlocks(alloc, block, buffers));
                for (ByteBuf buf : live) {
                    addresses.add(buf.memoryAddress());
                }
            });
            releaseAll(live);
            Thread second = jfr.runOnThread(false, () -> {
                live.addAll(wholeBlocks(alloc, block, buffers));
            });
            releaseAll(live);
            jfr.stop();

            List<RecordedEvent> maps = jfr.eventsOf(PageStoreMapEvent.NAME, first);
            assertEquals((buffers + regionBlocks - 1) / regionBlocks, maps.size());
            Set<Integer> regions = new HashSet<Integer>();
            for (RecordedEvent map : maps) {
                assertFalse(map.getBoolean("failed"));
                assertEquals((long) regionBlocks * block, map.getLong("length"));
                if (mapped) {
                    assertEquals(0, map.getLong("address") % config.regionAlignment);
                }
                regions.add(map.getInt("region"));
            }
            assertEquals(maps.size(), regions.size(), "every region has its own index");
            for (long address : addresses) {
                int holding = 0;
                for (RecordedEvent map : maps) {
                    holding += inside(address, map) ? 1 : 0;
                }
                assertEquals(1, holding, "a buffer is in exactly one region");
            }
            assertEquals(0, jfr.eventsOf(PageStoreMapEvent.NAME, second).size(), "the freed memory serves the round");
        }
    }

    /** With nothing live, the close unmaps what was mapped: one Unmap per Map, of the same range. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void theCloseUnmapsEveryRegion(boolean mapped) throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(mapped, Geometry.SMALL_REGIONS);
            final int block = Geometry.SMALL_REGIONS.direct.segmentSize;
            jfr.runOnThread(false, () -> releaseAll(wholeBlocks(alloc, block, 3)));
            // The closer closes it again: closing twice is harmless.
            alloc.close();
            assertEquals(0, alloc.usedDirectMemory());
            assertEquals(0, alloc.usedHeapMemory());
            jfr.stop();

            List<RecordedEvent> maps = jfr.events(PageStoreMapEvent.NAME);
            List<RecordedEvent> unmaps = jfr.events(PageStoreUnmapEvent.NAME);
            assertEquals(maps.size(), unmaps.size());
            assertEquals(ranges(maps), ranges(unmaps));
            for (RecordedEvent unmap : unmaps) {
                assertFalse(unmap.getBoolean("failed"));
            }
        }
    }

    private static Set<List<Long>> ranges(List<RecordedEvent> events) {
        Set<List<Long>> ranges = new HashSet<List<Long>>();
        for (RecordedEvent event : events) {
            ranges.add(Arrays.asList(event.getLong("address"), event.getLong("length"), (long) event.getInt("region")));
        }
        return ranges;
    }

    /**
     * Regions of one block that stayed idle are unmapped, never the one holding a buffer, and the bytes the Unmap
     * events report are what the allocator no longer uses. Holding more whole blocks than were ever mapped then maps
     * again, and every event of a region, the Map that made it and the Unmap that ended it, carries one region index,
     * which no region still mapped has.
     */
    @Test
    void anIdleMallocRegionIsUnmappedAndEveryEventOfARegionCarriesItsIndex() throws Exception {
        assumeJfr();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(false, Geometry.SMALL_REGIONS);
            final int block = Geometry.SMALL_REGIONS.direct.segmentSize;
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            Thread first = jfr.runOnThread(false, () -> live.addAll(wholeBlocks(alloc, block, 3)));
            long heldAddress = live.get(1).memoryAddress();
            live.get(0).release();
            live.get(2).release();
            ByteBuf held = live.get(1);
            live.clear();
            live.add(held);
            long before = alloc.usedDirectMemory();
            Thread purger = jfr.runOnThread(false, () -> useUntilQuiet(alloc, ticker, aboveABlock(alloc, true, block)));
            long dropped = before - alloc.usedDirectMemory();
            // More whole blocks than were ever mapped: some must be mapped again.
            Thread later = jfr.runOnThread(false, () -> {
                final List<ByteBuf> more = wholeBlocks(alloc, block, 4);
                live.addAll(more);
            });
            releaseAll(live);
            alloc.close();
            jfr.stop();

            List<RecordedEvent> mapped = jfr.eventsOf(PageStoreMapEvent.NAME, first);
            List<RecordedEvent> unmapped = jfr.eventsOf(PageStoreUnmapEvent.NAME, purger);
            assertFalse(unmapped.isEmpty(), "nothing was unmapped");
            long unmappedBytes = 0;
            for (RecordedEvent unmap : unmapped) {
                assertFalse(unmap.getBoolean("failed"));
                assertFalse(inside(heldAddress, unmap), "the region of a live buffer was unmapped");
                unmappedBytes += unmap.getLong("length");
                // The Unmap of a region of the first phase has the region index of its Map.
                RecordedEvent map = withAddress(mapped, unmap.getLong("address"));
                assertEquals(map.getInt("region"), unmap.getInt("region"));
            }
            assertEquals(dropped, unmappedBytes);

            List<RecordedEvent> remapped = jfr.eventsOf(PageStoreMapEvent.NAME, later);
            assertFalse(remapped.isEmpty(), "no region was mapped again");
            Set<Integer> stillMapped = new HashSet<Integer>();
            for (RecordedEvent map : mapped) {
                if (inside(heldAddress, map)) {
                    stillMapped.add(map.getInt("region"));
                }
            }
            for (RecordedEvent map : remapped) {
                assertTrue(stillMapped.add(map.getInt("region")), "a region took the index of one still mapped");
            }
            // The close, on this thread, unmaps what is mapped: each Unmap has the index of the last Map at its
            // address.
            List<RecordedEvent> maps = new ArrayList<RecordedEvent>(mapped);
            maps.addAll(remapped);
            for (RecordedEvent unmap : jfr.eventsOf(PageStoreUnmapEvent.NAME, Thread.currentThread())) {
                assertEquals(lastWithAddress(maps, unmap.getLong("address")).getInt("region"), unmap.getInt("region"));
            }
        }
    }

    private static RecordedEvent withAddress(List<RecordedEvent> events, long address) {
        for (RecordedEvent event : events) {
            if (event.getLong("address") == address) {
                return event;
            }
        }
        throw new AssertionError("no event at 0x" + Long.toHexString(address));
    }

    private static RecordedEvent lastWithAddress(List<RecordedEvent> events, long address) {
        RecordedEvent found = null;
        for (RecordedEvent event : events) {
            if (event.getLong("address") == address) {
                found = event;
            }
        }
        if (found == null) {
            throw new AssertionError("no event at 0x" + Long.toHexString(address));
        }
        return found;
    }

    /**
     * A heap that died with a buffer out leaves its chunk to the store: its region is not unmapped while the buffer
     * lives, and once it is released the region is unmapped, once, by a purge, and the close does not unmap it again.
     */
    @Test
    void aRegionOfADeadHeapIsNotUnmappedWhileItsBufferLives() throws Exception {
        assumeJfr();
        assumeThreadLocalHeaps();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(false, Geometry.SMALL_REGIONS);
            final int block = Geometry.SMALL_REGIONS.direct.segmentSize;
            final AtomicReference<ByteBuf> held = new AtomicReference<ByteBuf>();
            Thread owner = jfr.runOnThread(true, () -> held.set(alloc.directBuffer(1024)));
            Thread whileHeld = jfr.runOnThread(false,
                    () -> useUntilQuiet(alloc, ticker, aboveABlock(alloc, true, block)));
            held.get().release();
            Thread afterRelease = jfr.runOnThread(false,
                    () -> useUntilQuiet(alloc, ticker, aboveABlock(alloc, true, block)));
            alloc.close();
            jfr.stop();

            List<RecordedEvent> allocated = jfr.eventsOf(AllocateBufferEvent.NAME, owner);
            assertEquals(1, allocated.size());
            assertTrue(allocated.get(0).getBoolean("chunkThreadLocal"), "the buffer was of a thread-local heap");
            List<RecordedEvent> maps = jfr.events(PageStoreMapEvent.NAME);
            assertEquals(1, maps.size());
            assertEquals(0, jfr.eventsOf(PageStoreUnmapEvent.NAME, whileHeld).size(), "unmapped with a buffer out");
            int unmaps = 0;
            for (RecordedEvent unmap : jfr.events(PageStoreUnmapEvent.NAME)) {
                if (unmap.getLong("address") == maps.get(0).getLong("address")) {
                    unmaps++;
                    assertEquals(maps.get(0).getInt("region"), unmap.getInt("region"));
                }
            }
            assertEquals(1, unmaps);
            int byPurge = 0;
            for (RecordedEvent unmap : jfr.eventsOf(PageStoreUnmapEvent.NAME, afterRelease)) {
                if (unmap.getLong("address") == maps.get(0).getLong("address")) {
                    byPurge++;
                }
            }
            assertEquals(1, byPurge, "the region was not unmapped by a purge");
        }
    }

    /**
     * What the allocator gives back is free memory only: every purge is inside a mapped region and never over a live
     * buffer, the bytes of the Purge events are what the allocator no longer uses, something was purged, and taking
     * the memory again maps nothing. How many calls it takes and how much each gives back is the allocator's policy.
     */
    @ParameterizedTest(name = "{0} blocks")
    @ValueSource(ints = {6, 20})
    void idleFreeSlicesArePurgedNeverALiveBuffers(int count) throws Exception {
        assumeJfr();
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(true, Geometry.DEFAULTS);
            final int block = Geometry.DEFAULTS.direct.segmentSize;
            final List<ByteBuf> blocks = new ArrayList<ByteBuf>();
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            jfr.runOnThread(false, () -> {
                blocks.addAll(wholeBlocks(alloc, block, count));
                live.add(blocks.remove(0));
                live.add(alloc.directBuffer(1024));
                live.add(alloc.directBuffer(100_000));
            });
            releaseAll(blocks);
            long[][] liveRanges = new long[live.size()][];
            for (int i = 0; i < liveRanges.length; i++) {
                liveRanges[i] = new long[] {live.get(i).memoryAddress(), slotLength(live.get(i))};
            }
            long before = alloc.usedDirectMemory();
            jfr.runOnThread(false, () -> useUntilQuiet(alloc, ticker, pooledBuffers(alloc, true)));
            long after = alloc.usedDirectMemory();
            Thread again = jfr.runOnThread(false, () -> blocks.addAll(wholeBlocks(alloc, block, count - 1)));
            assertEquals(before, alloc.usedDirectMemory());
            releaseAll(blocks);
            releaseAll(live);
            jfr.stop();

            long purged = 0;
            List<RecordedEvent> maps = jfr.events(PageStoreMapEvent.NAME);
            for (RecordedEvent purge : jfr.events(PageStorePurgeEvent.NAME)) {
                assertFalse(purge.getBoolean("failed"));
                purged += purge.getLong("length");
                boolean mapped = false;
                for (RecordedEvent map : maps) {
                    mapped |= inside(purge.getLong("address"), map);
                }
                assertTrue(mapped, "a purge is inside a mapped region");
                for (long[] range : liveRanges) {
                    assertFalse(overlaps(purge.getLong("address"), purge.getLong("length"), range[0], range[1]),
                            "a purge never covers a live buffer");
                }
            }
            assertEquals(before - after, purged);
            assertTrue(purged > 0, "nothing was purged");
            assertEquals(0, jfr.eventsOf(PageStoreMapEvent.NAME, again).size());
        }
    }

    /** A region that cannot be mapped is reported failed, and the store goes on with regions of one block. */
    @Test
    void aRegionThatCannotBeMappedIsReportedAndTheStoreFallsBack() throws Exception {
        assumeJfr();
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.SMALL_REGIONS, a -> {
                source.set(new HookedRegionSource(a));
                return source.get();
            });
            PageStoreConfig config = Geometry.SMALL_REGIONS.direct;
            final int block = config.segmentSize;
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            jfr.runOnThread(false, () -> {
                source.get().failNextMap();
                live.addAll(wholeBlocks(alloc, block, 3));
                for (int i = 0; i < live.size(); i++) {
                    live.get(i).writeLong(i + 1).setLong(block - 8, -i - 1);
                }
            });
            for (int i = 0; i < live.size(); i++) {
                assertEquals(i + 1, live.get(i).getLong(0));
                assertEquals(-i - 1, live.get(i).getLong(block - 8));
            }
            releaseAll(live);
            jfr.stop();

            int failed = 0;
            int blockSized = 0;
            for (RecordedEvent map : jfr.events(PageStoreMapEvent.NAME)) {
                if (map.getBoolean("failed")) {
                    failed++;
                    assertEquals(config.regionSize, map.getLong("length"));
                } else {
                    blockSized++;
                    assertEquals(block, map.getLong("length"));
                }
            }
            assertEquals(1, failed);
            assertEquals(3, blockSized);
        }
    }

    /** A mapping that fails other than by mmap(2) refusing is reported failed and rethrown: no fallback. */
    @Test
    void anErrorWhileMappingIsReportedAndRethrownWithoutFallingBack() throws Exception {
        assumeJfr();
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.SMALL_REGIONS, a -> {
                source.set(new HookedRegionSource(a));
                return source.get();
            });
            PageStoreConfig config = Geometry.SMALL_REGIONS.direct;
            final int block = config.segmentSize;
            final StackOverflowError error = new StackOverflowError("on purpose");
            final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            jfr.runOnThread(false, () -> {
                source.get().failNextMap(error);
                try {
                    live.addAll(wholeBlocks(alloc, block, 1));
                } catch (Throwable t) {
                    thrown.set(t);
                }
                live.addAll(wholeBlocks(alloc, block, 1));
            });
            releaseAll(live);
            jfr.stop();

            assertSame(error, thrown.get());
            List<RecordedEvent> maps = jfr.events(PageStoreMapEvent.NAME);
            assertEquals(2, maps.size());
            assertTrue(maps.get(0).getBoolean("failed"));
            assertFalse(maps.get(1).getBoolean("failed"));
            assertEquals(config.regionSize, maps.get(1).getLong("length"));
        }
    }

    /**
     * A region whose unmap fails at the close does not keep the others mapped, and the failure is reported and
     * rethrown.
     */
    @Test
    void closingUnmapsEveryRegionWhenOneUnmapFails() throws Exception {
        assumeJfr();
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.SMALL_REGIONS, a -> {
                source.set(new HookedRegionSource(a));
                return source.get();
            });
            final int block = Geometry.SMALL_REGIONS.direct.segmentSize;
            final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
            jfr.runOnThread(false, () -> releaseAll(wholeBlocks(alloc, block, 4)));
            jfr.runOnThread(false, () -> {
                source.get().failNextUnmap();
                try {
                    alloc.close();
                } catch (Throwable t) {
                    thrown.set(t);
                }
            });
            jfr.stop();

            List<RecordedEvent> maps = jfr.events(PageStoreMapEvent.NAME);
            assertEquals(2, maps.size());
            for (RecordedEvent map : maps) {
                assertFalse(map.getBoolean("failed"));
            }
            assertTrue(thrown.get() instanceof NativeCallException, String.valueOf(thrown.get()));
            assertEquals(0, thrown.get().getSuppressed().length);
            List<RecordedEvent> unmaps = jfr.events(PageStoreUnmapEvent.NAME);
            assertEquals(2, unmaps.size());
            int failed = 0;
            for (RecordedEvent unmap : unmaps) {
                failed += unmap.getBoolean("failed") ? 1 : 0;
            }
            assertEquals(1, failed);
            Set<Long> mapped = new HashSet<Long>();
            for (RecordedEvent map : maps) {
                mapped.add(map.getLong("address"));
            }
            Set<Long> unmapped = new HashSet<Long>();
            for (RecordedEvent unmap : unmaps) {
                unmapped.add(unmap.getLong("address"));
            }
            assertEquals(mapped, unmapped);
            assertEquals(0, alloc.usedDirectMemory());
        }
    }

    /**
     * What the periodic event says at the stop of a recording: every region of the allocator, with its bytes in the
     * four states adding up to its length, the claimed and the free-with-memory ones to the used memory, and each live
     * buffer in a claimed range.
     */
    @Test
    void theStateEventAddsUpToTheRegionsAndTheUsedMemory() throws Exception {
        assumeJfr();
        assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
        try (JfrEvents jfr = new JfrEvents(dir, true)) {
            final AdaptiveByteBufAllocator alloc = allocator(true, Geometry.DEFAULTS);
            final int block = Geometry.DEFAULTS.direct.segmentSize;
            final List<ByteBuf> blocks = new ArrayList<ByteBuf>();
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            final List<ByteBuf> heapBuffer = new ArrayList<ByteBuf>();
            jfr.runOnThread(false, () -> {
                blocks.addAll(wholeBlocks(alloc, block, 3));
                live.add(blocks.remove(2));
                live.add(alloc.directBuffer(1024));
                live.add(alloc.directBuffer(100_000));
                // A heap region has no address: its ranges are empty, its byte counts are not.
                heapBuffer.add(alloc.heapBuffer(1024));
            });
            blocks.get(0).release();
            jfr.runOnThread(false, () -> useUntilQuiet(alloc, ticker, pooledBuffers(alloc, true)));
            blocks.get(1).release();
            long used = alloc.usedDirectMemory();
            List<Long> addresses = new ArrayList<Long>();
            for (ByteBuf buf : live) {
                addresses.add(buf.memoryAddress());
            }
            jfr.stop();
            try {
                List<RecordedEvent> maps = jfr.events(PageStoreMapEvent.NAME);
                assertFalse(maps.isEmpty());
                // Any recording's start or stop in the JVM ends a chunk of this one too, and the periodic event is
                // taken at each of them: the last one of a region is the one taken at this recording's stop.
                long usedByEvents = 0;
                long purged = 0;
                for (RecordedEvent map : maps) {
                    if (map.getLong("address") == 0) {
                        // The region of the heap buffer: no address, no direct memory.
                        continue;
                    }
                    RecordedEvent state = null;
                    for (RecordedEvent candidate : jfr.eventsOfAnyThread(PageStoreStateEvent.NAME)) {
                        if (candidate.getLong("base") == map.getLong("address")) {
                            state = candidate;
                        }
                    }
                    assertNotNull(state, "a state event of the region");
                    assertEquals(state.getLong("length"), state.getLong("outBytes")
                            + state.getLong("freeCommittedBytes") + state.getLong("purgedBytes")
                            + state.getLong("untouchedBytes"));
                    usedByEvents += state.getLong("outBytes") + state.getLong("freeCommittedBytes");
                    purged += state.getLong("purgedBytes");
                    for (long address : addresses) {
                        if (inside(address, map)) {
                            assertTrue(inRanges(address, state.getString("out")), "a live buffer is claimed");
                        }
                    }
                }
                assertEquals(used, usedByEvents);
                boolean heapRegion = false;
                for (RecordedEvent state : jfr.eventsOfAnyThread(PageStoreStateEvent.NAME)) {
                    if (state.getLong("base") == 0) {
                        heapRegion = true;
                        assertEquals("", state.getString("out"));
                        assertEquals("", state.getString("freeCommitted"));
                        assertEquals("", state.getString("purged"));
                        assertEquals(state.getLong("length"), state.getLong("outBytes")
                                + state.getLong("freeCommittedBytes") + state.getLong("purgedBytes")
                                + state.getLong("untouchedBytes"));
                    }
                }
                assertTrue(heapRegion, "a state event of a heap region");
                // What the state says is purged is what the Purge events say was.
                long purgeEvents = 0;
                for (RecordedEvent purge : jfr.events(PageStorePurgeEvent.NAME)) {
                    purgeEvents += purge.getLong("length");
                }
                assertEquals(purgeEvents, purged);
                assertTrue(purged > 0, "nothing was purged");
            } finally {
                releaseAll(live);
                releaseAll(heapBuffer);
            }
        }
    }

    private static boolean inRanges(long address, String ranges) {
        for (String range : ranges.split(",")) {
            String[] bounds = range.split("-");
            if (address >= Long.parseUnsignedLong(bounds[0], 16) && address < Long.parseUnsignedLong(bounds[1], 16)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The chunk events are the memory taken and given back: what they add up to is the allocator's used memory. Each
     * scenario has an allocator of its own and a recording of its own: a block and one buffer more takes two blocks
     * (two chunk events), churn in a block takes one, and a heap that died with a buffer out has its memory given
     * back when another thread releases it and a pass runs.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void chunkEventsAddUpToTheUsedMemory(boolean direct) throws Exception {
        assumeJfr();
        assumeThreadLocalHeaps();
        final int block = (direct ? Geometry.DEFAULTS.direct : Geometry.DEFAULTS.heap).segmentSize;

        try (JfrEvents jfr = new JfrEvents(Files.createDirectory(dir.resolve("block-and-one")), false)) {
            final AdaptiveByteBufAllocator alloc = allocator(false, Geometry.DEFAULTS);
            final List<ByteBuf> live = new ArrayList<ByteBuf>();
            Thread worker = jfr.runOnThread(false, () -> {
                live.add(direct ? alloc.directBuffer(block) : alloc.heapBuffer(block));
                live.add(direct ? alloc.directBuffer(1) : alloc.heapBuffer(1));
            });
            releaseAll(live);
            long inUse = used(alloc, direct);
            jfr.stop();
            assertEquals(2, jfr.eventsOf(AllocateChunkEvent.NAME, worker).size());
            for (RecordedEvent event : jfr.eventsOf(AllocateChunkEvent.NAME, worker)) {
                assertEquals(block, event.getInt("capacity"));
                assertEquals(direct, event.getBoolean("direct"));
            }
            assertEquals(inUse, chunkBalance(jfr));
        }

        try (JfrEvents jfr = new JfrEvents(Files.createDirectory(dir.resolve("churn")), false)) {
            final AdaptiveByteBufAllocator alloc = allocator(false, Geometry.DEFAULTS);
            Thread worker = jfr.runOnThread(false, () -> {
                for (int i = 0; i < 1000; i++) {
                    (direct ? alloc.directBuffer(1024) : alloc.heapBuffer(1024)).release();
                }
            });
            jfr.stop();
            assertEquals(1, jfr.eventsOf(AllocateChunkEvent.NAME, worker).size());
            assertEquals(used(alloc, direct), chunkBalance(jfr));
        }

        try (JfrEvents jfr = new JfrEvents(Files.createDirectory(dir.resolve("dead-heap")), false)) {
            final AdaptiveByteBufAllocator alloc = allocator(false, Geometry.DEFAULTS);
            final AtomicReference<ByteBuf> held = new AtomicReference<ByteBuf>();
            jfr.runOnThread(true, () -> held.set(direct ? alloc.directBuffer(1024) : alloc.heapBuffer(1024)));
            held.get().release();
            long released = used(alloc, direct);
            jfr.runOnThread(false, () -> useUntilQuiet(alloc, ticker, aboveABlock(alloc, direct, block)));
            jfr.stop();
            assertTrue(used(alloc, direct) < released, "the dead heap's memory was not given back");
            assertEquals(used(alloc, direct), chunkBalance(jfr));
        }
    }

    private static long used(AdaptiveByteBufAllocator alloc, boolean direct) {
        return direct ? alloc.usedDirectMemory() : alloc.usedHeapMemory();
    }

    /** What the Allocate events of a recording add up to, less what the Free events do. */
    private static long chunkBalance(JfrEvents jfr) {
        long balance = 0;
        for (RecordedEvent event : jfr.events(AllocateChunkEvent.NAME)) {
            balance += event.getInt("capacity");
        }
        for (RecordedEvent event : jfr.events(FreeChunkEvent.NAME)) {
            balance -= event.getInt("capacity");
        }
        return balance;
    }

    /**
     * One AllocateBuffer per buffer, one more for the buffer that grew past its slot, with the Reallocate that
     * announces it, and one FreeBuffer per release; the events say where the buffer is.
     */
    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void bufferEventsFollowEveryBuffer(boolean direct, boolean threadLocal) throws Exception {
        assumeJfr();
        if (threadLocal) {
            assumeThreadLocalHeaps();
        }
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = allocator(false, Geometry.DEFAULTS);
            Thread worker = jfr.runOnThread(threadLocal, () -> {
                final List<ByteBuf> buffers = new ArrayList<ByteBuf>();
                for (int size : new int[] {100, 5000, 70_000}) {
                    buffers.add(direct ? alloc.directBuffer(size) : alloc.heapBuffer(size));
                }
                buffers.get(0).capacity(100_000);
                releaseAll(buffers);
            });
            jfr.stop();

            List<Integer> allocated = new ArrayList<Integer>();
            for (RecordedEvent event : jfr.eventsOf(AllocateBufferEvent.NAME, worker)) {
                allocated.add(event.getInt("size"));
                assertEquals(direct, event.getBoolean("direct"));
                assertEquals(threadLocal, event.getBoolean("chunkThreadLocal"));
                assertTrue(event.getBoolean("chunkPooled"));
            }
            Collections.sort(allocated);
            assertEquals(Arrays.asList(100, 5000, 70_000, 100_000), allocated);
            List<RecordedEvent> grown = jfr.eventsOf(ReallocateBufferEvent.NAME, worker);
            assertEquals(1, grown.size());
            assertEquals(100, grown.get(0).getInt("size"));
            assertEquals(100_000, grown.get(0).getInt("newCapacity"));
            List<Integer> freed = new ArrayList<Integer>();
            for (RecordedEvent event : jfr.eventsOf(FreeBufferEvent.NAME, worker)) {
                freed.add(event.getInt("size"));
                assertEquals(direct, event.getBoolean("direct"));
            }
            Collections.sort(freed);
            assertEquals(Arrays.asList(5000, 70_000, 100_000), freed);
        }
    }
}
