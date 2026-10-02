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

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.MemoryAddress;
import jdk.jfr.Name;
import jdk.jfr.Period;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Where every {@link PageStore}'s memory is, periodically, on the JFR periodic thread: one event per region, with the
 * address ranges of its slices claimed, free with memory behind them, purged, and never used; and one event per
 * store (region -1) with the segments in the heaps' reserves and those allocated on their own. Read racily, without
 * locks: a slice or reserve that changes meanwhile may be reported in either state.
 */
@Enabled(false)
@Category("Netty")
@Period("1 s")
@Name(PageStoreStateEvent.NAME)
@Label("Page Store State")
@Description("Periodic map of a page store's memory: region slices by state, heap reserves, own segments")
@SuppressWarnings("Since15")
final class PageStoreStateEvent extends Event {
    static final String NAME = "io.netty.PageStoreState";
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PageStoreStateEvent.class);
    private static final List<WeakReference<PageStore>> STORES = new CopyOnWriteArrayList<WeakReference<PageStore>>();

    static {
        try {
            FlightRecorder.addPeriodicEvent(PageStoreStateEvent.class, new Runnable() {
                @Override
                public void run() {
                    emitAll();
                }
            });
        } catch (Throwable t) {
            logger.debug("Cannot register the periodic page store event.", t);
        }
    }

    @Description("Identity of the page store (its allocator's)")
    public int store;
    @Description("Index of the region, or -1 for the store's heap reserves and own segments")
    public int region;
    @Description("Start of the region")
    @MemoryAddress
    public long base;
    @DataAmount
    @Description("Length of the region")
    public long length;
    @DataAmount
    @Description("Bytes of slices claimed by chunks or buffers")
    public long outBytes;
    @DataAmount
    @Description("Bytes of free slices with memory behind them")
    public long freeCommittedBytes;
    @DataAmount
    @Description("Bytes of free slices purged since they were last used")
    public long purgedBytes;
    @DataAmount
    @Description("Bytes of slices never used")
    public long untouchedBytes;
    @DataAmount
    @Description("Bytes of the segments in the heaps' reserves")
    public long reserveBytes;
    @DataAmount
    @Description("Bytes of the segments allocated on their own that are out")
    public long ownBytes;
    @Description("Address ranges of the slices claimed, as start-end in hex, comma separated")
    public String out;
    @Description("Address ranges of the free slices with memory behind them")
    public String freeCommitted;
    @Description("Address ranges of the purged free slices")
    public String purged;
    @Description("Address ranges of the reserved segments, each prefixed by its heap kind")
    public String reserve;
    @Description("Address ranges of the segments allocated on their own")
    public String own;

    private static final PageStoreStateEvent INSTANCE = new PageStoreStateEvent();

    static boolean isEventEnabled() {
        return INSTANCE.isEnabled();
    }

    static void register(PageStore store) {
        for (WeakReference<PageStore> ref : STORES) {
            if (ref.get() == null) {
                STORES.remove(ref);
            }
        }
        STORES.add(new WeakReference<PageStore>(store));
    }

    private static void emitAll() {
        for (WeakReference<PageStore> ref : STORES) {
            PageStore store = ref.get();
            if (store == null) {
                STORES.remove(ref);
            } else {
                emit(store);
            }
        }
    }

    private static void emit(PageStore store) {
        int id = System.identityHashCode(store.allocator);
        long sliceSize = store.config.sliceSize;
        int perBlock = store.config.slicesPerSegment();
        for (Region region : store.regions) {
            if (region.released) {
                continue;
            }
            int units = region.slots * perBlock;
            PageStoreStateEvent event = new PageStoreStateEvent();
            event.store = id;
            event.region = region.index;
            event.base = region.buffer._memoryAddress();
            event.length = region.slots * (long) store.config.segmentSize;
            StringBuilder out = new StringBuilder();
            StringBuilder committed = new StringBuilder();
            StringBuilder purged = new StringBuilder();
            int runStart = 0;
            int runState = -1;
            for (int slot = 0; slot <= units; slot++) {
                int state = slot == units ? -1 : sliceStateOf(region, slot, perBlock);
                if (state != runState) {
                    if (runState >= 0) {
                        long bytes = (slot - runStart) * sliceSize;
                        long start = event.base + runStart * sliceSize;
                        switch (runState) {
                            case OUT:
                                event.outBytes += bytes;
                                range(out, "", start, bytes);
                                break;
                            case COMMITTED:
                                event.freeCommittedBytes += bytes;
                                range(committed, "", start, bytes);
                                break;
                            case PURGED:
                                event.purgedBytes += bytes;
                                range(purged, "", start, bytes);
                                break;
                            default:
                                event.untouchedBytes += bytes;
                        }
                    }
                    runStart = slot;
                    runState = state;
                }
            }
            event.out = out.toString();
            event.freeCommitted = committed.toString();
            event.purged = purged.toString();
            event.commit();
        }
        PageStoreStateEvent event = new PageStoreStateEvent();
        event.store = id;
        event.region = -1;
        StringBuilder reserve = new StringBuilder();
        if (store.heaps != null) {
            for (Iterator<WeakReference<HeapSegments>> it = store.heaps.iterator(); it.hasNext();) {
                HeapSegments heap = it.next().get();
                if (heap == null || heap.isFreed()) {
                    it.remove();
                    continue;
                }
                Segment[] segments = heap.reserve;
                for (int i = 0, n = Math.min(heap.reserved, segments.length); i < n; i++) {
                    Segment segment = segments[i];
                    if (segment != null) {
                        event.reserveBytes += segment.capacity();
                        range(reserve, heap.kind() + ':', segment.memoryAddress(), segment.capacity());
                    }
                }
            }
        }
        StringBuilder own = new StringBuilder();
        if (store.ownSegments != null) {
            for (Segment segment : store.ownSegments) {
                event.ownBytes += segment.capacity();
                range(own, "", segment.memoryAddress(), segment.capacity());
            }
        }
        event.reserve = reserve.toString();
        event.own = own.toString();
        event.commit();
    }

    private static final int OUT = 0;
    private static final int COMMITTED = 1;
    private static final int PURGED = 2;
    private static final int UNTOUCHED = 3;

    private static int sliceStateOf(Region region, int slice, int perBlock) {
        Segment block = region.block(slice / perBlock);
        int i = slice % perBlock;
        if ((block.free & 1L << i) == 0) {
            return OUT;
        }
        if (block.freedAt[i] != Region.UNCOMMITTED) {
            return COMMITTED;
        }
        return region.sliceEverCommitted[slice] ? PURGED : UNTOUCHED;
    }

    private static void range(StringBuilder ranges, String prefix, long start, long bytes) {
        if (ranges.length() != 0) {
            ranges.append(',');
        }
        ranges.append(prefix).append(Long.toHexString(start)).append('-').append(Long.toHexString(start + bytes));
    }
}
