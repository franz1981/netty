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

import io.netty.buffer.AdaptivePoolingAllocator.Heap;
import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import io.netty.buffer.PageStoreTestSupport.CountingMemorySource;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static io.netty.buffer.AdaptivePoolingAllocator.IS_LOW_MEM;
import static io.netty.buffer.PageStoreConfig.SLICE_SIZE_BYTES;
import static io.netty.buffer.PageStoreTestSupport.INTERVAL;
import static io.netty.buffer.PageStoreTestSupport.SEGMENT_SIZE;
import static io.netty.buffer.PageStoreTestSupport.asOwner;
import static io.netty.buffer.PageStoreTestSupport.assertAccounted;
import static io.netty.buffer.PageStoreTestSupport.newAllocator;
import static io.netty.buffer.PageStoreTestSupport.offsetIn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The page store inside an {@link AdaptivePoolingAllocator}, of direct or heap memory: size-class chunks as spans of
 * its shared slices, here of regions of one block, reused across size classes, freed by the heaps' decays and given
 * back by the purge, and the defaults per memory mode.
 */
public class AdaptiveSegmentsTest {
    private static final int[] SIZE_CLASSES = SizeClassTable.SIZES.clone();

    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    /** The page kinds of this test's blocks: 64 slices of 64 KiB. */
    private static final int[] KINDS = SizeClassTable.pageKinds(SEGMENT_SIZE / SLICE_SIZE_BYTES,
            SLICE_SIZE_BYTES);

    /** Slices of the chunk of each size class in this test's blocks. */
    private static int expectedSlices(int sizeClass) {
        return SizeClassTable.slicesPerChunk(sizeClass, KINDS, SLICE_SIZE_BYTES);
    }

    /** The buffers a chunk hands out: an exact fit gives one up for its colours. */
    private static int expectedBuffers(int sizeClass) {
        return SizeClassTable.slotsPerChunk(sizeClass, expectedSlices(sizeClass) * SLICE_SIZE_BYTES);
    }

    /**
     * Whether {@code buf} is a slot of a size-class chunk: every size class's buffers are, but in low-memory mode,
     * where the classes above 16896 bytes are not pooled.
     */
    private static boolean pooled(ByteBuf buf) {
        boolean pooled = PageStoreTestSupport.adaptive(buf).chunk instanceof SizeClassedChunk;
        assertTrue(pooled || IS_LOW_MEM, "every size class is pooled outside low-memory mode");
        return pooled;
    }

    /** A purge as of {@code now}, after which every region idle for the purge delay is gone. */
    private static void purge(AdaptivePoolingAllocator allocator, long now) {
        allocator.pageStore.purgeIfDue(now + 2 * allocator.pageStore.config.purgeDelayNanos);
    }

    /** Runs a forced decay of {@code heap}, under its lock if it is a stripe, as its allocations would. */
    private static void decay(Heap heap, long now) {
        asOwner(heap, () -> heap.releaseIdle(now));
    }

    private static SizeClassedChunk chunkOf(ByteBuf buf) {
        return PageStoreTestSupport.chunk(buf);
    }

    private static <T> T onFastThreadLocalThread(Callable<T> task) throws Exception {
        FutureTask<T> future = new FutureTask<T>(task);
        Thread thread = new FastThreadLocalThread(future);
        thread.start();
        T result = future.get();
        thread.join();
        return result;
    }

    /**
     * Every pooled size class gets a chunk that is a span of a segment of its heap, of the size of the plan's table,
     * holding as many segments as fit; the buffer's memory lies inside the segment, at the span's offset. No chunk
     * buffer is allocated on its own, and the used memory is the segments, whole.
     */
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void sizeClassChunksAreSpansOfSegments(boolean heap, final boolean threadLocal) throws Exception {
        assumeFalse(IS_LOW_MEM && threadLocal, "low-memory mode has no thread-local heaps");
        final CountingMemorySource source = new CountingMemorySource(heap);
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        Callable<List<ByteBuf>> work = () -> {
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int size : SIZE_CLASSES) {
                ByteBuf buf = allocator.allocate(size, size);
                bufs.add(buf);
                if (!pooled(buf)) {
                    continue;
                }
                SizeClassedChunk chunk = chunkOf(buf);
                assertNotNull(chunk.segment, "size " + size);
                long offset = offsetIn(buf, chunk.segment);
                assertTrue(offset >= (long) chunk.spanStart * SLICE_SIZE_BYTES
                        && offset + size <= ((long) chunk.spanStart + chunk.magazine.slices) * SLICE_SIZE_BYTES,
                        "size " + size + " outside its span");
                buf.setLong(0, 0x0123456789ABCDEFL);
                assertEquals(0x0123456789ABCDEFL, buf.getLong(0));
            }
            return bufs;
        };
        List<ByteBuf> bufs = threadLocal ? onFastThreadLocalThread(work) : work.call();
        if (!IS_LOW_MEM) {
            assertTrue(source.chunks.isEmpty(), "no chunk buffer of its own");
        }
        assertEquals((long) source.segmentsAllocated() * SEGMENT_SIZE + source.unreleasedChunkBytes(),
                allocator.usedMemory());
        assertAccounted(source, allocator);
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }

    /**
     * Consecutive chunks of a class whose segments leave a tail start 64 bytes further into their span each time,
     * as many times as the tail allows, then wrap; a class that fits its chunk exactly always starts at the span.
     * Buffers stay inside the span, and a released chunk gives back exactly the slices it claimed, recycled or not.
     */
    @ParameterizedTest
    @CsvSource({"false, 640", "false, 1024", "false, 2048", "false, 2304", "false, 4096",
            "true, 640", "true, 1024", "true, 2048", "true, 2304", "true, 4096"})
    void spanChunksRotateTheirStartThroughTheirTail(boolean heapMemory, int size) throws Exception {
        CountingMemorySource source = new CountingMemorySource(heapMemory);
        AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        int slices = expectedSlices(size);
        int perChunk = expectedBuffers(size);
        int colours = Math.min(16, (slices * SLICE_SIZE_BYTES - perChunk * size) / 64 + 1);
        assertEquals(size == 640 ? 5 : 16, colours);
        int chunks = 40;
        Heap heap = null;
        for (int round = 0; round < 2; round++) {
            // The second round re-creates the chunks with the free lists the first one gave up.
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            List<SizeClassedChunk> seen = new ArrayList<SizeClassedChunk>();
            long[] firstOffsets = new long[chunks];
            for (int i = 0; i < chunks * perChunk; i++) {
                ByteBuf buf = allocator.allocate(size, size);
                bufs.add(buf);
                SizeClassedChunk chunk = chunkOf(buf);
                long offset = offsetInSpan(buf, chunk);
                int c = firstOffset(seen, chunk, firstOffsets);
                firstOffsets[c] = Math.min(firstOffsets[c], offset);
                assertTrue(offset >= 0 && offset + size <= (long) slices * SLICE_SIZE_BYTES,
                        "size " + size + " outside its span");
                buf.setLong(size - 8, 0x0123456789ABCDEFL);
            }
            assertEquals(chunks, seen.size());
            heap = seen.get(0).magazine.heap;
            int perBlock = SEGMENT_SIZE / SLICE_SIZE_BYTES;
            long expectedRegions = (chunks * (long) slices + perBlock - 1) / perBlock;
            assertEquals(expectedRegions * SEGMENT_SIZE, allocator.usedMemory(),
                    "each chunk claimed its slices, no more");
            if (round == 0) {
                for (int i = 0; i < chunks; i++) {
                    // Every buffer of every chunk is out: the lowest offset is where the chunk's buffers start.
                    assertEquals(i % colours * 64, firstOffsets[i], "chunk " + i + " of size " + size);
                    assertEquals(slices, seen.get(i).magazine.slices);
                }
            }
            for (ByteBuf buf : bufs) {
                assertEquals(0x0123456789ABCDEFL, buf.getLong(size - 8));
                buf.release();
            }
            assertAccounted(source, allocator);
        }
        long now = System.nanoTime();
        for (int decay = 1; decay <= 8; decay++) {
            decay(heap, now += INTERVAL);
        }
        purge(allocator, now);
        assertEquals(0, source.segmentsLive(), "every span went back whole");
        assertAccounted(source, allocator);
    }

    /**
     * As {@link #spanChunksRotateTheirStartThroughTheirTail} on a thread-local heap, whose owner allocates from the
     * chunk's local free list: an exact fit's dropped segment is never handed out, coloured or not.
     */
    @ParameterizedTest
    @CsvSource({"false, 1024", "false, 4096", "true, 1024", "true, 4096"})
    void threadLocalSpanChunksNeverHandOutTheDroppedSegment(boolean heapMemory, final int size) throws Exception {
        assumeFalse(IS_LOW_MEM, "low-memory mode has no thread-local heaps");
        final CountingMemorySource source = new CountingMemorySource(heapMemory);
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        final int slices = expectedSlices(size);
        final int perChunk = expectedBuffers(size);
        assertEquals(slices * SLICE_SIZE_BYTES / size - 1, perChunk);
        final int chunks = 20;
        onFastThreadLocalThread(() -> {
            Heap heap = null;
            for (int round = 0; round < 2; round++) {
                // The second round re-creates the chunks with the free lists the first one gave up.
                List<ByteBuf> bufs = new ArrayList<ByteBuf>();
                List<SizeClassedChunk> seen = new ArrayList<SizeClassedChunk>();
                long[] firstOffsets = new long[chunks];
                for (int i = 0; i < chunks * perChunk; i++) {
                    ByteBuf buf = allocator.allocate(size, size);
                    bufs.add(buf);
                    SizeClassedChunk chunk = chunkOf(buf);
                    heap = chunk.magazine.heap;
                    assertTrue(chunk.inThreadLocalMagazine());
                    long offset = offsetInSpan(buf, chunk);
                    int c = firstOffset(seen, chunk, firstOffsets);
                    firstOffsets[c] = Math.min(firstOffsets[c], offset);
                    assertTrue(offset >= 0 && offset + size <= (long) slices * SLICE_SIZE_BYTES,
                            "size " + size + " outside its span");
                    buf.setLong(size - 8, 0x0123456789ABCDEFL);
                }
                assertEquals(chunks, seen.size(), perChunk + " buffers per chunk");
                for (int i = 0; i < chunks; i++) {
                    long colour = firstOffsets[i];
                    assertTrue(colour >= 0 && colour < 16 * 64 && colour % 64 == 0, "colour " + colour);
                    if (round == 0) {
                        assertEquals(i % 16 * 64, colour, "chunk " + i + " of size " + size);
                    }
                }
                for (ByteBuf buf : bufs) {
                    assertEquals(0x0123456789ABCDEFL, buf.getLong(size - 8));
                    buf.release();
                }
            }
            long now = System.nanoTime();
            for (int decay = 1; decay <= 8; decay++) {
                heap.releaseIdle(now += INTERVAL);
            }
            purge(allocator, now);
            return null;
        });
        assertEquals(0, source.segmentsLive(), "every span went back whole");
        assertAccounted(source, allocator);
    }

    /** Where {@code buf} starts in the span of its chunk. */
    private static long offsetInSpan(ByteBuf buf, SizeClassedChunk chunk) {
        return offsetIn(buf, chunk.segment) - (long) chunk.spanStart * SLICE_SIZE_BYTES;
    }

    /** The index of {@code chunk} in {@code seen}, added on first sight with no offset seen yet. */
    private static int firstOffset(List<SizeClassedChunk> seen, SizeClassedChunk chunk, long[] firstOffsets) {
        int i = seen.indexOf(chunk);
        if (i < 0) {
            i = seen.size();
            seen.add(chunk);
            firstOffsets[i] = Long.MAX_VALUE;
        }
        return i;
    }

    /**
     * Buffers of different chunks of one heap segment, each written whole with its own pattern, then read back: no two
     * overlap. The second round's chunks are re-created at other spans than the first round's.
     */
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void buffersOfRecreatedChunksStayInTheirSpans(boolean heap, boolean threadLocal) throws Exception {
        assumeFalse(IS_LOW_MEM && threadLocal, "low-memory mode has no thread-local heaps");
        final CountingMemorySource source = new CountingMemorySource(heap);
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        final int small = 32768; // 8-slice chunks, as the large class's: one bin
        final int large = IS_LOW_MEM ? 16384 : 65536; // 8-slice chunks
        Callable<Void> work = () -> {
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < 3 * (8 * SLICE_SIZE_BYTES / large); i++) {
                bufs.add(allocator.allocate(large, large));
            }
            for (ByteBuf buf : bufs) {
                buf.release();
            }
            bufs.clear();
            // The large class keeps one chunk: another class of its bin takes the freed spans, at other offsets.
            for (int i = 0; i < 3 * (8 * SLICE_SIZE_BYTES / small); i++) {
                bufs.add(allocator.allocate(small, small));
            }
            for (int i = 0; i < 3 * (8 * SLICE_SIZE_BYTES / large); i++) {
                bufs.add(allocator.allocate(large, large));
            }
            int recreated = 0;
            List<SizeClassedChunk> seen = new ArrayList<SizeClassedChunk>();
            for (int i = 0; i < bufs.size(); i++) {
                ByteBuf buf = bufs.get(i);
                SizeClassedChunk chunk = chunkOf(buf);
                if (!seen.contains(chunk)) {
                    seen.add(chunk);
                    recreated += chunk.spanStart != 0 ? 1 : 0;
                }
                long offset = offsetIn(buf, chunk.segment);
                assertTrue(offset >= (long) chunk.spanStart * SLICE_SIZE_BYTES && offset + buf.capacity()
                        <= ((long) chunk.spanStart + chunk.magazine.slices) * SLICE_SIZE_BYTES, "outside its span");
                for (int j = 0; j < buf.capacity(); j += 8) {
                    buf.setLong(j, (long) i << 32 | j);
                }
            }
            assertTrue(recreated > 0, "a chunk re-created, not at the segment's start");
            for (int i = 0; i < bufs.size(); i++) {
                ByteBuf buf = bufs.get(i);
                for (int j = 0; j < buf.capacity(); j += 8) {
                    assertEquals((long) i << 32 | j, buf.getLong(j));
                }
                buf.release();
            }
            assertAccounted(source, allocator);
            return null;
        };
        if (threadLocal) {
            onFastThreadLocalThread(work);
        } else {
            work.call();
        }
        assertEquals(1, source.segmentsAllocated());
        assertTrue(source.chunks.isEmpty(), "no chunk buffer of its own");
    }

    /**
     * Chunks given up by one size class free their spans, which other classes reuse, of any chunk size: no new block.
     * Then decays free everything (an idle class gives up its chunks), and the purge gives back the blocks idle for
     * its delay.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void spansAreReusedAcrossClassesAndDecaysGiveSegmentsBack(boolean heap) throws Exception {
        CountingMemorySource source = new CountingMemorySource(heap);
        AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        int size = IS_LOW_MEM ? 16384 : 65536; // 8-slice chunks either way
        int perChunk = expectedBuffers(size);
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 12 * perChunk; i++) {
            bufs.add(allocator.allocate(size, size));
        }
        // 12 chunks of 8 slices: the first block is filled (8 of them), then a second one.
        assertEquals(2, source.segmentsAllocated());
        Heap stripe = chunkOf(bufs.get(0)).magazine.heap;
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        bufs.clear();
        assertAccounted(source, allocator);
        // Every chunk ran out of segments, so none is active; the class keeps the last four to empty (its floor), in
        // the second block. The first block's slices are free, the block stays until it is idle for the purge delay.
        assertEquals(2, source.segmentsLive());
        // Another class, another chunk size: from the free slices.
        int other = 1024; // 1-slice chunks
        for (int i = 0; i < 16 * expectedBuffers(other); i++) {
            bufs.add(allocator.allocate(other, other));
        }
        assertEquals(2, source.segmentsAllocated(), "16 slices fit in the free ones");
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(source, allocator);

        // Decays, forced, one interval apart. Nothing allocates in between, so both classes go idle.
        long now = System.nanoTime();
        for (int decay = 1; decay <= 8; decay++) {
            now += INTERVAL;
            decay(stripe, now);
            assertAccounted(source, allocator);
        }
        purge(allocator, now);
        assertEquals(0, source.segmentsLive(), "every chunk was given up");
        assertEquals(0, allocator.usedMemory());
        assertEquals(2, source.segmentsAllocated());
    }

    /**
     * On a thread-local heap every other thread's release is a note. The chunks such releases empty are applied by
     * the owner's decays: an idle class gives them up, and their slices are free.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void foreignReleasesEmptyASegmentThroughTheNotes(boolean heapMemory) throws Exception {
        assumeFalse(IS_LOW_MEM, "low-memory mode has no thread-local heaps");
        final CountingMemorySource source = new CountingMemorySource(heapMemory);
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        final int size = 65536;
        final int perChunk = expectedBuffers(size);
        onFastThreadLocalThread(() -> {
            final List<ByteBuf> bufs = new ArrayList<ByteBuf>();
            for (int i = 0; i < 3 * perChunk; i++) {
                bufs.add(allocator.allocate(size, size));
            }
            Heap heap = chunkOf(bufs.get(0)).magazine.heap;
            Thread releaser = new Thread(() -> {
                for (ByteBuf buf : bufs) {
                    buf.release();
                }
            });
            releaser.start();
            releaser.join();
            long now = System.nanoTime();
            heap.releaseIdle(now + INTERVAL); // the class allocated since the last decay: not idle yet
            heap.releaseIdle(now + 2 * INTERVAL); // idle: its chunks, applied from the notes, are given up
            // Every span is free now, and nothing else holds the block: the purge can give the whole thing back.
            purge(allocator, now + 2 * INTERVAL);
            assertAccounted(source, allocator);
            return null;
        });
        assertEquals(0, source.segmentsLive(), "the notes were applied and every span went back");
        assertEquals(1, source.segmentsAllocated());
        assertAccounted(source, allocator);
    }

    /**
     * A thread-local heap freed (its thread ended) while buffers are still out: their chunks are abandoned to the page
     * store, the releases, on another thread, bring every segment back, and the next purge pass frees their slices;
     * the block goes back once idle for the purge delay.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void segmentsOfAnEndedThreadGoBackWithTheirLastBuffer(boolean heap) throws Exception {
        assumeFalse(IS_LOW_MEM, "low-memory mode has no thread-local heaps");
        final CountingMemorySource source = new CountingMemorySource(heap);
        final AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, SEGMENT_SIZE));
        List<ByteBuf> bufs = onFastThreadLocalThread(() -> {
            List<ByteBuf> out = new ArrayList<ByteBuf>();
            for (int i = 0; i < 20; i++) {
                // 8-slice chunks both: one bin, one block.
                out.add(allocator.allocate(65536, 65536));
                out.add(allocator.allocate(32768, 32768));
            }
            // Some released before the thread ends.
            for (int i = 0; i < 10; i++) {
                out.remove(out.size() - 1).release();
            }
            return out;
        });
        assertEquals(1, source.segmentsAllocated());
        assertEquals(SEGMENT_SIZE, allocator.usedMemory());
        assertEquals(1, source.segmentsLive(), "spans are still out");
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        purge(allocator, System.nanoTime());
        assertEquals(0, source.segmentsLive());
        assertEquals(0, allocator.usedMemory());
        assertAccounted(source, allocator);
    }

    /** 2 MiB segments: 32 slices, page kinds of 1, 4 and 16 slices; 16896 takes 4-slice chunks of 15 buffers. */
    @Test
    void twoMebibyteSegments() throws Exception {
        CountingMemorySource source = new CountingMemorySource();
        AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, 2 * 1024 * 1024));
        int size = 16896;
        int perChunk = 15;
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int i = 0; i < 9 * perChunk; i++) {
            bufs.add(allocator.allocate(size, size));
        }
        assertEquals(2, source.segmentsAllocated(), "eight 4-slice chunks per 32-slice segment");
        assertEquals(4L * 1024 * 1024, allocator.usedMemory());
        for (ByteBuf buf : bufs) {
            buf.release();
        }
        assertAccounted(source, allocator);
    }

    /**
     * The defaults: 4 MiB segments in 64-segment regions; 2 MiB segments in low-memory mode, where the single stripe
     * carves its chunks out of segments too. {@code mmap} regions wherever they can be mapped, for direct memory only;
     * else regions of one block, as heap memory always has. The memory accounted is a segment, direct or heap. Heap
     * segments are cut under G1 (see {@link PageStoreConfig#clampHeapSegmentSize}).
     */
    @Test
    void segmentDefaultsFollowTheMemoryMode() throws Exception {
        assumeFalse(System.getProperty("io.netty.allocator.segmentSize") != null
                || System.getProperty("io.netty.allocator.segmentRegionSize") != null, "set explicitly");
        assertEquals(IS_LOW_MEM ? 2 * 1024 * 1024 : 4 * 1024 * 1024, PageStoreConfig.SEGMENT_SIZE_BYTES);
        assertEquals(Long.SIZE * PageStoreConfig.SEGMENT_SIZE_BYTES, PageStoreConfig.SEGMENT_REGION_SIZE_BYTES);
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(true, false));
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        // mmap'd: the chunk's slices, 64 KiB for 1 KiB buffers; malloc'd: the whole region.
        long directUsed = PageStoreTestSupport.directRegionsAreMapped() ? 64 * 1024 :
                PageStoreConfig.SEGMENT_SIZE_BYTES;
        assertEquals(directUsed, allocator.metric().usedDirectMemory());
        int heapSegmentSize = PageStoreConfig.heapDefaults().segmentSize;
        ByteBuf heap = allocator.heapBuffer(1024, 1024);
        assertSame(chunkOf(heap).segment.buffer.array(), heap.array(), "the block's own byte[]");
        assertEquals(heapSegmentSize, allocator.metric().usedHeapMemory());
        buf.release();
        heap.release();
        assertEquals(directUsed, allocator.metric().usedDirectMemory(),
                "the segment stays with the chunk its size class keeps");
    }

    /** The allocator rejects a page store whose segments cannot hold a buffer of its largest size class. */
    @Test
    void segmentsMustHoldTheLargestSizeClass() {
        final CountingMemorySource source = new CountingMemorySource();
        assertThrows(IllegalArgumentException.class, () -> new AdaptivePoolingAllocator(source, true, null,
                new PageStoreConfig(2 * SLICE_SIZE_BYTES, SLICE_SIZE_BYTES, INTERVAL, 0, 0, 2 * SLICE_SIZE_BYTES)),
                "132 KiB takes 3 slices");
    }

    /**
     * Segments of 7 slices, as heap segments under G1 with 1 MiB regions: page kinds of 1 and 7 slices, every size
     * class still allocates, and every buffer stays inside its segment.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sevenSliceSegmentsHoldEverySizeClass(boolean heap) throws Exception {
        CountingMemorySource source = new CountingMemorySource(heap);
        int segmentSize = 7 * SLICE_SIZE_BYTES;
        AdaptivePoolingAllocator allocator = closer.add(newAllocator(source, segmentSize));
        List<ByteBuf> bufs = new ArrayList<ByteBuf>();
        for (int size : SIZE_CLASSES) {
            for (int j = 0; j < 2; j++) {
                ByteBuf buf = allocator.allocate(size, size);
                buf.setLong(size - 8, size);
                bufs.add(buf);
                if (!pooled(buf)) {
                    continue;
                }
                SizeClassedChunk chunk = chunkOf(buf);
                long offset = offsetIn(buf, chunk.segment);
                assertTrue(offset >= 0 && offset + size <= segmentSize, "size " + size + " outside its segment");
            }
        }
        for (ByteBuf buf : bufs) {
            assertEquals(buf.capacity(), buf.getLong(buf.capacity() - 8));
            buf.release();
        }
        if (!IS_LOW_MEM) {
            assertTrue(source.chunks.isEmpty(), "no chunk buffer of its own");
        }
        assertAccounted(source, allocator);
    }
}
