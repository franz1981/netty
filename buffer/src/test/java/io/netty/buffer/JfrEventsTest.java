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

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Timeout(10)
@EnabledForJreRange(min = JRE.JAVA_17) // RecordingStream
@Isolated
public class JfrEventsTest {
    PooledByteBufAllocator newPooledAllocator(boolean preferDirect) {
        return new PooledByteBufAllocator(preferDirect);
    }

    /** Heap chunks allocated one by one, no segments: the tests count the events of those chunks. */
    AdaptiveByteBufAllocator newAdaptiveAllocator(boolean preferDirect) {
        return new AdaptiveByteBufAllocator(preferDirect, false, false);
    }

    /**
     * A chunk whose buffer goes to the {@code SizeClassChunkRecycler} has its {@code delegate}
     * nulled before it is deallocated. {@code deallocate()} used to fire the FreeChunk event first,
     * and {@code AbstractChunkEvent.fill} reads {@code isDirect()}/{@code memoryAddress()}, both of
     * which dereference the delegate - so with chunk recording enabled this threw NPE straight out
     * of {@link ByteBuf#release()}.
     */
    @SuppressWarnings("Since15")
    @Test
    public void adaptiveChunkRecyclingMustNotThrowWhileChunkEventsAreRecorded() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            stream.enable(FreeChunkEvent.class);
            stream.onEvent(FreeChunkEvent.NAME, e -> { });
            stream.startAsync();

            // useCacheForNonEventLoopThreads=false -> shared path, so chunkRecycler is non-null
            AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(false, false, false);
            int buffersPerChunk = 512;
            List<ByteBuf> bufs = new ArrayList<ByteBuf>();

            // A working set above the retention floor, then fully released, so eviction fires and
            // recycleOrDeallocate hands buffers to the recycler.
            for (int i = 0; i < 40 * buffersPerChunk; i++) {
                bufs.add(alloc.heapBuffer(256));
            }
            for (ByteBuf b : bufs) {
                b.release();
            }
            bufs.clear();

            // keep allocating so purge ticks fire against the now-idle chunks
            for (int round = 0; round < 80; round++) {
                for (int i = 0; i < buffersPerChunk; i++) {
                    bufs.add(alloc.heapBuffer(256));
                }
                for (ByteBuf b : bufs) {
                    b.release();
                }
                bufs.clear();
            }
        }
    }

    /**
     * The chunk events describe the memory the allocator takes from and gives back to its chunk allocator, so over
     * any workload the bytes allocated minus the bytes freed must equal the allocator's used memory: chunk buffers
     * that a heap's recycler takes over and hands to a chunk of another size class are neither freed nor allocated
     * again, a one-shot buffer is announced both ways, and a thread-local heap that dies frees what its recycler
     * held. The workload runs on one thread, whose events are the only ones counted.
     */
    @SuppressWarnings("Since15")
    @Test
    public void adaptiveChunkEventsAddUpToUsedMemory() throws Exception {
        Field lowMem = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        lowMem.setAccessible(true);
        assumeFalse(lowMem.getBoolean(null), "low-memory mode has no thread-local heaps and pools less");
        // useCacheForNonEventLoopThreads: the workload thread gets a thread-local heap, which its end frees. Heap
        // chunks allocated one by one: no segments.
        final AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(false, true, false);
        final String threadName = "adaptive-chunk-events";
        // A one-shot chunk of a size nothing else allocates, above a segment, committed after the workload: events
        // arrive in the order they were committed, so once it is seen every event of the workload has been.
        final int sentinel = 5 * 1024 * 1024 + 4099;
        final CountDownLatch sentinelSeen = new CountDownLatch(1);
        final long[] allocatedFreed = new long[2];
        final int[] oneShots = new int[2];
        final AtomicInteger sizeClassChunksFreed = new AtomicInteger();
        final AtomicInteger sizeClassChunksAllocated = new AtomicInteger();
        try (RecordingStream stream = new RecordingStream()) {
            stream.enable(AllocateChunkEvent.class);
            stream.enable(FreeChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME, event -> {
                if (event.getInt("capacity") == sentinel) {
                    sentinelSeen.countDown();
                } else if (onWorkloadThread(event, threadName)) {
                    allocatedFreed[0] += event.getInt("capacity");
                    if (event.getInt("capacity") == 512 * 1024) {
                        sizeClassChunksAllocated.incrementAndGet();
                    }
                    oneShots[0] += event.getBoolean("pooled") ? 0 : 1;
                }
            });
            stream.onEvent(FreeChunkEvent.NAME, event -> {
                if (onWorkloadThread(event, threadName)) {
                    int capacity = event.getInt("capacity");
                    allocatedFreed[1] += capacity;
                    oneShots[1] += event.getBoolean("pooled") ? 0 : 1;
                    if (event.getBoolean("pooled") && capacity == 512 * 1024) {
                        sizeClassChunksFreed.incrementAndGet();
                    }
                }
            });
            stream.startAsync();

            Thread thread = new FastThreadLocalThread(() -> {
                // 16 KiB and 64 KiB buffers use 512 KiB chunks and share a recycler pool: a burst of one, fully
                // released, leaves chunk buffers in the recycler that the other then builds its chunks from.
                releaseAll(allocateMany(alloc, 16 * 1024, 32 * 8));
                releaseAll(allocateMany(alloc, 64 * 1024, 8 * 8));
                // One-shot: above the largest pooled buffer.
                alloc.heapBuffer(4 * 1024 * 1024).release();
                // Above the size classes: large-buffer chunks of the thread-local heap, kept idle there after the
                // release.
                releaseAll(allocateMany(alloc, 512 * 1024, 8));
                // The thread-local heap is freed when this thread ends, with what its recycler holds.
            }, threadName);
            thread.start();
            thread.join();
            new AdaptiveByteBufAllocator(false).heapBuffer(sentinel).release();
            sentinelSeen.await();
        }
        // 8 chunks of 16 KiB buffers, then 8 of 64 KiB: the second burst must build some of its chunks from the
        // buffers the first one gave up, or the balance below would not cover the recycled path.
        assertTrue(sizeClassChunksAllocated.get() < 16,
                sizeClassChunksAllocated.get() + " chunk buffers allocated: none came from the recycler");
        assertEquals(1, oneShots[0], "the one-shot chunk is announced when it is allocated");
        assertEquals(1, oneShots[1], "and when it is freed");
        assertTrue(sizeClassChunksFreed.get() > 0, "the dying thread-local heap must free its chunk buffers");
        assertEquals(alloc.metric().usedHeapMemory(), allocatedFreed[0] - allocatedFreed[1],
                "allocated " + allocatedFreed[0] + " - freed " + allocatedFreed[1] + " must be the used memory");
    }

    /**
     * The same balance for memory whose size-class chunks are spans of segments, direct or heap: the events are per
     * segment (a span fires none; a segment of a region fires them when first taken and when purged or unmapped), and
     * a segment still holding a buffer when its thread-local heap dies goes back when another thread releases that
     * buffer, with its events on that thread.
     */
    @SuppressWarnings("Since15")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void adaptiveSegmentChunkEventsAddUpToUsedMemory(final boolean direct) throws Exception {
        Field lowMem = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        lowMem.setAccessible(true);
        assumeFalse(lowMem.getBoolean(null), "low-memory mode has no thread-local heaps and pools less");
        final AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, true);
        final int segmentSize = direct ? AdaptiveByteBufAllocatorTest.directSegmentSize(alloc) :
                AdaptiveByteBufAllocatorTest.heapSegmentSize(alloc);
        assumeTrue(segmentSize > 0, "chunks are not carved out of segments");
        final String threadName = "adaptive-segment-chunk-events";
        final int sentinel = 5 * 1024 * 1024 + 4099;
        final CountDownLatch sentinelSeen = new CountDownLatch(1);
        final long[] allocatedFreed = new long[2];
        final int[] oneShots = new int[2];
        final int[] segments = new int[2];
        final List<Integer> units = new ArrayList<Integer>();
        final List<String> otherSegmentEvents = new ArrayList<String>();
        final ByteBuf[] heldPastTheEnd = new ByteBuf[1];
        try (RecordingStream stream = new RecordingStream()) {
            stream.enable(AllocateChunkEvent.class);
            stream.enable(FreeChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME, event -> {
                if (event.getInt("capacity") == sentinel) {
                    sentinelSeen.countDown();
                } else if (onWorkloadThread(event, threadName)) {
                    allocatedFreed[0] += event.getInt("capacity");
                    oneShots[0] += event.getBoolean("pooled") ? 0 : 1;
                    if (isPageStore(event)) {
                        segments[0]++;
                        units.add(event.getInt("capacity"));
                    }
                } else if (isPageStore(event) && !"Finalizer".equals(threadOf(event))) {
                    // The finalizer frees the allocators earlier tests dropped, with their segments.
                    otherSegmentEvents.add(threadOf(event));
                }
            });
            stream.onEvent(FreeChunkEvent.NAME, event -> {
                if (onWorkloadThread(event, threadName)) {
                    allocatedFreed[1] += event.getInt("capacity");
                    oneShots[1] += event.getBoolean("pooled") ? 0 : 1;
                    segments[1] += isPageStore(event) ? 1 : 0;
                } else if (isPageStore(event) && !"Finalizer".equals(threadOf(event))) {
                    // The finalizer frees the allocators earlier tests dropped, with their segments.
                    otherSegmentEvents.add(threadOf(event));
                }
            });
            stream.startAsync();

            Thread thread = new FastThreadLocalThread(() -> {
                // Spans of 8 slices: the 16 KiB burst fills a segment, the 64 KiB one reuses its spans.
                releaseAll(allocateMany(alloc, direct, 16 * 1024, 32 * 8));
                releaseAll(allocateMany(alloc, direct, 64 * 1024, 8 * 8));
                allocateMany(alloc, direct, segmentSize, 1).get(0).release();
                // At most a segment: heap segments are smaller under G1 with small regions.
                releaseAll(allocateMany(alloc, direct, Math.min(512 * 1024, segmentSize), 8));
                // The size classes go idle and give their spans back: the emptied segment waits in the cache.
                decayThreadLocalHeap(alloc, direct ? "direct" : "heap", 3);
                heldPastTheEnd[0] = allocateMany(alloc, direct, 1024, 1).get(0);
            }, threadName);
            thread.start();
            thread.join();
            // The heap is gone; this release empties its last segment, from another thread.
            Thread releaser = new Thread(() -> heldPastTheEnd[0].release(), threadName + "-releaser");
            releaser.start();
            releaser.join();
            new AdaptiveByteBufAllocator(false).heapBuffer(sentinel).release();
            sentinelSeen.await();
        }
        assertTrue(segments[0] > 0, "segments are announced");
        assertEquals(Collections.emptyList(), otherSegmentEvents, "segment events are on the workload threads");
        long unit = segmentSize;
        boolean shared = direct && AdaptiveByteBufAllocatorTest.directSharesSlices(alloc);
        for (int capacity : units) {
            if (shared) {
                // With shared slices, each event is the slices a claim committed.
                assertEquals(0, capacity % PageStoreConfig.SLICE_SIZE_BYTES, "page store events are whole slices");
            } else {
                assertEquals(unit, capacity, "segment events have the size of a segment");
            }
        }
        // The 512 KiB buffers are spans of the heaps' segments. The segment-sized one takes a slot of a region, or,
        // without regions, is a one-shot of its own exact size.
        int oneShot = direct && AdaptiveByteBufAllocatorTest.directRegions(alloc) ? 0 : 1;
        assertEquals(oneShot, oneShots[0], "one-shots allocated outside the page store");
        assertEquals(oneShot, oneShots[1]);
        assertEquals(direct ? alloc.metric().usedDirectMemory() : alloc.metric().usedHeapMemory(),
                allocatedFreed[0] - allocatedFreed[1],
                "allocated " + allocatedFreed[0] + " - freed " + allocatedFreed[1] + " must be the used memory");
    }

    @SuppressWarnings("Since15")
    private static boolean isPageStore(RecordedEvent event) {
        return event.getBoolean("segment");
    }

    private static List<ByteBuf> allocateMany(ByteBufAllocator alloc, boolean direct, int size, int count) {
        List<ByteBuf> bufs = new ArrayList<ByteBuf>(count);
        for (int i = 0; i < count; i++) {
            bufs.add(direct ? alloc.directBuffer(size, size) : alloc.heapBuffer(size, size));
        }
        return bufs;
    }

    /**
     * Run the calling thread's thread-local heap's decay {@code decays} times now.
     */
    private static void decayThreadLocalHeap(AdaptiveByteBufAllocator alloc, String which, int decays) {
        try {
            Field heapField = AdaptiveByteBufAllocator.class.getDeclaredField(which);
            heapField.setAccessible(true);
            Object pooling = heapField.get(alloc);
            Field tlField = pooling.getClass().getDeclaredField("threadLocalSizeClassHeap");
            tlField.setAccessible(true);
            Object heap = ((FastThreadLocal<?>) tlField.get(pooling)).get();
            Field decayField = heap.getClass().getDeclaredField("idleDecay");
            decayField.setAccessible(true);
            AdaptivePoolingAllocator.IdleDecay idleDecay = (AdaptivePoolingAllocator.IdleDecay) decayField.get(heap);
            for (int i = 0; i < decays; i++) {
                idleDecay.decay(System.nanoTime());
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("Since15")
    private static String threadOf(RecordedEvent event) {
        return event.getThread() != null ? event.getThread().getJavaName() : null;
    }

    @SuppressWarnings("Since15")
    private static boolean onWorkloadThread(RecordedEvent event, String threadName) {
        return event.getThread() != null && event.getThread().getJavaName() != null
                && event.getThread().getJavaName().startsWith(threadName);
    }

    private static List<ByteBuf> allocateMany(ByteBufAllocator alloc, int size, int count) {
        List<ByteBuf> bufs = new ArrayList<ByteBuf>(count);
        for (int i = 0; i < count; i++) {
            bufs.add(alloc.heapBuffer(size, size));
        }
        return bufs;
    }

    private static void releaseAll(List<ByteBuf> bufs) {
        for (ByteBuf buf : bufs) {
            buf.release();
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void pooledJfrChunkAllocation() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            CompletableFuture<RecordedEvent> allocateFuture = new CompletableFuture<>();

            stream.enable(AllocateChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME, allocateFuture::complete);
            stream.startAsync();

            PooledByteBufAllocator alloc = newPooledAllocator(true);
            alloc.directBuffer(128).release();

            RecordedEvent allocate = allocateFuture.get();
            assertEquals(alloc.metric().chunkSize(), allocate.getInt("capacity"));
            assertTrue(allocate.getBoolean("pooled"));
            assertFalse(allocate.getBoolean("threadLocal"));
            assertTrue(allocate.getBoolean("direct"));
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void pooledShouldCreateTwoChunks() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            final CountDownLatch eventsFlushed = new CountDownLatch(2);
            stream.enable(AllocateChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME,
                    event -> {
                        eventsFlushed.countDown();
                    });
            stream.startAsync();
            PooledByteBufAllocator allocator = newPooledAllocator(false);
            int bufSize = 16896;
            int bufsToAllocate = 1 + allocator.metric().chunkSize() / bufSize;
            List<ByteBuf> buffers = new ArrayList<>(bufsToAllocate);
            for (int i = 0; i < bufsToAllocate; ++i) {
                buffers.add(allocator.heapBuffer(bufSize, bufSize));
            }
            // release all buffers
            for (ByteBuf buffer : buffers) {
                buffer.release();
            }
            buffers.clear();
            eventsFlushed.await();
            assertEquals(0, eventsFlushed.getCount());
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void pooledShouldReuseTheSameChunk() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            final CountDownLatch eventsFlushed = new CountDownLatch(1);
            final AtomicInteger chunksAllocations = new AtomicInteger();
            stream.enable(AllocateChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME,
                    event -> {
                        chunksAllocations.incrementAndGet();
                        eventsFlushed.countDown();
                    });
            stream.startAsync();
            int bufSize = 16896;
            PooledByteBufAllocator allocator = newPooledAllocator(false);
            ByteBuf buf = allocator.heapBuffer(bufSize, bufSize);
            int bufPin = Math.toIntExact(allocator.pinnedHeapMemory());
            buf.release();
            int bufsPerChunk = allocator.metric().chunkSize() / bufPin;
            List<ByteBuf> buffers = new ArrayList<>(bufsPerChunk);
            for (int i = 0; i < bufsPerChunk - 2; ++i) {
                buffers.add(allocator.heapBuffer(bufSize, bufSize));
            }
            // we still have 2 available segments in the chunk, so we should not allocate a new one
            for (int i = 0; i < 128; ++i) {
                allocator.heapBuffer(bufSize, bufSize).release();
            }
            // release all buffers
            for (ByteBuf buffer : buffers) {
                buffer.release();
            }
            buffers.clear();
            eventsFlushed.await();
            assertEquals(1, chunksAllocations.get());
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void pooledJfrBufferAllocation() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            CompletableFuture<RecordedEvent> allocateFuture = new CompletableFuture<>();
            CompletableFuture<RecordedEvent> releaseFuture = new CompletableFuture<>();

            stream.enable(AllocateBufferEvent.class);
            stream.onEvent(AllocateBufferEvent.NAME, allocateFuture::complete);
            stream.enable(FreeBufferEvent.class);
            stream.onEvent(FreeBufferEvent.NAME, releaseFuture::complete);
            stream.startAsync();

            PooledByteBufAllocator alloc = newPooledAllocator(true);
            alloc.directBuffer(128).release();

            RecordedEvent allocate = allocateFuture.get();
            assertEquals(128, allocate.getInt("size"));
            assertEquals(128, allocate.getInt("maxFastCapacity"));
            assertEquals(Integer.MAX_VALUE, allocate.getInt("maxCapacity"));
            assertTrue(allocate.getBoolean("chunkPooled"));
            assertFalse(allocate.getBoolean("chunkThreadLocal"));
            assertTrue(allocate.getBoolean("direct"));

            RecordedEvent release = releaseFuture.get();
            assertEquals(128, release.getInt("size"));
            assertEquals(128, release.getInt("maxFastCapacity"));
            assertEquals(Integer.MAX_VALUE, release.getInt("maxCapacity"));
            assertTrue(release.getBoolean("direct"));
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void pooledJfrBufferAllocationThreadLocal() throws Exception {
        ByteBufAllocator alloc = newPooledAllocator(true);

        Callable<Void> allocateAndRelease = () -> {
            try (RecordingStream stream = new RecordingStream()) {
                CompletableFuture<RecordedEvent> allocateFuture = new CompletableFuture<>();
                CompletableFuture<RecordedEvent> releaseFuture = new CompletableFuture<>();

                // Prime the cache.
                alloc.directBuffer(128).release();

                stream.enable(AllocateBufferEvent.class);
                stream.onEvent(AllocateBufferEvent.NAME, allocateFuture::complete);
                stream.enable(FreeBufferEvent.class);
                stream.onEvent(FreeBufferEvent.NAME, releaseFuture::complete);
                stream.startAsync();

                // Allocate out of the cache.
                alloc.directBuffer(128).release();

                RecordedEvent allocate = allocateFuture.get();
                assertEquals(128, allocate.getInt("size"));
                assertEquals(128, allocate.getInt("maxFastCapacity"));
                assertEquals(Integer.MAX_VALUE, allocate.getInt("maxCapacity"));
                assertTrue(allocate.getBoolean("chunkPooled"));
                assertTrue(allocate.getBoolean("chunkThreadLocal"));
                assertTrue(allocate.getBoolean("direct"));

                RecordedEvent release = releaseFuture.get();
                assertEquals(128, release.getInt("size"));
                assertEquals(128, release.getInt("maxFastCapacity"));
                assertEquals(Integer.MAX_VALUE, release.getInt("maxCapacity"));
                assertTrue(release.getBoolean("direct"));
                return null;
            }
        };
        FutureTask<Void> task = new FutureTask<>(allocateAndRelease);
        FastThreadLocalThread thread = new FastThreadLocalThread(task);
        thread.start();
        task.get();
    }

    @SuppressWarnings("Since15")
    @Test
    public void adaptiveJfrChunkAllocation() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            CompletableFuture<RecordedEvent> allocateFuture = new CompletableFuture<>();

            stream.enable(AllocateChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME, allocateFuture::complete);
            stream.startAsync();

            AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, false);
            alloc.directBuffer(128).release();

            RecordedEvent allocate = allocateFuture.get();
            // A direct size class takes a segment and carves its chunk out of it: the segment is the event, a
            // region's or not.
            int segmentSize = AdaptiveByteBufAllocatorTest.directSegmentSize(alloc);
            // With shared slices, the event is the chunk's slices.
            boolean perSegment = segmentSize > 0 && !AdaptiveByteBufAllocatorTest.directSharesSlices(alloc);
            assertEquals(perSegment ? segmentSize : AdaptivePoolingAllocator.MIN_CHUNK_SIZE,
                    allocate.getInt("capacity"));
            assertEquals(segmentSize > 0, allocate.getBoolean("segment"));
            assertTrue(allocate.getBoolean("pooled"));
            assertFalse(allocate.getBoolean("threadLocal"));
            assertTrue(allocate.getBoolean("direct"));
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void adaptiveShouldCreateTwoChunks() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            final CountDownLatch eventsFlushed = new CountDownLatch(2);
            stream.enable(AllocateChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME,
                    event -> {
                        eventsFlushed.countDown();
                    });
            stream.startAsync();
            ByteBufAllocator allocator = newAdaptiveAllocator(false);
            int bufSize = 16896;
            int minSegmentsPerChunk = 32; // See AdaptivePoolingAllocator.SizeClassChunkController.
            int bufsToAllocate = 1 + minSegmentsPerChunk;
            List<ByteBuf> buffers = new ArrayList<>(bufsToAllocate);
            for (int i = 0; i < bufsToAllocate; ++i) {
                buffers.add(allocator.heapBuffer(bufSize, bufSize));
            }
            // release all buffers
            for (ByteBuf buffer : buffers) {
                buffer.release();
            }
            buffers.clear();
            eventsFlushed.await();
            assertEquals(0, eventsFlushed.getCount());
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void adaptiveShouldReuseTheSameChunk() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            final CountDownLatch eventsFlushed = new CountDownLatch(1);
            final AtomicInteger chunksAllocations = new AtomicInteger();
            stream.enable(AllocateChunkEvent.class);
            stream.onEvent(AllocateChunkEvent.NAME,
                    event -> {
                        chunksAllocations.incrementAndGet();
                        eventsFlushed.countDown();
                    });
            stream.startAsync();
            int bufSize = 16896;
            ByteBufAllocator allocator = newAdaptiveAllocator(false);
            List<ByteBuf> buffers = new ArrayList<>(32);
            for (int i = 0; i < 30; ++i) {
                buffers.add(allocator.heapBuffer(bufSize, bufSize));
            }
            // we still have 2 available segments in the chunk, so we should not allocate a new one
            for (int i = 0; i < 128; ++i) {
                allocator.heapBuffer(bufSize, bufSize).release();
            }
            // release all buffers
            for (ByteBuf buffer : buffers) {
                buffer.release();
            }
            buffers.clear();
            eventsFlushed.await();
            assertEquals(1, chunksAllocations.get());
        }
    }

    /**
     * A buffer above the size classes on a thread-local heap comes from a chunk of that heap: both events say so.
     */
    @SuppressWarnings("Since15")
    @Test
    public void adaptiveLargeBufferOnAThreadLocalHeapIsThreadLocalInBothEvents() throws Exception {
        Field lowMem = AdaptivePoolingAllocator.class.getDeclaredField("IS_LOW_MEM");
        lowMem.setAccessible(true);
        assumeFalse(lowMem.getBoolean(null), "low-memory mode has no thread-local heaps and pools no 512 KiB buffers");
        final int size = 512 * 1024;
        AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, true);
        // With shared slices the page store's event is the span's own slices.
        final boolean shared = AdaptiveByteBufAllocatorTest.directSharesSlices(alloc);
        Callable<Void> allocateAndRelease = () -> {
            try (RecordingStream stream = new RecordingStream()) {
                CompletableFuture<RecordedEvent> chunkFuture = new CompletableFuture<>();
                CompletableFuture<RecordedEvent> bufferFuture = new CompletableFuture<>();
                stream.enable(AllocateChunkEvent.class);
                stream.onEvent(AllocateChunkEvent.NAME, e -> {
                    int capacity = e.getInt("capacity");
                    if (shared ? e.getBoolean("segment") && capacity == size : capacity > size) {
                        chunkFuture.complete(e);
                    }
                });
                stream.enable(AllocateBufferEvent.class);
                stream.onEvent(AllocateBufferEvent.NAME, e -> {
                    if (e.getInt("size") == size) {
                        bufferFuture.complete(e);
                    }
                });
                stream.startAsync();

                alloc.directBuffer(size, size).release();

                assertTrue(chunkFuture.get(10, TimeUnit.SECONDS).getBoolean("threadLocal"), "the chunk event");
                assertTrue(bufferFuture.get(10, TimeUnit.SECONDS).getBoolean("chunkThreadLocal"), "the buffer event");
                return null;
            }
        };
        FutureTask<Void> task = new FutureTask<>(allocateAndRelease);
        FastThreadLocalThread thread = new FastThreadLocalThread(task);
        thread.start();
        task.get();
    }

    @SuppressWarnings("Since15")
    @Test
    public void adaptiveJfrBufferAllocation() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            CompletableFuture<RecordedEvent> allocateFuture = new CompletableFuture<>();
            CompletableFuture<RecordedEvent> releaseFuture = new CompletableFuture<>();

            stream.enable(AllocateBufferEvent.class);
            stream.onEvent(AllocateBufferEvent.NAME, allocateFuture::complete);
            stream.enable(FreeBufferEvent.class);
            stream.onEvent(FreeBufferEvent.NAME, releaseFuture::complete);
            stream.startAsync();

            AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, false);
            alloc.directBuffer(128).release();

            RecordedEvent allocate = allocateFuture.get();
            assertEquals(128, allocate.getInt("size"));
            assertEquals(128, allocate.getInt("maxFastCapacity"));
            assertEquals(Integer.MAX_VALUE, allocate.getInt("maxCapacity"));
            assertTrue(allocate.getBoolean("chunkPooled"));
            assertFalse(allocate.getBoolean("chunkThreadLocal"));
            assertTrue(allocate.getBoolean("direct"));

            RecordedEvent release = releaseFuture.get();
            assertEquals(128, release.getInt("size"));
            assertEquals(128, release.getInt("maxFastCapacity"));
            assertEquals(Integer.MAX_VALUE, release.getInt("maxCapacity"));
            assertTrue(release.getBoolean("direct"));
        }
    }

    @SuppressWarnings("Since15")
    @Test
    public void adaptiveJfrBufferAllocationThreadLocal() throws Exception {
        ByteBufAllocator alloc = new AdaptiveByteBufAllocator(true, true);

        Callable<Void> allocateAndRelease = () -> {
            try (RecordingStream stream = new RecordingStream()) {
                CompletableFuture<RecordedEvent> allocateFuture = new CompletableFuture<>();
                CompletableFuture<RecordedEvent> releaseFuture = new CompletableFuture<>();

                // Prime the cache.
                alloc.directBuffer(128).release();

                stream.enable(AllocateBufferEvent.class);
                stream.onEvent(AllocateBufferEvent.NAME, allocateFuture::complete);
                stream.enable(FreeBufferEvent.class);
                stream.onEvent(FreeBufferEvent.NAME, releaseFuture::complete);
                stream.startAsync();

                // Allocate out of the cache.
                alloc.directBuffer(128).release();

                RecordedEvent allocate = allocateFuture.get();
                assertEquals(128, allocate.getInt("size"));
                assertEquals(128, allocate.getInt("maxFastCapacity"));
                assertEquals(Integer.MAX_VALUE, allocate.getInt("maxCapacity"));
                assertTrue(allocate.getBoolean("chunkPooled"));
                assertTrue(allocate.getBoolean("chunkThreadLocal"));
                assertTrue(allocate.getBoolean("direct"));

                RecordedEvent release = releaseFuture.get();
                assertEquals(128, release.getInt("size"));
                assertEquals(128, release.getInt("maxFastCapacity"));
                assertEquals(Integer.MAX_VALUE, release.getInt("maxCapacity"));
                assertTrue(release.getBoolean("direct"));
                return null;
            }
        };
        FutureTask<Void> task = new FutureTask<>(allocateAndRelease);
        FastThreadLocalThread thread = new FastThreadLocalThread(task);
        thread.start();
        task.get();
    }
}
