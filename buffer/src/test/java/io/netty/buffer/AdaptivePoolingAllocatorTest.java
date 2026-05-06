/*
 * Copyright 2024 The Netty Project
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptivePoolingAllocatorTest implements Supplier<String> {
    private int i;

    @BeforeEach
    void setUp() {
        i = 0;
    }

    @Override
    public String get() {
        return "i = " + i;
    }

    @Test
    void sizeBucketComputations() throws Exception {
        assertSizeBucket(0, 8 * 1024);
        assertSizeBucket(1, 16 * 1024);
        assertSizeBucket(2, 32 * 1024);
        assertSizeBucket(3, 64 * 1024);
        assertSizeBucket(4, 128 * 1024);
        assertSizeBucket(5, 256 * 1024);
        assertSizeBucket(6, 512 * 1024);
        assertSizeBucket(7, 1024 * 1024);
        // The sizeBucket function will be used for sizes up to 10 MiB
        assertSizeBucket(7, 2 * 1024 * 1024);
        assertSizeBucket(7, 3 * 1024 * 1024);
        assertSizeBucket(7, 4 * 1024 * 1024);
        assertSizeBucket(7, 5 * 1024 * 1024);
        assertSizeBucket(7, 6 * 1024 * 1024);
        assertSizeBucket(7, 7 * 1024 * 1024);
        assertSizeBucket(7, 8 * 1024 * 1024);
        assertSizeBucket(7, 9 * 1024 * 1024);
        assertSizeBucket(7, 10 * 1024 * 1024);
    }

    private void assertSizeBucket(int expectedSizeBucket, int maxSizeIncluded) {
        for (; i <= maxSizeIncluded; i++) {
            assertEquals(expectedSizeBucket, AdaptivePoolingAllocator.sizeBucket(i), this);
        }
    }

    /**
     * Verify the default values for the purge-strategy configuration properties.
     */
    @Test
    void purgeConfigDefaults() {
        assertEquals(1024L, AdaptivePoolingAllocator.CHUNK_REUSE_QUEUE_POLLS_PER_PURGE,
                "Default polls-per-purge should be 1024");
        assertEquals(3, AdaptivePoolingAllocator.CHUNK_REUSE_QUEUE_PURGE_THRESHOLD,
                "Default purge epoch threshold should be 3");
        assertTrue(AdaptivePoolingAllocator.CHUNK_REUSE_QUEUE_CAPACITY >= 8,
                "Queue capacity should be at least 8");
    }

    /**
     * Verifies that fully-free chunks in the shared cache are pruned after surviving
     * {@link AdaptivePoolingAllocator#CHUNK_REUSE_QUEUE_PURGE_THRESHOLD} + 1 consecutive purge-scan cycles
     * without being used.
     * <p>
     * The test uses the package-private {@code setPurgeBudgetForTesting()} helper to trigger scans
     * at will, rather than waiting for the production cadence of 1024 polls.
     */
    @Test
    void purgeScanShouldRemoveFullyFreeChunksAfterThreshold() {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(false);
        AdaptivePoolingAllocator heapAlloc = allocator.heapAllocator();

        // Allocate enough buffers to fill two full chunks so that, on release,
        // at least one recycled chunk ends up in the shared cache queue (the second
        // one cannot fit in magazine.nextInLine once the first one already occupies it).
        //
        // MIN_CHUNK_SIZE = 128 KiB, buffer size = 8 KiB  → 16 buffers per chunk.
        // Round 1 (bufs1, 32 buffers, 2 chunks A + B):
        //   - chunk A fills nextInLine on recycle
        //   - chunk B ends up in the shared cache queue
        int minChunkSize = 128 * 1024;
        int bufSize = 8 * 1024;
        int buffersPerChunk = minChunkSize / bufSize; // 16
        List<ByteBuf> bufs1 = new ArrayList<>();
        for (int j = 0; j < buffersPerChunk * 2; j++) {
            bufs1.add(allocator.heapBuffer(bufSize));
        }
        for (ByteBuf b : bufs1) {
            b.release();
        }
        bufs1.clear();

        // After releasing, at least one chunk should be cached.
        int cachedBefore = heapAlloc.cachedChunkCount();
        assertTrue(cachedBefore > 0, "Expected at least one chunk in the cache after allocate/release. " +
                "cachedBefore=" + cachedBefore);

        long memBefore = allocator.usedHeapMemory();

        // Trigger PURGE_EPOCH_THRESHOLD + 1 purge scans.
        // Each scan increments purgeEpoch for fully-free chunks; once purgeEpoch > threshold, the chunk
        // is deallocated.  We need threshold+1 scans for the first pruning to happen.
        int scansNeeded = AdaptivePoolingAllocator.CHUNK_REUSE_QUEUE_PURGE_THRESHOLD + 1;
        for (int scan = 0; scan < scansNeeded; scan++) {
            // Force the next pollChunk() to trigger a purge scan
            heapAlloc.setPurgeBudgetForTesting(0L);
            // Allocate then immediately release a buffer; this drives a pollChunk() call which
            // will discover budget==0 and run the purge scan.  The buffer allocation itself may use
            // a chunk already in magazine.nextInLine, but the poll will still happen for the queue.
            // To ensure pollChunk() is actually called, we run enough allocations to exhaust
            // any magazine-local chunks.
            for (int j = 0; j < buffersPerChunk + 1; j++) {
                ByteBuf tmp = allocator.heapBuffer(bufSize);
                tmp.release();
            }
        }

        long memAfter = allocator.usedHeapMemory();
        assertTrue(memAfter < memBefore,
                "Memory should decrease after " + scansNeeded + " purge scan(s). " +
                "Before=" + memBefore + ", After=" + memAfter);
    }
}
