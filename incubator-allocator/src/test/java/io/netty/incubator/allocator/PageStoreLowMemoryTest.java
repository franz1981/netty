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
import io.netty.util.internal.SystemPropertyUtil;
import jdk.jfr.consumer.RecordedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.List;

import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Low-memory mode, which is read when the class loads: these tests run in the {@code test-low-memory} executions of
 * the module, with {@code disableThreadLocalMagazinesOnLowMemory} at its default and set to false, and are skipped
 * elsewhere.
 */
@EnabledForJreRange(min = JRE.JAVA_11)
@Isolated("A recording takes the page store events of every thread")
class PageStoreLowMemoryTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    @TempDir
    Path dir;

    @BeforeEach
    void onlyInALowMemoryExecution() {
        assumeTrue(Boolean.getBoolean("pagestore.test.lowMemoryExecution"),
                "runs in the test-low-memory executions only");
        assertTrue(AdaptivePoolingAllocator.IS_LOW_MEM, "the execution sets io.netty.allocator.pagestore.lowMemory");
    }

    /** A FastThreadLocalThread allocates from a heap of its own only when the property keeps per-thread heaps. */
    @Test
    void aFastThreadLocalThreadHasAHeapOfItsOwnOnlyWhenThePropertyKeepsIt() throws Exception {
        assumeTrue(PlatformDependent.isJfrEnabled(), "JFR is not available");
        boolean kept = !SystemPropertyUtil.getBoolean(
                "io.netty.allocator.pagestore.disableThreadLocalMagazinesOnLowMemory", true);
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            final AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.DEFAULTS, null);
            Thread owner = jfr.runOnThread(true, () -> alloc.directBuffer(1024).release());
            jfr.stop();
            List<RecordedEvent> allocated = jfr.eventsOf(AllocateBufferEvent.NAME, owner);
            assertEquals(1, allocated.size());
            assertTrue(allocated.get(0).getBoolean("chunkPooled"));
            assertEquals(kept, allocated.get(0).getBoolean("chunkThreadLocal"));
        }
    }

    /** Only the size classes up to 16896 bytes are pooled: one byte more is a buffer of its own, of its exact size. */
    @Test
    void aBufferAboveTheLowMemorySizeClassesIsAllocatedOnItsOwn() {
        AdaptiveByteBufAllocator alloc = newAllocator(closer, ticker, Geometry.DEFAULTS, null);
        ByteBuf own = alloc.directBuffer(16897);
        assertEquals(16897, alloc.usedDirectMemory());
        own.release();
        assertEquals(0, alloc.usedDirectMemory());
        ByteBuf pooled = alloc.directBuffer(16896);
        assertEquals(Geometry.DEFAULTS.direct.segmentSize, alloc.usedDirectMemory(), "a region of one block");
        pooled.release();
    }
}
