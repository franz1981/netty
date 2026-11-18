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
import io.netty.util.internal.OutOfDirectMemoryError;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static io.netty.incubator.allocator.BufferChecks.releaseAll;
import static io.netty.incubator.allocator.BufferChecks.stamp;
import static io.netty.incubator.allocator.BufferChecks.verify;
import static io.netty.incubator.allocator.PageStoreAllocators.assumeThreadLocalHeaps;
import static io.netty.incubator.allocator.PageStoreAllocators.pooledBuffers;
import static io.netty.incubator.allocator.PageStoreAllocators.useUntilQuiet;
import static io.netty.incubator.allocator.PageStoreAllocators.mmapWhereLinked;
import static io.netty.incubator.allocator.PageStoreAllocators.newAllocator;
import static io.netty.incubator.allocator.PageStoreAllocators.directRegionsAreMapped;
import static io.netty.incubator.allocator.PageStoreAllocators.wholeBlocks;
import static io.netty.incubator.allocator.TestThreads.runOnThread;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The allocator's direct memory against Netty's direct memory counter, the one {@code -XX:MaxDirectMemorySize}
 * limits: charged by what the allocator uses, credited by its close, and an allocation past the limit throws and
 * leaves the allocator usable. The counter is the JVM's: it also moves whenever the finalizer frees another test's
 * allocator, which only credits it, so it is checked one way: charged by no more than the allocator reports, and
 * back to its base or below once the allocator is closed.
 */
@Isolated("Fills PlatformDependent's direct memory counter past its limit, which concurrent allocations would hit")
final class DirectMemoryChargeTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();
    private final MockTicker ticker = Ticker.newMockTicker();

    @BeforeEach
    void counted() {
        assumeTrue(PlatformDependent.usedDirectMemory() >= 0, "the direct memory counter is off");
    }

    private AdaptiveByteBufAllocator allocator(boolean mapped) {
        if (mapped) {
            assumeTrue(directRegionsAreMapped(), "mmap regions are not linked in this execution");
            return newAllocator(closer, ticker, Geometry.DEFAULTS, mmapWhereLinked());
        }
        return newAllocator(closer, ticker, Geometry.DEFAULTS, null);
    }

    /** The counter rose by {@code bytes} at most since {@code base}. */
    private static void assertChargedAtMost(long base, long bytes, String message) {
        long charged = PlatformDependent.usedDirectMemory() - base;
        assertTrue(charged <= bytes, message + ": charged " + charged + ", the allocator reports " + bytes);
    }

    /** The counter is back to {@code base} or below. */
    private static void assertCredited(long base) {
        long used = PlatformDependent.usedDirectMemory();
        assertTrue(used <= base, "not credited: " + (used - base) + " bytes above the base");
    }

    /**
     * The counter is never charged above what the allocator reports as used, whether the regions are {@code malloc}'d
     * (charged by their allocation) or mapped (by the slices claimed), also when released memory serves the same
     * buffers again, and the close does not leave it charged. Both bounds are one-sided on purpose: a finalizer of an
     * allocator another test leaked may credit the JVM-wide counter at any time, so the counter may be below what is
     * used, and an exact bound could fail on correct code. A double credit is not covered by any test.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void directMemoryIsNeverChargedAboveWhatIsUsedNorLeftChargedByTheClose(boolean mapped) {
        long base = PlatformDependent.usedDirectMemory();
        AdaptiveByteBufAllocator alloc = allocator(mapped);
        ByteBuf small = alloc.directBuffer(1024);
        ByteBuf medium = alloc.directBuffer(100_000);
        long used = alloc.usedDirectMemory();
        assertTrue(used > 0);
        assertChargedAtMost(base, used, "two buffers");
        small.release();
        medium.release();
        ByteBuf again = alloc.directBuffer(1024);
        ByteBuf againMedium = alloc.directBuffer(100_000);
        assertEquals(used, alloc.usedDirectMemory(), "released memory serves the same buffers");
        assertChargedAtMost(base, used, "the same two buffers again");
        again.release();
        againMedium.release();
        alloc.close();
        assertEquals(0, alloc.usedDirectMemory());
        assertCredited(base);
    }

    /**
     * With the counter full, an allocation that needs memory throws {@link OutOfDirectMemoryError} from
     * {@code directBuffer()} and changes nothing; released memory still serves; once the limit has room again, the
     * allocator allocates, and the bytes of the buffers it kept are intact.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anAllocationPastTheLimitThrowsAndTheAllocatorRecovers(boolean mapped) {
        long base = PlatformDependent.usedDirectMemory();
        AdaptiveByteBufAllocator alloc = allocator(mapped);
        int block = Geometry.DEFAULTS.direct.segmentSize;
        ByteBuf kept = alloc.directBuffer(1024);
        stamp(kept, 1);
        ByteBuf blockBuffer = alloc.directBuffer(block);
        blockBuffer.release();
        long used = alloc.usedDirectMemory();
        long filler = overfill();
        try {
            ByteBuf reused = alloc.directBuffer(block);
            assertEquals(used, alloc.usedDirectMemory(), "released memory serves, and charges nothing");
            assertThrows(OutOfDirectMemoryError.class, () -> alloc.directBuffer(block));
            assertEquals(used, alloc.usedDirectMemory(), "the failed allocation took nothing");
            reused.release();
        } finally {
            credit(filler);
        }
        ByteBuf more = alloc.directBuffer(block);
        ByteBuf moreAgain = alloc.directBuffer(block);
        stamp(more, 2);
        verify(more, 2);
        verify(kept, 1);
        assertChargedAtMost(base, alloc.usedDirectMemory(), "after the recovery");
        more.release();
        moreAgain.release();
        kept.release();
        alloc.close();
        assertCredited(base);
    }

    /**
     * Memory the allocator purged is credited to the counter, so the counter is never above what the allocator reports
     * once it has purged; and with the limit full, taking purged memory again needs a charge it cannot get, so it
     * throws {@link OutOfDirectMemoryError}, and succeeds once the limit has room again.
     */
    @Test
    void purgedMemoryIsCreditedAndChargedAgain() {
        long base = PlatformDependent.usedDirectMemory();
        AdaptiveByteBufAllocator alloc = allocator(true);
        int block = Geometry.DEFAULTS.direct.segmentSize;
        List<ByteBuf> blocks = wholeBlocks(alloc, block, 4);
        long used = alloc.usedDirectMemory();
        assertChargedAtMost(base, used, "four blocks");
        releaseAll(blocks);
        useUntilQuiet(alloc, ticker, pooledBuffers(alloc, true));
        long afterIdle = alloc.usedDirectMemory();
        assertTrue(afterIdle < used, "the allocator purged nothing");
        assertChargedAtMost(base, afterIdle, "after the purge");
        long filler = overfill();
        try {
            assertThrows(OutOfDirectMemoryError.class, () -> alloc.directBuffer(block));
            assertEquals(afterIdle, alloc.usedDirectMemory(), "the failed allocation took nothing");
        } finally {
            credit(filler);
        }
        ByteBuf again = alloc.directBuffer(block);
        again.release();
        alloc.close();
        assertCredited(base);
    }

    /**
     * A run of blocks whose second block cannot be charged leaves the first one usable, and a block whose charge fails
     * goes back to the store: with the limit full, a buffer of three blocks throws, a buffer of one block that needs
     * no charge is served, and once the limit has room all the other blocks of the region are still free, so the
     * whole test maps one region.
     */
    @Test
    void aBlockRunThatCannotBeChargedGivesEveryBlockBack(@TempDir Path dir) throws Exception {
        assumeTrue(PlatformDependent.isJfrEnabled(), "JFR is not available");
        long base = PlatformDependent.usedDirectMemory();
        try (JfrEvents jfr = new JfrEvents(dir, false)) {
            AdaptiveByteBufAllocator alloc = allocator(true);
            PageStoreConfig config = Geometry.DEFAULTS.direct;
            int block = config.segmentSize;
            ByteBuf kept = alloc.directBuffer(1024);
            stamp(kept, 1);
            // The block of the released buffer has memory behind it: taking it again needs no charge.
            alloc.directBuffer(block).release();
            long filler = overfill();
            List<ByteBuf> held = new ArrayList<ByteBuf>();
            try {
                // The first block of the run needs no charge, the second one's fails.
                assertThrows(OutOfDirectMemoryError.class, () -> alloc.directBuffer(3 * block));
                ByteBuf served = alloc.directBuffer(block);
                served.release();
            } finally {
                credit(filler);
            }
            try {
                held.addAll(wholeBlocks(alloc, block, config.segmentsPerRegion() - 1));
                verify(kept, 1);
                jfr.stop();
                assertEquals(1, jfr.events(PageStoreMapEvent.NAME).size(), "the regions mapped");
            } finally {
                releaseAll(held);
                kept.release();
            }
            alloc.close();
            assertCredited(base);
        }
    }

    /**
     * A span whose charge fails goes back to the store: with the limit full, 1 KiB buffers are served from the memory
     * the allocator has until one needs more and throws; a buffer released then serves the next one, the allocator
     * goes on serving once the limit has room, and every buffer kept its bytes. Both kinds of region. With mapped
     * regions, once every buffer is released the blocks of the region serve whole-block buffers, and the block of the
     * 1 KiB chunks all the 1 KiB buffers it has room for: the whole test maps one region, which it does not if the
     * refused span was lost.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aSizeClassThatCannotBeChargedFailsAndGoesOnServing(boolean mapped, @TempDir Path dir) throws Exception {
        // Only the mapped case reads events.
        assumeTrue(!mapped || PlatformDependent.isJfrEnabled(), "JFR is not available");
        long base = PlatformDependent.usedDirectMemory();
        try (JfrEvents jfr = mapped ? new JfrEvents(dir, false) : null) {
            AdaptiveByteBufAllocator alloc = allocator(mapped);
            PageStoreConfig config = Geometry.DEFAULTS.direct;
            SizeClassTable table = new SizeClassTable(config);
            int index = SizeClassTable.sizeClassIndex(1024);
            int cap = table.slots[index] * config.slicesPerSegment() + 1;
            List<ByteBuf> held = new ArrayList<ByteBuf>();
            try {
                // The first buffer's region exists.
                held.add(alloc.directBuffer(1024));
                stamp(held.get(0), 1);
                long filler = overfill();
                try {
                    boolean thrown = false;
                    for (int i = 0; i < cap && !thrown; i++) {
                        try {
                            ByteBuf buf = alloc.directBuffer(1024);
                            held.add(buf);
                            stamp(buf, held.size());
                        } catch (OutOfDirectMemoryError e) {
                            thrown = true;
                        }
                    }
                    assertTrue(thrown, "no allocation was refused in " + cap + " buffers");
                    ByteBuf last = held.remove(held.size() - 1);
                    verify(last, held.size() + 1);
                    last.release();
                    ByteBuf replaced = alloc.directBuffer(1024);
                    held.add(replaced);
                    stamp(replaced, held.size());
                } finally {
                    credit(filler);
                }
                ByteBuf more = alloc.directBuffer(1024);
                held.add(more);
                stamp(more, held.size());
                for (int i = 0; i < held.size(); i++) {
                    verify(held.get(i), i + 1);
                }
                if (mapped) {
                    releaseAll(held);
                    held.addAll(wholeBlocks(alloc, config.segmentSize, config.segmentsPerRegion() - 1));
                    int inBlock = config.slicesPerSegment() / table.chunkSlices[index] * table.slots[index];
                    for (int i = 0; i < inBlock; i++) {
                        held.add(alloc.directBuffer(1024));
                    }
                    jfr.stop();
                    assertEquals(1, jfr.events(PageStoreMapEvent.NAME).size(), "the regions mapped");
                }
            } finally {
                releaseAll(held);
            }
            alloc.close();
            assertEquals(0, alloc.usedDirectMemory());
            assertCredited(base);
        }
    }

    /**
     * A buffer whose growth to a span on another thread's heap cannot be charged stays as it was: its bytes are
     * intact, and releasing it on that thread, which is not its heap's owner, gives its slot back to its own heap.
     * What fails without the fix is the assertion in {@code SizeClassedChunk.release}, so it needs the assertions
     * the build enables ({@code -ea:io.netty...}).
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aGrowthThatCannotBeChargedLeavesTheBufferAsItWas(boolean mapped) throws Exception {
        assumeThreadLocalHeaps();
        long base = PlatformDependent.usedDirectMemory();
        AdaptiveByteBufAllocator alloc = allocator(mapped);
        int block = Geometry.DEFAULTS.direct.segmentSize;
        ByteBuf[] buf = new ByteBuf[1];
        runOnThread(true, () -> {
            buf[0] = alloc.directBuffer(1024);
            stamp(buf[0], 1);
        });
        runOnThread(true, () -> {
            long filler = overfill();
            try {
                assertThrows(OutOfDirectMemoryError.class, () -> buf[0].capacity(block));
            } finally {
                credit(filler);
            }
            verify(buf[0], 1);
            buf[0].release();
        });
        alloc.close();
        assertCredited(base);
    }

    /**
     * Charges the limit on top of what is used, unchecked: frees elsewhere credit only what they charged, so the
     * counter stays past the limit until {@link #credit}.
     */
    private static long overfill() {
        long filler = PlatformDependent.maxDirectMemory();
        // Charged as a negative credit: incrementMemoryCounter throws past the limit.
        for (long left = filler; left > 0; left -= Integer.MAX_VALUE) {
            PlatformDependent.decrementMemoryCounter((int) -Math.min(left, Integer.MAX_VALUE));
        }
        return filler;
    }

    private static void credit(long bytes) {
        for (long left = bytes; left > 0; left -= Integer.MAX_VALUE) {
            PlatformDependent.decrementMemoryCounter((int) Math.min(left, Integer.MAX_VALUE));
        }
    }
}
