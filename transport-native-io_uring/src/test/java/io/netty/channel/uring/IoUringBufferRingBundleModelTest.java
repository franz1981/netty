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
package io.netty.channel.uring;

import io.netty.channel.unix.Buffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Random;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Model check of the RECVSEND_BUNDLE walk: a simulated kernel consumes consecutive ring SLOTS (reading the bid that
 * IoUringBufferRing wrote into the registered ring memory), emits bundle completions and ENOBUFS, and the Netty
 * side replays exactly the AbstractIoUringStreamChannel loop (snapshot, useBuffer, nextBid). Every predicted next
 * bid is compared with the bid the kernel really took from the next slot.
 * Three predictors are counted: the PR (snapshot before useBuffer + modulo), "after" (modulo with the value read
 * after useBuffer) and "old" (4.2: mask with the value read after useBuffer).
 */
public class IoUringBufferRingBundleModelTest {
    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IoUring.isAvailable());
        assumeTrue(IoUring.isRegisterBufferRingSupported());
    }

    @ParameterizedTest
    @CsvSource({"16, 4, false", "16, 8, false", "64, 4, false", "1024, 512, false",
            "16, 4, true", "16, 8, true", "64, 4, true"})
    public void predictedNextBidMatchesRingSlot(short entries, int batchSize, boolean batchAllocation) {
        int entrySize = 8;
        long prMismatch = 0;
        long afterMismatch = 0;
        long oldMismatch = 0;
        long predictions = 0;
        long growthsInsideBundle = 0;
        for (long seed = 0; seed < 200; seed++) {
            Random rnd = new Random(seed);
            RingBuffer ringBuffer = Native.createRingBuffer(8, 0);
            try {
                int ringFd = ringBuffer.fd();
                long addr = Native.ioUringRegisterBufRing(ringFd, entries, (short) 1, 0);
                assumeThat(addr).isGreaterThan(0);
                ByteBuffer ringMem = Buffer.wrapMemoryAddressWithNativeOrder(addr,
                        Native.ioUringBufRingSize(entries));
                IoUringBufferRing ring = new IoUringBufferRing(ringFd, ringMem, entries, batchSize, (short) 1,
                        false, new IoUringFixedBufferRingAllocator(entrySize), batchAllocation);
                try {
                    ring.initialize();
                    int mask = entries - 1;
                    short head = 0;
                    // Completions produced by the "kernel" and not yet processed by the "event loop".
                    // Each entry: the bids the kernel really consumed, in slot order; empty array = ENOBUFS.
                    ArrayDeque<short[]> cqes = new ArrayDeque<short[]>();
                    for (int step = 0; step < 400; step++) {
                        if (rnd.nextBoolean()) {
                            short tail = ringMem.getShort(Native.IO_URING_BUFFER_RING_TAIL);
                            int posted = (tail - head) & 0xFFFF;
                            if (posted == 0) {
                                cqes.add(new short[0]);
                            } else {
                                int n = 1 + rnd.nextInt(posted);
                                short[] bids = new short[n];
                                for (int i = 0; i < n; i++) {
                                    int slot = (head + i) & mask;
                                    bids[i] = ringMem.getShort(Native.SIZEOF_IOURING_BUF * slot
                                            + Native.IOURING_BUFFER_OFFSETOF_BID);
                                }
                                head += n;
                                cqes.add(bids);
                            }
                        } else if (!cqes.isEmpty()) {
                            short[] bids = cqes.poll();
                            if (bids.length == 0) {
                                ring.expand();
                                continue;
                            }
                            short bid = bids[0];
                            int read = bids.length * entrySize;
                            for (int i = 0; ; i++) {
                                int snapshot = ring.allocatedBuffers();
                                ring.useBuffer(bid, read, false).release();
                                read -= entrySize;
                                if (read == 0) {
                                    break;
                                }
                                int after = ring.allocatedBuffers();
                                if (after != snapshot) {
                                    growthsInsideBundle++;
                                }
                                short expected = bids[i + 1];
                                predictions++;
                                if (ring.nextBid(bid, snapshot) != expected) {
                                    prMismatch++;
                                }
                                if (ring.nextBid(bid, after) != expected) {
                                    afterMismatch++;
                                }
                                if ((short) ((bid + 1) & after - 1) != expected) {
                                    oldMismatch++;
                                }
                                // Follow what the kernel really did so the model stays in sync.
                                bid = expected;
                            }
                        }
                    }
                } finally {
                    ring.close();
                }
            } finally {
                ringBuffer.close();
            }
        }
        System.err.println("BUNDLE-MODEL entries=" + entries + " batch=" + batchSize + " batchAlloc="
                + batchAllocation + " predictions=" + predictions + " growthsInsideBundle=" + growthsInsideBundle
                + " mismatches: pr=" + prMismatch + " after=" + afterMismatch + " old=" + oldMismatch);
        assertTrue(predictions > 0);
        assertEquals(0, prMismatch);
    }
}
