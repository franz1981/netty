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

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The arena's cold-boundary JFR events: one block switch and one iteration must be recorded. */
@Timeout(30)
@EnabledForJreRange(min = JRE.JAVA_17)
@Isolated
@SuppressWarnings("Since15")
public class CycleArenaJfrTest {

    @Test
    public void blockSwitchAndIterationAreRecorded() throws Exception {
        try (RecordingStream stream = new RecordingStream()) {
            CompletableFuture<RecordedEvent> switchEvent = new CompletableFuture<RecordedEvent>();
            CompletableFuture<RecordedEvent> iterationEvent = new CompletableFuture<RecordedEvent>();
            CompletableFuture<RecordedEvent> outsideEvent = new CompletableFuture<RecordedEvent>();
            stream.enable(ArenaEvents.BlockSwitch.class);
            stream.enable(ArenaEvents.Iteration.class);
            stream.enable(ArenaEvents.AllocationOutside.class);
            stream.onEvent(ArenaEvents.BlockSwitch.NAME, switchEvent::complete);
            stream.onEvent(ArenaEvents.Iteration.NAME, iterationEvent::complete);
            stream.onEvent(ArenaEvents.AllocationOutside.NAME, outsideEvent::complete);
            stream.startAsync();

            CycleArenaAllocator alloc = new CycleArenaAllocator();
            try {
                // Above the cap: one ArenaAllocationOutside.
                alloc.heapBuffer(CycleArenaAllocator.CAP + 1).release();
                // Fill the current block so that it is retired: one ArenaBlockSwitch.
                List<ByteBuf> live = new ArrayList<ByteBuf>();
                while (space(alloc).blockCount() < 2) {
                    live.add(alloc.heapBuffer(CycleArenaAllocator.CAP));
                }
                for (ByteBuf b : live) {
                    b.release();
                }
                // Enough hooks to pass the period: one ArenaIteration.
                for (int i = 0; i <= CycleArenaAllocator.JFR_PERIOD; i++) {
                    alloc.heapBuffer(64).release();
                    alloc.runHookForTest();
                }

                RecordedEvent blockSwitch = switchEvent.get();
                assertEquals("HEAP", blockSwitch.getString("space"));
                assertTrue(blockSwitch.getInt("bytesBumped") > 0);
                assertTrue(blockSwitch.getInt("allocations") > 0);

                RecordedEvent iteration = iterationEvent.get();
                assertEquals("HEAP", iteration.getString("space"));


                RecordedEvent outside = outsideEvent.get();
                assertEquals(CycleArenaAllocator.REASON_CAP, outside.getString("reason"));
            } finally {
                alloc.removeForTest();
            }
        }
    }

    private static CycleArenaAllocator.Space space(CycleArenaAllocator alloc) {
        return alloc.arenaForTest().heap;
    }
}
