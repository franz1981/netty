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

import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;

/**
 * EXPERIMENT (event-loop arena): the JFR events of the arena, modelled on the JDK's TLAB events. They are
 * emitted only on cold boundaries - a block switch, the end-of-iteration hook, a delegate allocation, a
 * confinement violation - so the bump and release paths carry no event instruction at all, exactly as
 * {@code jdk.ObjectAllocationInNewTLAB} carries none in the TLAB fast path.
 * <p>
 * All of them are disabled by default, like {@link AllocateBufferEvent}. The per-buffer
 * {@code AllocateBuffer}/{@code FreeBuffer}/{@code ReallocateBuffer} events stay as the expensive mode.
 */
final class ArenaEvents {

    private ArenaEvents() {
    }

    /**
     * The analogue of {@code jdk.ObjectAllocationInNewTLAB}: the current block is retired and another one
     * (or the delegate) takes over.
     */
    @SuppressWarnings("Since15")
    @Label("Arena Block Switch")
    @Name(BlockSwitch.NAME)
    @Description("An event-loop arena retired its current block")
    static final class BlockSwitch extends AbstractAllocatorEvent {
        static final String NAME = "io.netty.ArenaBlockSwitch";
        private static final BlockSwitch INSTANCE = new BlockSwitch();

        static boolean isEventEnabled() {
            return INSTANCE.isEnabled();
        }

        @DataAmount
        @Description("Bytes bump-allocated in the retired block")
        public int bytesBumped;
        @Description("Allocations served by the retired block")
        public int allocations;
        @Description("Buffers still live in the retired block")
        public int liveAtRetire;
        @Description("The retired block's id")
        public int blockId;
        @Description("What served the request that retired it: REUSE, GROWTH or DELEGATE")
        public String next;
        @Description("HEAP or DIRECT")
        public String space;
    }

    /** The analogue of {@code jdk.ObjectAllocationOutsideTLAB}: one allocation the arena did not serve. */
    @SuppressWarnings("Since15")
    @Label("Arena Allocation Outside")
    @Name(AllocationOutside.NAME)
    @Description("An allocation an event-loop arena handed to the delegate allocator")
    static final class AllocationOutside extends AbstractAllocatorEvent {
        static final String NAME = "io.netty.ArenaAllocationOutside";
        private static final AllocationOutside INSTANCE = new AllocationOutside();

        static boolean isEventEnabled() {
            return INSTANCE.isEnabled();
        }

        @DataAmount
        @Description("Requested capacity")
        public int size;
        @Description("CAP, BOUND, OBJECTS or OFF_LOOP")
        public String reason;
        @Description("HEAP or DIRECT")
        public String space;
    }

    /**
     * The analogue of {@code jdk.ObjectAllocationSample}: taken only at a block switch or at the hook, with
     * the bytes bumped since the last sample as its weight. The period is manual
     * ({@code -Darena.jfr.period}): the JFR API this module compiles against has no throttle annotation.
     */
    @SuppressWarnings("Since15")
    @Label("Arena Allocation Sample")
    @Name(AllocationSample.NAME)
    @Description("Sampled bump-allocation weight of an event-loop arena")
    static final class AllocationSample extends AbstractAllocatorEvent {
        static final String NAME = "io.netty.ArenaAllocationSample";
        private static final AllocationSample INSTANCE = new AllocationSample();

        static boolean isEventEnabled() {
            return INSTANCE.isEnabled();
        }

        @DataAmount
        @Description("Bytes bump-allocated since the previous sample")
        public long weight;
        @Description("Allocations since the previous sample")
        public long allocations;
        @Description("HEAP or DIRECT")
        public String space;
    }

    /** One end-of-iteration hook, every {@code period} hooks. */
    @SuppressWarnings("Since15")
    @Label("Arena Iteration")
    @Name(Iteration.NAME)
    @Description("An event-loop arena closed an iteration")
    static final class Iteration extends AbstractAllocatorEvent {
        static final String NAME = "io.netty.ArenaIteration";
        private static final Iteration INSTANCE = new Iteration();

        static boolean isEventEnabled() {
            return INSTANCE.isEnabled();
        }

        @Description("Blocks the hook reset")
        public int blocksReset;
        @Description("Blocks still pinned at the hook")
        public int pinned;
        @DataAmount
        @Description("Bytes bump-allocated since the previous reported hook")
        public long bytesBumped;
        @Description("Arena allocations since the previous reported hook")
        public long allocations;
        @Description("Delegated allocations since the previous reported hook")
        public long delegated;
        @Description("Hooks this event stands for")
        public long hooks;
        @Description("HEAP or DIRECT")
        public String space;
    }

    /**
     * One buffer retained or released on a thread that does not own it. Stack traces are on by default for
     * an event without a {@code stackTrace=false} setting, so the recording shows who did it.
     */
    @SuppressWarnings("Since15")
    @Label("Arena Confinement Violation")
    @Name(ConfinementViolation.NAME)
    @Description("An event-loop arena buffer was retained or released off its owner thread")
    static final class ConfinementViolation extends AbstractAllocatorEvent {
        static final String NAME = "io.netty.ArenaConfinementViolation";
        private static final ConfinementViolation INSTANCE = new ConfinementViolation();

        static boolean isEventEnabled() {
            return INSTANCE.isEnabled();
        }

        @Description("The name of the thread that owns the buffer")
        public String owner;
        @Description("The name of the thread that touched it")
        public String offender;
        @Description("RETAIN or RELEASE")
        public String operation;
    }
}
