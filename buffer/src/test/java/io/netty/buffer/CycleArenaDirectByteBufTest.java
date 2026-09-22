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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * The general {@link ByteBuf} contract, on direct buffers served by the event-loop arena. Every buffer is
 * allocated at the start of a fresh "iteration", so that the arena really serves it instead of running out
 * of blocks and delegating (this test thread is not an event loop: nothing else ever runs the hook).
 */
public class CycleArenaDirectByteBufTest extends AbstractByteBufTest {

    private static final CycleArenaAllocator ALLOCATOR = new CycleArenaAllocator();

    @Override
    protected ByteBuf newBuffer(int capacity, int maxCapacity) {
        ALLOCATOR.runHookForTest();
        return ALLOCATOR.directBuffer(capacity, maxCapacity);
    }

    // These two release and retain the same buffer from other threads, which Invariant A forbids by
    // design: the arena throws instead of counting, so the test's latches never count down.
    @Disabled("the event-loop arena forbids cross-thread retain/release (Invariant A)")
    @Test
    @Override
    public void testRefCnt() {
    }

    @Disabled("the event-loop arena forbids cross-thread retain/release (Invariant A)")
    @Test
    @Override
    public void testRefCnt2() {
    }
}
