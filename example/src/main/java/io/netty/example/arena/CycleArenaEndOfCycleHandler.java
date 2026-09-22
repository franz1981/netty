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
package io.netty.example.arena;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CycleArenaAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * EXPERIMENT: closes a cycle of the {@link CycleArenaAllocator} at the natural Netty boundary.
 * <p>
 * {@code channelReadComplete} is the end of one read cycle of the event loop: every inbound handler before
 * this one has already seen the reads and written whatever it wanted to write. With
 * {@code -Darena.release=hook} that is the only moment a block is reset, so this handler belongs at the end
 * of the pipeline. It does nothing unless the channel actually allocates from a {@link CycleArenaAllocator}.
 */
@ChannelHandler.Sharable
public final class CycleArenaEndOfCycleHandler extends ChannelInboundHandlerAdapter {

    public static final CycleArenaEndOfCycleHandler INSTANCE = new CycleArenaEndOfCycleHandler();

    private CycleArenaEndOfCycleHandler() {
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelReadComplete();
        ByteBufAllocator alloc = ctx.alloc();
        if (alloc instanceof CycleArenaAllocator) {
            ((CycleArenaAllocator) alloc).endOfCycle();
        }
    }
}
