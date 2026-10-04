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

import io.netty.buffer.AdaptivePoolingAllocator.SizeClassedChunk;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class ChunkQueueTest {

    private static SizeClassedChunk chunkWithCapacity() {
        SizeClassedChunk chunk = mock(SizeClassedChunk.class);
        return chunk;
    }

    @Test
    void theQueueKeepsItsTailAcrossRemovals() {
        AdaptivePoolingAllocator.ChunkQueue queue = new AdaptivePoolingAllocator.ChunkQueue();
        SizeClassedChunk a = chunkWithCapacity();
        SizeClassedChunk b = chunkWithCapacity();
        SizeClassedChunk c = chunkWithCapacity();
        SizeClassedChunk d = chunkWithCapacity();
        queue.pushBack(b);
        queue.pushFront(a);
        queue.pushBack(c);
        queue.remove(c);
        // The tail followed the removal of c: a pushBack now lands right after b, not lost.
        queue.pushBack(d);
        assertSame(a, queue.pollFront());
        assertSame(b, queue.pollFront());
        assertSame(d, queue.pollFront());
        assertNull(queue.pollFront());
        assertEquals(0, queue.size());

        // Removing the only chunk clears both ends too: the next push starts a fresh queue.
        queue.pushBack(a);
        queue.remove(a);
        assertEquals(0, queue.size());
        queue.pushBack(b);
        assertSame(b, queue.pollFront());
        assertNull(queue.pollFront());
    }
}
