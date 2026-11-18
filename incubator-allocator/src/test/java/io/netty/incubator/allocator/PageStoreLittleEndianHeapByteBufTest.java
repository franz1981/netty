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

import io.netty.buffer.AdaptiveLittleEndianHeapByteBufTest;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The shared AdaptiveLittleEndianHeapByteBufTest, on the page-store allocator. */
public class PageStoreLittleEndianHeapByteBufTest extends AdaptiveLittleEndianHeapByteBufTest {
    /**
     * Set by {@link #newAllocator()}, which the superclass constructor calls: no initializer, which would run after
     * it and drop the closer. Closes the allocator once the test ended, after the superclass releases its buffer.
     */
    @RegisterExtension
    AllocatorCloser closer;

    @Override
    protected ByteBufAllocator newAllocator() {
        closer = new AllocatorCloser();
        return closer.add(new AdaptiveByteBufAllocator());
    }
}
