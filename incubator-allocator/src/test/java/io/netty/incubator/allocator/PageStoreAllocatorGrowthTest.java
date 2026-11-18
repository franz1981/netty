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

import io.netty.buffer.AbstractAdaptiveByteBufAllocatorGrowthTest;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.parallel.Isolated;

@Isolated("Uses a large amount of heap memory, so we do not want it to run concurrently with other allocator tests")
public class PageStoreAllocatorGrowthTest extends AbstractAdaptiveByteBufAllocatorGrowthTest {
    private AdaptiveByteBufAllocator allocator;

    @Override
    protected ByteBufAllocator newAllocator() {
        allocator = new AdaptiveByteBufAllocator(false);
        return allocator;
    }

    /** The superclass' own {@code @AfterAll} only drops its reference. */
    @AfterAll
    void closeAllocator() {
        if (allocator != null) {
            allocator.close();
        }
    }
}
