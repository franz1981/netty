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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Closes the allocators a test made once it ended, after its {@code @AfterEach} methods: never left to the finalizer,
 * which would free them at any time, under another test.
 */
final class AllocatorCloser implements AfterEachCallback {
    private final List<Runnable> closes = new ArrayList<Runnable>();

    synchronized AdaptivePoolingAllocator add(AdaptivePoolingAllocator allocator) {
        closes.add(allocator::close);
        return allocator;
    }

    synchronized AdaptiveByteBufAllocator add(AdaptiveByteBufAllocator allocator) {
        closes.add(allocator::close);
        return allocator;
    }

    @Override
    public synchronized void afterEach(ExtensionContext context) {
        for (Runnable close : closes) {
            close.run();
        }
        closes.clear();
    }
}
