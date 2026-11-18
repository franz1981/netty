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
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs in its own surefire execution, with {@code -Dio.netty.allocator.type=pagestore} and a marker property that only
 * that execution sets. Skipped in the others; in its own it asserts, so an overridden property fails the build.
 */
class PageStoreDefaultAllocatorTest {

    @Test
    void theDefaultAllocatorIsThePageStoreOne() {
        assumeTrue(Boolean.getBoolean("pagestore.test.defaultAllocatorExecution"));
        assertEquals("pagestore", System.getProperty("io.netty.allocator.type"));
        assertEquals(AdaptiveByteBufAllocator.class, ByteBufAllocator.DEFAULT.getClass());
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(100);
        try {
            assertEquals(AdaptiveByteBuf.class, unwrap(buf).getClass());
        } finally {
            assertTrue(buf.release());
        }
    }

    private static ByteBuf unwrap(ByteBuf buf) {
        // with leak detection the buffer is wrapped
        while (buf.unwrap() != null) {
            buf = buf.unwrap();
        }
        return buf;
    }
}
