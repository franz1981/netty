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
package io.netty.testsuite_jpms.it;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Run with {@code -Dio.netty.allocator.type=pagestore} (see the pom) and the page-store module on the module path:
 * {@code ByteBufUtil} must load it by name as the default allocator.
 */
public class PageStoreAllocatorTest {

    @Test
    public void thePageStoreAllocatorIsTheDefault() {
        assertEquals("pagestore", System.getProperty("io.netty.allocator.type"));
        Class<?> allocatorClass = ByteBufAllocator.DEFAULT.getClass();
        assertEquals("io.netty.incubator.allocator.AdaptiveByteBufAllocator", allocatorClass.getName());
        assertTrue(allocatorClass.getModule().isNamed(), "loaded from the module path");
        assertEquals("io.netty.incubator.allocator", allocatorClass.getModule().getName());
        ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(100);
        try {
            buf.writeLong(42);
            assertEquals(42, buf.readLong());
        } finally {
            assertTrue(buf.release());
        }
    }
}
