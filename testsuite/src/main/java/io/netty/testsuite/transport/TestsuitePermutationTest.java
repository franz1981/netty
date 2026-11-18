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
package io.netty.testsuite.transport;

import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestsuitePermutationTest {

    @Test
    public void thePageStoreAllocatorRunsTheTestsuiteWhenSelected() {
        assumeTrue("pagestore".equals(System.getProperty("io.netty.allocator.type")));
        boolean found = false;
        for (ByteBufAllocator allocator : TestsuitePermutation.allocator()) {
            found |= "io.netty.incubator.allocator.AdaptiveByteBufAllocator".equals(allocator.getClass().getName());
        }
        assertTrue(found, "-Dio.netty.allocator.type=pagestore did not add the page-store allocator: "
                + TestsuitePermutation.allocator());
    }
}
