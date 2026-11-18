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

import io.netty.util.internal.UnstableApi;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * Contains classes that must have public visibility but are not public API.
 */
class Hidden {

    /**
     * This class integrates the page-store allocator with BlockHound.
     * <p>
     * It is public but only because of the ServiceLoader's limitations
     * and SHOULD NOT be considered a public API.
     */
    @UnstableApi
    public static final class PageStoreBlockHoundIntegration implements BlockHoundIntegration {

        @Override
        public void applyTo(BlockHound.Builder builder) {
            // The page store's lock is the price of one store shared by every thread: it is taken only to add a
            // region, to give one back whole and to close the store, and it can be contended by any of them.
            builder.allowBlockingCallsInside(
                    "io.netty.incubator.allocator.PageStore",
                    "addRegion"
            );

            builder.allowBlockingCallsInside(
                    "io.netty.incubator.allocator.PageStore",
                    "close"
            );

            builder.allowBlockingCallsInside(
                    "io.netty.incubator.allocator.PageStore",
                    "releaseIdleRegion"
            );

            // The thread-local heap of a thread: made on its first allocation, dropped when the thread ends.
            builder.allowBlockingCallsInside(
                    "io.netty.incubator.allocator.AdaptivePoolingAllocator$1",
                    "initialValue"
            );

            builder.allowBlockingCallsInside(
                    "io.netty.incubator.allocator.AdaptivePoolingAllocator$1",
                    "onRemoval"
            );

            // Closing a striped heap takes its write lock, as the adaptive allocator's Magazine.free does.
            builder.allowBlockingCallsInside(
                    "io.netty.incubator.allocator.AdaptivePoolingAllocator$Heap",
                    "close"
            );
        }

        @Override
        public int compareTo(BlockHoundIntegration o) {
            return 0;
        }
    }
}
