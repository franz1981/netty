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
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@code -Dio.netty.allocator.pagestore.mmap}: what the public constructor does without it and with it. The
 * property is read once, so each case runs in the surefire execution that sets it (or not); the
 * {@code pagestore.test.nativeAccess} property says the JVM was given native access, so that mmap(2) must link
 * where the platform is Linux x86_64 or aarch64.
 */
public class PageStoreMmapOptInTest {
    @RegisterExtension
    final AllocatorCloser closer = new AllocatorCloser();

    // The JVM was given native access and the platform is one that LinuxMmapLinker links: Linux, x86_64 or aarch64.
    private static final boolean NATIVE_ACCESS = Boolean.getBoolean("pagestore.test.nativeAccess")
            && "linux".equals(PlatformDependent.normalizedOs())
            && ("x86_64".equals(PlatformDependent.normalizedArch())
            || "aarch_64".equals(PlatformDependent.normalizedArch()));

    /** Default: the whole block is charged at the first buffer, as for a malloc'd region, whether mmap links or not. */
    @Test
    void withoutThePropertyDirectRegionsAreMalloced() {
        assumeFalse(PageStoreConfig.MMAP, "the property is true in this execution");
        if (NATIVE_ACCESS) {
            assertTrue(MmapRegionSource.isAvailable(), "this execution has native access: mmap(2) would link");
        }
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(true, false));
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        assertEquals(PageStoreConfig.SEGMENT_SIZE_BYTES, allocator.metric().usedDirectMemory());
        buf.release();
    }

    /**
     * Opted in, with Netty counting direct memory: the slices of the chunk are charged as they are committed: a slice
     * for a small buffer.
     */
    @Test
    void withThePropertyDirectRegionsAreMapped() {
        assumeTrue(PageStoreConfig.MMAP, "the property is not true in this execution");
        if (NATIVE_ACCESS) {
            assertTrue(MmapRegionSource.isAvailable(), "this execution has native access: mmap(2) must link");
        }
        assumeTrue(MmapRegionSource.isAvailable(), "mmap(2) regions are not available here");
        assumeTrue(PlatformDependent.usedDirectMemory() >= 0, "the direct memory counter is off");
        AdaptiveByteBufAllocator allocator = closer.add(new AdaptiveByteBufAllocator(true, false));
        ByteBuf buf = allocator.directBuffer(1024, 1024);
        assertEquals(PageStoreConfig.SLICE_SIZE_BYTES, allocator.metric().usedDirectMemory());
        buf.release();
    }
}
