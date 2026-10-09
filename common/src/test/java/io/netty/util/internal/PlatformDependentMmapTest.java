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
package io.netty.util.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link PlatformDependent#mmapAnonymous(long)}, {@link PlatformDependent#munmap(long, long)} and
 * {@link PlatformDependent#madviseDontNeed(long, long)}, where they are linked (Java 22+ on Linux with native
 * access): a failed call reports its errno.
 */
class PlatformDependentMmapTest {

    /** A failed call throws: a mapping larger than the address space, an address that is not on a page. */
    @Test
    void failuresThrow() {
        assumeTrue(PlatformDependent.hasMmap(), "mmap(2) is not linked here");

        assertThrows(NativeCallException.class, () -> PlatformDependent.mmapAnonymous(1L << 62));
        assertThrows(NativeCallException.class, () -> PlatformDependent.munmap(1, 4096));
        assertThrows(NativeCallException.class, () -> PlatformDependent.madviseDontNeed(1, 4096));
    }
}
