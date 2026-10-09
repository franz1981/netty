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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link PlatformDependent#mmapAnonymous(long)}, {@link PlatformDependent#munmap(long, long)} and
 * {@link PlatformDependent#madviseDontNeed(long, long)}, where they are linked (Java 22+ on Linux with native
 * access): a failed call reports its errno.
 */
class PlatformDependentMmapTest {

    /**
     * A failed call says why with its errno: ENOMEM (12) for a mapping larger than the address space, EINVAL (22)
     * for an address that is not on a page.
     */
    @Test
    void failuresCarryTheErrno() {
        assumeTrue(PlatformDependent.hasMmap(), "mmap(2) is not linked here");

        NativeCallException tooLarge = assertThrows(NativeCallException.class,
                () -> PlatformDependent.mmapAnonymous(1L << 62));
        assertEquals(12, tooLarge.errno());
        assertTrue(tooLarge.getMessage().endsWith(": errno 12"), tooLarge.getMessage());

        NativeCallException unmap = assertThrows(NativeCallException.class,
                () -> PlatformDependent.munmap(1, 4096));
        assertEquals(22, unmap.errno());
        assertTrue(unmap.getMessage().endsWith(": errno 22"), unmap.getMessage());

        NativeCallException advise = assertThrows(NativeCallException.class,
                () -> PlatformDependent.madviseDontNeed(1, 4096));
        assertEquals(22, advise.errno());
        assertTrue(advise.getMessage().endsWith(": errno 22"), advise.getMessage());
    }
}
