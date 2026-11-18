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
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs in its own surefire execution, with {@code -Dio.netty.buffer.checkAccessible=false} (read once, when
 * AbstractByteBuf is initialized) and a marker property that only that execution sets. Skipped in the others.
 * The indexOf case reads raw memory without the guard only where Unsafe is on, which is not the default from Java 25:
 * there it passes through the base path whatever the guard does.
 */
class PageStoreReleasedBufferTest {

    @Test
    void indexOfOnAReleasedBufferThrowsInsteadOfReadingMemory() {
        assumeTrue(Boolean.getBoolean("pagestore.test.uncheckedAccessExecution"));
        AdaptiveByteBufAllocator alloc = new AdaptiveByteBufAllocator();
        try {
            for (boolean direct : new boolean[] {true, false}) {
                ByteBuf buf = direct ? alloc.directBuffer(64) : alloc.heapBuffer(64);
                // a direct root without an address (-Dio.netty.noUnsafe=true on Java 24 and before) has none to check
                boolean hasAddress = buf.hasMemoryAddress();
                buf.writeZero(64);
                buf.release();
                // as the adaptive allocator of netty-buffer: a released buffer has no memory address, so that nothing
                // that trusts hasMemoryAddress() reads raw memory
                assertFalse(buf.hasMemoryAddress());
                if (direct && hasAddress) {
                    // proves the flag took effect: with the check on this would throw IllegalReferenceCountException
                    assertThrows(UnsupportedOperationException.class, buf::memoryAddress);
                    // the copies of netty-buffer that take the address of a source or destination with one
                    ByteBuf dst = Unpooled.directBuffer(64);
                    try {
                        assertThrows(IllegalReferenceCountException.class, () -> dst.setBytes(0, buf, 0, 16));
                        assertThrows(IllegalReferenceCountException.class, () -> dst.getBytes(0, buf, 0, 16));
                    } finally {
                        dst.release();
                    }
                }
                assertThrows(IllegalReferenceCountException.class, () -> buf.indexOf(0, 16, (byte) 1));
                assertThrows(IllegalReferenceCountException.class, () -> buf.indexOf(8, 64, (byte) 0));
            }
        } finally {
            alloc.close();
        }
    }
}
