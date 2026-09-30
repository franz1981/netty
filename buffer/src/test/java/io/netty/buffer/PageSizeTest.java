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

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** {@link PageSize}: the resolved value, and its {@code auxv} parser in isolation. */
final class PageSizeTest {
    @Test
    void pageSizeIsAPowerOfTwoAtLeast4096() {
        int pageSize = PageSize.PAGE_SIZE;
        assertTrue(pageSize >= 4096, "" + pageSize);
        assertEquals(0, pageSize & pageSize - 1, "" + pageSize);
    }

    @Test
    void pageSizeMatchesGetconf() throws Exception {
        Process process;
        try {
            process = new ProcessBuilder("getconf", "PAGESIZE").start();
        } catch (Exception e) {
            assumeTrue(false, "getconf unavailable: " + e);
            return;
        }
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        try {
            line = reader.readLine();
        } finally {
            reader.close();
        }
        int exitCode = process.waitFor();
        assumeTrue(exitCode == 0 && line != null, "getconf PAGESIZE failed");
        assertEquals(Integer.parseInt(line.trim()), PageSize.PAGE_SIZE);
    }

    /** A crafted, little-endian {@code auxv}: an unrelated entry, {@code AT_PAGESZ}, then {@code AT_NULL}. */
    @Test
    void parseAuxvFindsPageSizeAndStopsAtNull() {
        assumeTrue(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN, "little-endian only");
        byte[] auxv = littleEndianAuxv(
                new long[] {3, 0x1000},
                new long[] {6, 4096},
                new long[] {0, 0},
                new long[] {6, 8192});
        assertEquals(4096, PageSize.parseAuxv(auxv));
    }

    @Test
    void parseAuxvWithoutPageSizeReturnsNegativeOne() {
        assumeTrue(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN, "little-endian only");
        byte[] auxv = littleEndianAuxv(new long[] {3, 0x1000}, new long[] {0, 0});
        assertEquals(-1, PageSize.parseAuxv(auxv));
    }

    private static byte[] littleEndianAuxv(long[]... entries) {
        byte[] bytes = new byte[entries.length * 16];
        int offset = 0;
        for (long[] entry : entries) {
            offset = putLongLE(bytes, offset, entry[0]);
            offset = putLongLE(bytes, offset, entry[1]);
        }
        return bytes;
    }

    private static int putLongLE(byte[] bytes, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            bytes[offset + i] = (byte) (value >>> 8 * i);
        }
        return offset + 8;
    }
}
