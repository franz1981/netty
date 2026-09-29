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
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Public-API only (compiles with and without the fix).
 * 1) Bound: N alternating SETTINGS_HEADER_TABLE_SIZE changes with no header block in between must not
 *    produce more than two dynamic table size updates (RFC 7541 4.2) in the next header block.
 * 2) Correctness: random size-change sequences interleaved with random header blocks round-trip through
 *    DefaultHttp2HeadersDecoder, and every block starts with at most two size updates.
 */
public class HpackTableSizeUpdateBoundTest {

    @Test
    public void alternatingChangesAreCoalesced() throws Http2Exception {
        for (int n : new int[] {1, 2, 100, 100000}) {
            DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder();
            for (int i = 0; i < n; i++) {
                encoder.maxHeaderTableSize(i % 2 == 0 ? 100 : 101);
            }
            ByteBuf out = Unpooled.buffer();
            try {
                encoder.encodeHeaders(1, EmptyHttp2Headers.INSTANCE, out);
                int updates = countLeadingSizeUpdates(out.duplicate());
                System.out.println("[bound] n=" + n + " headerBlockBytes=" + out.readableBytes()
                        + " sizeUpdates=" + updates);
                assertTrue(updates <= 2, "n=" + n + " emitted " + updates + " size updates");
                assertTrue(out.readableBytes() <= 6, "n=" + n + " block is " + out.readableBytes() + " bytes");
            } finally {
                out.release();
                encoder.close();
            }
        }
    }

    @Test
    public void randomSizeChangesRoundTrip() throws Http2Exception {
        Random r = new Random(42);
        String[] names = {"a", "bb", "ccc", "x-custom", "cookie", "user-agent"};
        int maxUpdatesSeen = 0;
        for (int conn = 0; conn < 200; conn++) {
            DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder();
            DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(false);
            try {
                for (int block = 0; block < 50; block++) {
                    int changes = r.nextInt(6);
                    for (int c = 0; c < changes; c++) {
                        // up to 6000 to also hit the encoder's 4096 clamp
                        long v = r.nextInt(4) == 0 ? r.nextInt(80) : r.nextInt(6000);
                        encoder.maxHeaderTableSize(v);
                        decoder.maxHeaderTableSize(v);
                    }
                    Http2Headers in = new DefaultHttp2Headers(false);
                    int count = 1 + r.nextInt(8);
                    for (int h = 0; h < count; h++) {
                        in.add(names[r.nextInt(names.length)], "v" + r.nextInt(20));
                    }
                    ByteBuf buf = Unpooled.buffer();
                    try {
                        encoder.encodeHeaders(3, in, buf);
                        int updates = countLeadingSizeUpdates(buf.duplicate());
                        maxUpdatesSeen = Math.max(maxUpdatesSeen, updates);
                        assertTrue(updates <= 2, "conn " + conn + " block " + block + ": " + updates + " updates");
                        Http2Headers outHeaders = decoder.decodeHeaders(3, buf);
                        assertEquals(in, outHeaders, "conn " + conn + " block " + block);
                    } finally {
                        buf.release();
                    }
                }
            } finally {
                encoder.close();
            }
        }
        System.out.println("[roundtrip] maxSizeUpdatesInOneBlock=" + maxUpdatesSeen);
    }

    /** Counts the dynamic table size update representations (001xxxxx) at the start of a header block. */
    private static int countLeadingSizeUpdates(ByteBuf in) {
        int count = 0;
        while (in.isReadable() && (in.getByte(in.readerIndex()) & 0xE0) == 0x20) {
            int prefix = in.readByte() & 0x1F;
            if (prefix == 0x1F) {
                byte b;
                do {
                    b = in.readByte();
                } while ((b & 0x80) != 0);
            }
            count++;
        }
        return count;
    }
}
