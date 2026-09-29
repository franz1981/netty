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
package io.netty.handler.codec.spdy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpHeadersFactory;
import io.netty.handler.codec.http.FullHttpMessage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic bound check: the headers retained by the buffered message never exceed the decoder's
 * maxHeaderSize (default 16384, via the pre-existing 5-arg constructor), for the request path
 * (non-final SYN_STREAM), the response path (non-final SYN_REPLY) and trailers after a DATA frame.
 */
public class SpdyHttpDecoderHeaderBoundTest {

    private static final int DEFAULT_LIMIT = 16384;
    private static final int FRAMES = 2000;

    @ParameterizedTest
    @ValueSource(strings = {"request", "response", "trailersAfterData"})
    public void retainedHeaderSizeStaysBounded(String path) {
        Map<Integer, FullHttpMessage> map = new HashMap<Integer, FullHttpMessage>();
        // Pre-existing constructor: no maxHeaderSize argument, so this exercises the default bound.
        EmbeddedChannel ch = new EmbeddedChannel(new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, 1 << 20, map,
                DefaultHttpHeadersFactory.headersFactory(), DefaultHttpHeadersFactory.trailersFactory()));
        int streamId = 1;
        if ("response".equals(path)) {
            SpdySynReplyFrame reply = new DefaultSpdySynReplyFrame(streamId);
            reply.setLast(false);
            reply.headers().set(":status", "200");
            reply.headers().set(":version", "HTTP/1.1");
            assertFalse(ch.writeInbound(reply));
        } else {
            SpdySynStreamFrame syn = new DefaultSpdySynStreamFrame(streamId, 0, (byte) 0);
            syn.setLast(false);
            syn.headers().set(":method", "POST").set(":path", "/").set(":version", "HTTP/1.1")
                    .set(":scheme", "https").set(":host", "netty.io");
            assertFalse(ch.writeInbound(syn));
            if ("trailersAfterData".equals(path)) {
                assertFalse(ch.writeInbound(new DefaultSpdyDataFrame(streamId, Unpooled.wrappedBuffer(new byte[16]))));
            }
        }
        assertTrue(map.containsKey(streamId));

        long maxRetained = 0;
        long maxEntries = 0;
        int acceptedFrames = 0;
        boolean rejected = false;
        for (int f = 0; f < FRAMES; f++) {
            SpdyHeadersFrame h = new DefaultSpdyHeadersFrame(streamId);
            h.setLast(false);
            for (int i = 0; i < 100; i++) {
                h.headers().add("x-pad-" + f + '-' + i, "v");
            }
            try {
                ch.writeInbound(h);
            } catch (TooLongFrameException expected) {
                rejected = true;
                break;
            }
            acceptedFrames++;
            FullHttpMessage m = map.get(streamId);
            long size = 0;
            int entries = 0;
            for (Iterator<Map.Entry<CharSequence, CharSequence>> it = m.headers().iteratorCharSequence();
                 it.hasNext();) {
                Map.Entry<CharSequence, CharSequence> e = it.next();
                size += e.getKey().length() + e.getValue().length() + 2;
                entries++;
            }
            maxRetained = Math.max(maxRetained, size);
            maxEntries = Math.max(maxEntries, entries);
        }
        System.out.println("[bound] path=" + path + " acceptedFrames=" + acceptedFrames + " rejected=" + rejected
                + " maxRetainedHeaderBytes=" + maxRetained + " maxEntries=" + maxEntries
                + " mapSizeAfter=" + map.size());
        try {
            assertTrue(maxRetained <= DEFAULT_LIMIT, "retained " + maxRetained + " > " + DEFAULT_LIMIT);
            assertTrue(rejected, "no rejection after " + FRAMES + " frames");
            assertEquals(0, map.size());
            // Further frames on the rejected stream must not start accumulating again.
            SpdyHeadersFrame again = new DefaultSpdyHeadersFrame(streamId);
            again.headers().add("x-again", "v");
            assertFalse(ch.writeInbound(again));
            assertEquals(0, map.size());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
}
