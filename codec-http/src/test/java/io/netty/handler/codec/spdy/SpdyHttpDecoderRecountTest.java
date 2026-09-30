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

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpHeadersFactory;
import io.netty.handler.codec.http.FullHttpMessage;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * One tiny header per HEADERS frame on one stream. The size check re-walks every header already on the message
 * for each frame, so entries walked = sum of the header count at each frame. Wire bytes are the frames encoded
 * by a real SpdyFrameCodec (zlib header compression).
 */
public class SpdyHttpDecoderRecountTest {

    @Test
    public void entriesWalkedPerStream() {
        Map<Integer, FullHttpMessage> map = new HashMap<Integer, FullHttpMessage>();
        EmbeddedChannel server = new EmbeddedChannel(new SpdyHttpDecoder(SpdyVersion.SPDY_3_1, 1 << 20, map,
                DefaultHttpHeadersFactory.headersFactory(), DefaultHttpHeadersFactory.trailersFactory()));
        EmbeddedChannel wire = new EmbeddedChannel(new SpdyFrameCodec(SpdyVersion.SPDY_3_1));
        int streamId = 1;
        SpdySynStreamFrame syn = new DefaultSpdySynStreamFrame(streamId, 0, (byte) 0);
        syn.setLast(false);
        syn.headers().set(":method", "POST").set(":path", "/").set(":version", "HTTP/1.1")
                .set(":scheme", "https").set(":host", "netty.io");
        wire.writeOutbound(new DefaultSpdySynStreamFrame(streamId, 0, (byte) 0).setLast(false));
        server.writeInbound(syn);
        long wireBytes = drain(wire);

        long walked = 0;
        int frames = 0;
        for (;;) {
            int before = map.get(streamId).headers().size();
            SpdyHeadersFrame h = new DefaultSpdyHeadersFrame(streamId);
            h.setLast(false);
            h.headers().add("a", "v");
            SpdyHeadersFrame onWire = new DefaultSpdyHeadersFrame(streamId);
            onWire.setLast(false);
            onWire.headers().add("a", "v");
            wire.writeOutbound(onWire);
            wireBytes += drain(wire);
            try {
                server.writeInbound(h);
            } catch (TooLongFrameException e) {
                walked += before;
                break;
            }
            walked += before;
            frames++;
        }
        System.out.println("[recount] framesAccepted=" + frames + " entriesWalked=" + walked
                + " wireBytes=" + wireBytes + " walkedPerWireByte=" + (walked / Math.max(1, wireBytes)));
        server.finishAndReleaseAll();
        wire.finishAndReleaseAll();
    }

    private static long drain(EmbeddedChannel ch) {
        long n = 0;
        ByteBuf b;
        while ((b = ch.readOutbound()) != null) {
            n += b.readableBytes();
            b.release();
        }
        return n;
    }
}
