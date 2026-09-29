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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Real frames (HPACK encoded, decoded by DefaultHttp2ConnectionDecoder with header validation on) delivered to
 * InboundHttp2ToHttpAdapter. The downgraded request must not carry the userinfo in Host, whatever :scheme says.
 */
public class AuthorityUserinfoWireTest {

    @ParameterizedTest
    @ValueSource(strings = {"https", "http", "ftp", "https\t", "foo"})
    public void userinfoNeverReachesHttp1Host(String scheme) throws Exception {
        String host = downgradedHost(scheme);
        assertNull(host, ":scheme '" + scheme + "' forwarded Host: " + host);
    }

    private static String downgradedHost(String scheme) throws Exception {
        Http2Connection connection = new DefaultHttp2Connection(true);
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .frameListener(new InboundHttp2ToHttpAdapterBuilder(connection)
                        .maxContentLength(1024).validateHttpHeaders(true).propagateSettings(false).build())
                .connection(connection)
                .gracefulShutdownTimeoutMillis(0)
                .build();
        EmbeddedChannel ch = new EmbeddedChannel();
        Http2FrameInboundWriter writer = new Http2FrameInboundWriter(ch);
        ch.connect(new InetSocketAddress(0));
        ch.pipeline().addLast(handler);
        try {
            ch.writeInbound(Http2CodecUtil.connectionPrefaceBuf());
            writer.writeInboundSettings(new Http2Settings());
            writer.writeInboundSettingsAck();
            Http2Headers headers = new DefaultHttp2Headers(false)
                    .method("GET").scheme(scheme).authority("trusted.example@attacker.example").path("/admin");
            writer.writeInboundHeaders(3, headers, 0, true);
            Object msg;
            while ((msg = ch.readInbound()) != null) {
                try {
                    if (msg instanceof FullHttpRequest) {
                        return ((FullHttpRequest) msg).headers().get(HttpHeaderNames.HOST);
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }
            return null;
        } finally {
            ch.finishAndReleaseAll();
        }
    }
}
