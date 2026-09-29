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
package io.netty.handler.codec.http3;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The :scheme is chosen by the peer and is not what an HTTP/1 downstream routes on; Host is. The userinfo must not
 * reach Host whatever :scheme says. Two layers: the QPACK header sink validation, then the HTTP/1 downgrade.
 */
public class AuthorityUserinfoSchemeGateTest {

    @ParameterizedTest
    @ValueSource(strings = {"https", "http", "ftp", "https\t", "foo"})
    public void userinfoNeverReachesHttp1Host(String scheme) throws Exception {
        // 1. header sink (what the QPACK decoder feeds); a validation exception here would stop the request.
        Http3HeadersSink sink = new Http3HeadersSink(new DefaultHttp3Headers(), 1024, true, false);
        sink.accept(Http3Headers.PseudoHeaderName.METHOD.value(), "GET");
        sink.accept(Http3Headers.PseudoHeaderName.SCHEME.value(), scheme);
        sink.accept(Http3Headers.PseudoHeaderName.AUTHORITY.value(), "trusted.example@attacker.example");
        sink.accept(Http3Headers.PseudoHeaderName.PATH.value(), "/admin");
        try {
            sink.finish();
        } catch (Http3HeadersValidationException expected) {
            return;
        }
        // 2. downgrade to HTTP/1
        EmbeddedQuicStreamChannel ch = new EmbeddedQuicStreamChannel(new Http3FrameToHttpObjectCodec(true));
        Http3Headers headers = new DefaultHttp3Headers(false);
        headers.method("GET").scheme(scheme).path("/admin").authority("trusted.example@attacker.example");
        ch.writeInbound(new DefaultHttp3HeadersFrame(headers));
        Object msg = ch.readInbound();
        String host = msg instanceof HttpRequest ? ((HttpRequest) msg).headers().get(HttpHeaderNames.HOST) : null;
        ReferenceCountUtil.release(msg);
        ch.finishAndReleaseAll();
        assertNull(host, ":scheme '" + scheme + "' forwarded Host: " + host);
    }
}
