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
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.AsciiString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * violetagg's scenarios from the GHSA-w424 PR conversation (2026-09-01), verbatim.
 */
public class Violetagg0901ScenariosTest {

    @ParameterizedTest
    @ValueSource(strings = {"https", "HTTPS", " https", "https\t",
            "http", "HTTP", " http", "http\t", "ftp"})
    public void toHttpRequestRejectsAuthorityWithUserInfoWithScheme(String scheme) {
        final Http2Headers headers = new DefaultHttp2Headers();
        headers.method(HttpMethod.GET.asciiName());
        headers.scheme(new AsciiString(scheme));
        headers.authority(new AsciiString("trusted.example@attacker.example"));
        headers.path(new AsciiString("/admin"));

        Http2Exception exception = assertThrows(Http2Exception.class,
            () -> HttpConversionUtil.toHttpRequest(3, headers, true));
        assertEquals(Http2Error.PROTOCOL_ERROR, exception.error());
    }

    @Test
    public void toHttpRequestRejectsHostHeaderWithUserInfo() {
        final Http2Headers headers = new DefaultHttp2Headers();
        headers.method(HttpMethod.GET.asciiName());
        headers.scheme(new AsciiString("http"));
        headers.add(HOST, new AsciiString("trusted.example@attacker.example"));
        headers.path(new AsciiString("/admin"));

        Http2Exception exception = assertThrows(Http2Exception.class,
                () -> HttpConversionUtil.toHttpRequest(3, headers, true));
        assertEquals(Http2Error.PROTOCOL_ERROR, exception.error());
    }

    @Test
    public void toHttpRequestRejectsConnectAuthorityWithUserInfo() {
        final Http2Headers headers = new DefaultHttp2Headers();
        headers.method(HttpMethod.CONNECT.asciiName());
        headers.authority(new AsciiString("trusted.example@attacker.example"));

        Http2Exception exception = assertThrows(Http2Exception.class,
                () -> HttpConversionUtil.toHttpRequest(3, headers, true));
        assertEquals(Http2Error.PROTOCOL_ERROR, exception.error());
    }

    @Test
    public void testDowngradeHeadersRejectsAuthorityWithUserInfoWithoutScheme() {
        final EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameToHttpObjectCodec(true));
        final Http2Headers headers = new DefaultHttp2Headers();
        headers.method("GET");
        headers.authority("trusted.example@attacker.example");
        headers.path("/admin");

        try {
            DecoderException exception = assertThrows(DecoderException.class,
                    () -> ch.writeInbound(new DefaultHttp2HeadersFrame(headers)));
            assertThat(exception.getCause()).isInstanceOf(Http2Exception.class);
            assertEquals(Http2Error.PROTOCOL_ERROR, ((Http2Exception) exception.getCause()).error());
            assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
}
