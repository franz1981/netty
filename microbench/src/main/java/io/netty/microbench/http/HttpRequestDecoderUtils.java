/*
 * Copyright 2023 The Netty Project
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
package io.netty.microbench.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResultProvider;
import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

final class HttpRequestDecoderUtils {

    private HttpRequestDecoderUtils() {
    }

    static final byte[] CONTENT_MIXED_DELIMITERS = createContent("\r\n", "\n");
    static final int CONTENT_LENGTH = 120;

    /**
     * {@link #CONTENT_MIXED_DELIMITERS} ends some lines with a bare LF, which only a decoder without strict line
     * parsing accepts.
     */
    static HttpRequestDecoder newDecoder(boolean validateHeaders) {
        return new HttpRequestDecoder(new HttpDecoderConfig()
                .setValidateHeaders(validateHeaders)
                .setStrictLineParsing(false));
    }

    /**
     * Decodes the buffers once and fails unless they produce {@code requests} requests and no decoder failure.
     */
    static void checkDecodes(EmbeddedChannel channel, int requests, ByteBuf... buffers) {
        int decoded = 0;
        for (ByteBuf buffer : buffers) {
            buffer.resetReaderIndex();
            channel.writeInbound(buffer.retain());
            Object msg;
            while ((msg = channel.readInbound()) != null) {
                try {
                    if (((DecoderResultProvider) msg).decoderResult().isFailure()) {
                        throw new IllegalStateException("Decoding failed",
                                ((DecoderResultProvider) msg).decoderResult().cause());
                    }
                    if (msg instanceof HttpRequest) {
                        decoded++;
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }
        }
        if (decoded != requests) {
            throw new IllegalStateException("Decoded " + decoded + " requests instead of " + requests);
        }
    }

    private static byte[] createContent(String... lineDelimiters) {
        String lineDelimiter;
        String lineDelimiter2;
        if (lineDelimiters.length == 2) {
            lineDelimiter = lineDelimiters[0];
            lineDelimiter2 = lineDelimiters[1];
        } else {
            lineDelimiter = lineDelimiters[0];
            lineDelimiter2 = lineDelimiters[0];
        }
        // This GET request is incorrect but it does not matter for HttpRequestDecoder.
        // It used only to get a long request.
        return ("GET /some/path?foo=bar&wibble=eek HTTP/1.1" + "\r\n" +
                "Upgrade: WebSocket" + lineDelimiter2 +
                "Connection: Upgrade" + lineDelimiter +
                "Host: localhost" + lineDelimiter2 +
                "Referer: http://www.site.ru/index.html" + lineDelimiter +
                "User-Agent: Mozilla/5.0 (X11; U; Linux i686; ru; rv:1.9b5) Gecko/2008050509 Firefox/3.0b5" +
                lineDelimiter2 +
                "Accept: text/html" + lineDelimiter +
                "Cookie: income=1" + lineDelimiter2 +
                "Origin: http://localhost:8080" + lineDelimiter +
                "Sec-WebSocket-Key1: 10  28 8V7 8 48     0" + lineDelimiter2 +
                "Sec-WebSocket-Key2: 8 Xt754O3Q3QW 0   _60" + lineDelimiter +
                "Content-Type: application/x-www-form-urlencoded" + lineDelimiter2 +
                "Content-Length: " + CONTENT_LENGTH + lineDelimiter +
                "\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n" +
                "1234567890\r\n"
        ).getBytes(CharsetUtil.US_ASCII);
    }

}
