/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http.cors;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.VARY;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * For a config whose Access-Control-Allow-Origin depends on the request Origin, the responses that carry
 * no ACAO (request without Origin, or with a non-allowed Origin) are also Origin-dependent, but CorsHandler
 * emits them without {@code Vary: Origin}. Fetch spec, "CORS protocol and HTTP caches": a cache that stores
 * such a response serves it to a later CORS request for the same URL, without ACAO.
 */
public class CorsHandlerVaryGapTest {

    private static final CorsConfig ORIGIN_LIST = CorsConfigBuilder.forOrigin("http://good.com")
            .allowCredentials().build();
    private static final CorsConfig ANY_WITH_CREDENTIALS = CorsConfigBuilder.forAnyOrigin()
            .allowCredentials().build();

    @Test
    public void allowedOriginCarriesVary() {
        HttpResponse r = simpleRequest(ORIGIN_LIST, "http://good.com");
        assertEquals("http://good.com", r.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN));
        assertEquals(ORIGIN.toString(), r.headers().get(VARY));
        ReferenceCountUtil.release(r);
    }

    @Test
    public void originListNoOriginRequestOmitsVary() {
        HttpResponse r = simpleRequest(ORIGIN_LIST, null);
        assertNull(r.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN));
        String vary = r.headers().get(VARY);
        ReferenceCountUtil.release(r);
        assertEquals(ORIGIN.toString(), vary);
    }

    @Test
    public void originListDisallowedOriginOmitsVary() {
        HttpResponse r = simpleRequest(ORIGIN_LIST, "http://other.com");
        assertNull(r.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN));
        String vary = r.headers().get(VARY);
        ReferenceCountUtil.release(r);
        assertEquals(ORIGIN.toString(), vary);
    }

    @Test
    public void anyOriginWithCredentialsNoOriginRequestOmitsVary() {
        HttpResponse r = simpleRequest(ANY_WITH_CREDENTIALS, null);
        assertNull(r.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN));
        String vary = r.headers().get(VARY);
        ReferenceCountUtil.release(r);
        assertEquals(ORIGIN.toString(), vary);
    }

    private static HttpResponse simpleRequest(CorsConfig config, String origin) {
        EmbeddedChannel channel = new EmbeddedChannel(new CorsHandler(config),
                new SimpleChannelInboundHandler<Object>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                        ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.buffer(0)));
                    }
                });
        FullHttpRequest req = new DefaultFullHttpRequest(HTTP_1_1, GET, "/info");
        if (origin != null) {
            req.headers().set(ORIGIN, origin);
        }
        channel.writeInbound(req);
        HttpResponse response = channel.readOutbound();
        channel.finishAndReleaseAll();
        return response;
    }
}
