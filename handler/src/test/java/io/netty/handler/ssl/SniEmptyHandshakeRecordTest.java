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
package io.netty.handler.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.DomainNameMapping;
import io.netty.util.DomainNameMappingBuilder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.ResourcesUtil;
import io.netty.util.internal.StringUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SniEmptyHandshakeRecordTest {

    private static final String TLS_CLIENT_HELLO_HEX =
            "16030100" +
            "c6010000c20303bb0855d66532c05a0ef784f7c384feeafa68b3" +
            "b655ac7288650d5eed4aa3fb52000038c02cc030009fcca9cca8ccaac02b" +
            "c02f009ec024c028006bc023c0270067c00ac0140039c009c0130033009d" +
            "009c003d003c0035002f00ff010000610000001700150000124348415434" +
            "2e4c45414e434c4f55442e434e000b000403000102000a000a0008001d00" +
            "170019001800230000000d0020001e060106020603050105020503040104" +
            "0204030301030203030201020202030016000000170000";

    private static final byte[] EMPTY_HANDSHAKE_RECORD = {0x16, 0x03, 0x03, 0x00, 0x00};

    private static final class CountingSniHandler extends AbstractSniHandler<Object> {
        final AtomicInteger lookups = new AtomicInteger();

        CountingSniHandler() {
            // no handshake timeout: the base SslClientHelloHandler has none either
            super(DEFAULT_MAX_CLIENT_HELLO_LENGTH, 0);
        }

        @Override
        protected Future<Object> lookup(ChannelHandlerContext ctx, String hostname) {
            lookups.incrementAndGet();
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(new Object());
        }

        @Override
        protected void onLookupComplete(ChannelHandlerContext ctx, String hostname, Future<Object> future) {
        }

        int buffered() {
            return internalBuffer().readableBytes();
        }
    }

    /**
     * A peer that sends only zero-length handshake records makes the handler accumulate them without any bound:
     * maxClientHelloLength only caps the declared handshake length, and empty records never add to it.
     * Before the change the first empty record ended the ClientHello search.
     */
    @Test
    public void emptyHandshakeRecordsAreNotBufferedWithoutBound() {
        CountingSniHandler handler = new CountingSniHandler();
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        final int recordsPerWrite = 1000;
        final int writes = 200;
        try {
            for (int i = 0; i < writes && ch.isActive() && handler.lookups.get() == 0; i++) {
                ByteBuf buf = Unpooled.buffer(recordsPerWrite * EMPTY_HANDSHAKE_RECORD.length);
                for (int r = 0; r < recordsPerWrite; r++) {
                    buf.writeBytes(EMPTY_HANDSHAKE_RECORD);
                }
                ch.writeInbound(buf);
            }
            int buffered = ch.pipeline().get(CountingSniHandler.class) == null ? 0 : handler.buffered();
            // A legit (fragmented) ClientHello within maxClientHelloLength needs at most
            // 6 bytes of record per byte of handshake message (1-byte fragments).
            int legitBound = 6 * (SslClientHelloHandler.DEFAULT_MAX_CLIENT_HELLO_LENGTH + 4);
            assertTrue(buffered <= legitBound,
                    "buffered " + buffered + " bytes of empty handshake records (lookups=" + handler.lookups.get() +
                            ", active=" + ch.isActive() + "), bound for a legit ClientHello is " + legitBound);
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    static Iterable<SslProvider> providers() {
        List<SslProvider> params = new ArrayList<SslProvider>();
        if (OpenSsl.isAvailable()) {
            params.add(SslProvider.OPENSSL);
        }
        params.add(SslProvider.JDK);
        return params;
    }

    /**
     * End to end with the real SniHandler: a leading empty handshake record followed by a ClientHello whose SNI
     * maps to a ClientAuth.REQUIRE context. Records which context got selected and whether the server engine
     * produced a ServerHello. The handshake must never make progress under the default context.
     */
    @ParameterizedTest(name = "{index}: sslProvider={0}")
    @MethodSource("providers")
    public void leadingEmptyHandshakeRecordEndToEnd(SslProvider provider) throws Exception {
        File keyFile = ResourcesUtil.getFile(SniHandlerTest.class, "test_encrypted.pem");
        File crtFile = ResourcesUtil.getFile(SniHandlerTest.class, "test.crt");
        SslContext defaultCtx = SslContextBuilder.forServer(crtFile, keyFile, "12345")
                .sslProvider(provider).build();
        SslContext protectedCtx = SslContextBuilder.forServer(crtFile, keyFile, "12345")
                .sslProvider(provider).clientAuth(ClientAuth.REQUIRE).trustManager(crtFile).build();
        DomainNameMapping<SslContext> mapping = new DomainNameMappingBuilder<SslContext>(defaultCtx)
                .add("chat4.leancloud.cn", protectedCtx).build();
        SniHandler handler = new SniHandler(mapping);
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        Throwable thrown = null;
        try {
            ByteBuf in = Unpooled.buffer();
            in.writeBytes(EMPTY_HANDSHAKE_RECORD);
            in.writeBytes(StringUtil.decodeHexDump(TLS_CLIENT_HELLO_HEX));
            try {
                ch.writeInbound(in);
            } catch (Throwable t) {
                thrown = t;
            }
            int outboundBytes = 0;
            for (;;) {
                Object o = ch.readOutbound();
                if (o == null) {
                    break;
                }
                if (o instanceof ByteBuf) {
                    outboundBytes += ((ByteBuf) o).readableBytes();
                }
                ReferenceCountUtil.release(o);
            }
            String selected = handler.sslContext() == defaultCtx ? "default" :
                    handler.sslContext() == protectedCtx ? "protected" : String.valueOf(handler.sslContext());
            Throwable root = thrown;
            while (root != null && root.getCause() != null) {
                root = root.getCause();
            }
            System.err.println("jp47-e2e[" + provider + "]: hostname=" + handler.hostname() +
                    " selected=" + selected + " serverOutboundBytes=" + outboundBytes + " rootCause=" + root);
            assertTrue(!"default".equals(selected) || outboundBytes == 0,
                    "server answered the ClientHello under the default context: " + outboundBytes + " bytes");
        } finally {
            ch.finishAndReleaseAll();
            ReferenceCountUtil.release(defaultCtx);
            ReferenceCountUtil.release(protectedCtx);
        }
    }
}
