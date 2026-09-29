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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalIoHandler;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.ssl.util.CachedSelfSignedCertificate;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Reviewer tests for GHSA-6xv6-9mgg-mfvc: effect of a per-SslContext random session_id_context on legitimate
 * resumption. Same LocalChannel + SslHandler harness as the PR's SslHandlerTest regression test; the SERVER side
 * records whether each handshake was abbreviated (isSessionReused()). Each test prints a RESUMPTION line.
 */
public class SessionIdContextResumptionTest {
    private static final String HOST = "resumption-review.netty.io";
    private static final int PORT = 443;

    @BeforeAll
    public static void checkOpenSsl() {
        assumeTrue(OpenSsl.isAvailable());
        System.err.println("OPENSSL-VERSION " + OpenSsl.versionString() + " boringssl=" + OpenSsl.isBoringSSL());
    }

    private static void assumeProtocol(String protocol) {
        if (SslProtocols.TLS_v1_3.equals(protocol)) {
            assumeTrue(OpenSsl.isTlsv13Supported());
        }
    }

    // Two server contexts built from the SAME configuration (two nodes of a cluster, or a context rebuilt on
    // reload) sharing one ticket key.
    @ParameterizedTest
    @ValueSource(strings = {SslProtocols.TLS_v1_2, SslProtocols.TLS_v1_3})
    public void sameConfigContextsSharingTicketKeys(String protocol) throws Throwable {
        assumeProtocol(protocol);
        String[] r = twoIdenticalContexts(protocol, null);
        System.err.println("RESUMPTION sameConfigContextsSharingTicketKeys " + protocol + " " + r[0] + " | " + r[1]);
        assertEquals("success reused=true peerCerts=0", r[1]);
    }

    // Same, but the application pins one session_id_context on both contexts via the public API.
    @ParameterizedTest
    @ValueSource(strings = {SslProtocols.TLS_v1_2, SslProtocols.TLS_v1_3})
    public void sameConfigContextsSharingTicketKeysAndExplicitSidCtx(String protocol) throws Throwable {
        assumeProtocol(protocol);
        String[] r = twoIdenticalContexts(protocol, new byte[] {'a', 'p', 'p'});
        System.err.println("RESUMPTION sameConfigContextsSharingTicketKeysAndExplicitSidCtx " + protocol + " "
                + r[0] + " | " + r[1]);
        assertEquals("success reused=true peerCerts=0", r[1]);
    }

    // Legitimate resumption into the SAME ClientAuth.REQUIRE context by a client that presented a certificate.
    @ParameterizedTest
    @ValueSource(strings = {SslProtocols.TLS_v1_2, SslProtocols.TLS_v1_3})
    public void resumeWithClientCertIntoRequireContext(String protocol) throws Throwable {
        assumeProtocol(protocol);
        SelfSignedCertificate serverCert = CachedSelfSignedCertificate.getCachedCertificate();
        SelfSignedCertificate clientCert = new SelfSignedCertificate("client.netty.io");
        SslContext client = SslContextBuilder.forClient()
                .keyManager(clientCert.key(), clientCert.cert())
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .sslProvider(SslProvider.OPENSSL).protocols(protocol).build();
        SslContext server = SslContextBuilder.forServer(serverCert.key(), serverCert.cert())
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.REQUIRE)
                .sslProvider(SslProvider.OPENSSL).protocols(protocol).build();
        try {
            ((ReferenceCountedOpenSslContext) client).sessionContext().setSessionCacheEnabled(true);
            String[] r = run(client, new SslContext[] {server, server}, protocol + "-cert");
            System.err.println("RESUMPTION resumeWithClientCertIntoRequireContext " + protocol + " "
                    + r[0] + " | " + r[1]);
            assertEquals("success reused=false peerCerts=1", r[0]);
            assertEquals("success reused=true peerCerts=1", r[1]);
        } finally {
            ReferenceCountUtil.release(client);
            ReferenceCountUtil.release(server);
            clientCert.delete();
        }
    }

    private static String[] twoIdenticalContexts(String protocol, byte[] sidCtx) throws Throwable {
        SelfSignedCertificate cert = CachedSelfSignedCertificate.getCachedCertificate();
        SslContext client = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .sslProvider(SslProvider.OPENSSL).protocols(protocol).build();
        SslContext server1 = SslContextBuilder.forServer(cert.key(), cert.cert())
                .sslProvider(SslProvider.OPENSSL).protocols(protocol).build();
        SslContext server2 = SslContextBuilder.forServer(cert.key(), cert.cert())
                .sslProvider(SslProvider.OPENSSL).protocols(protocol).build();
        try {
            ((ReferenceCountedOpenSslContext) client).sessionContext().setSessionCacheEnabled(true);
            OpenSslSessionTicketKey key = new OpenSslSessionTicketKey(new byte[OpenSslSessionTicketKey.NAME_SIZE],
                    new byte[OpenSslSessionTicketKey.HMAC_KEY_SIZE], new byte[OpenSslSessionTicketKey.AES_KEY_SIZE]);
            // Clears SSL_OP_NO_TICKET on the client, needed for TLSv1.2 tickets (same as the PR's test).
            ((OpenSslSessionContext) client.sessionContext()).setTicketKeys(key);
            ((OpenSslSessionContext) server1.sessionContext()).setTicketKeys(key);
            ((OpenSslSessionContext) server2.sessionContext()).setTicketKeys(key);
            if (sidCtx != null) {
                assertTrue(((OpenSslServerSessionContext) server1.sessionContext()).setSessionIdContext(sidCtx));
                assertTrue(((OpenSslServerSessionContext) server2.sessionContext()).setSessionIdContext(sidCtx));
            }
            String[] r = run(client, new SslContext[] {server1, server2},
                    protocol + (sidCtx == null ? "-same" : "-sid"));
            assertEquals("success reused=false peerCerts=0", r[0]);
            return r;
        } finally {
            ReferenceCountUtil.release(client);
            ReferenceCountUtil.release(server1);
            ReferenceCountUtil.release(server2);
        }
    }

    // Two sequential connections from the same client context and peer host/port; connection i is served by
    // servers[i]. Returns, per connection, what the SERVER observed.
    private static String[] run(SslContext client, final SslContext[] servers, String name) throws Throwable {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(LocalIoHandler.newFactory());
        final BlockingQueue<String> serverResults = new LinkedBlockingQueue<String>();
        final AtomicInteger index = new AtomicInteger();
        LocalAddress addr = new LocalAddress(SessionIdContextResumptionTest.class.getSimpleName() + "." + name);
        Channel sc = null;
        try {
            sc = new ServerBootstrap().group(group).channel(LocalServerChannel.class)
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            final SslHandler handler = servers[index.getAndIncrement()].newHandler(ch.alloc());
                            ch.pipeline().addLast(handler);
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                    if (evt instanceof SslHandshakeCompletionEvent) {
                                        SslHandshakeCompletionEvent e = (SslHandshakeCompletionEvent) evt;
                                        if (e.isSuccess()) {
                                            ReferenceCountedOpenSslEngine engine =
                                                    (ReferenceCountedOpenSslEngine) handler.engine();
                                            int peerCerts;
                                            try {
                                                peerCerts = engine.getSession().getPeerCertificates().length;
                                            } catch (Exception ex) {
                                                peerCerts = 0;
                                            }
                                            serverResults.add("success reused=" + engine.isSessionReused()
                                                    + " peerCerts=" + peerCerts);
                                            ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4}));
                                        } else {
                                            serverResults.add("failure " + e.cause());
                                        }
                                    }
                                }
                            });
                        }
                    }).bind(addr).syncUninterruptibly().channel();

            String[] out = new String[2];
            for (int i = 0; i < 2; i++) {
                final BlockingQueue<Object> clientQueue = new LinkedBlockingQueue<Object>();
                final SslHandler clientHandler = client.newHandler(UnpooledByteBufAllocator.DEFAULT, HOST, PORT);
                Channel cc = new Bootstrap().group(group).channel(LocalChannel.class)
                        .handler(new ChannelInitializer<Channel>() {
                            @Override
                            protected void initChannel(Channel ch) {
                                ch.pipeline().addLast(clientHandler);
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                        clientQueue.add(msg);
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        clientQueue.add(cause);
                                    }
                                });
                            }
                        }).connect(addr).syncUninterruptibly().channel();
                try {
                    out[i] = serverResults.poll(5, TimeUnit.SECONDS);
                    assertNotNull(out[i], "no server-side handshake outcome");
                    // Wait for the application bytes so the TLSv1.3 ticket (sent after the handshake) has been
                    // processed by the client before reconnecting.
                    ReferenceCountUtil.release(clientQueue.poll(5, TimeUnit.SECONDS));
                } finally {
                    cc.close().syncUninterruptibly();
                }
            }
            return out;
        } finally {
            if (sc != null) {
                sc.close().syncUninterruptibly();
            }
            group.shutdownGracefully().syncUninterruptibly();
        }
    }
}
