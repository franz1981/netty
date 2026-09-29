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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.DomainNameMapping;
import io.netty.util.DomainNameMappingBuilder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.ResourcesUtil;
import org.junit.jupiter.api.Test;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compares {@link AbstractSniHandler#extractSniHostname(ByteBuf)} with what a JDK server SSLEngine
 * accepts for a set of crafted server_name extensions.
 */
public class SniJdkDifferentialTest {

    private static byte[] entry(int type, String value) {
        byte[] v = value.getBytes(StandardCharsets.US_ASCII);
        byte[] e = new byte[3 + v.length];
        e[0] = (byte) type;
        e[1] = (byte) (v.length >>> 8);
        e[2] = (byte) v.length;
        System.arraycopy(v, 0, e, 3, v.length);
        return e;
    }

    private static byte[] list(int declaredLenDelta, byte[]... entries) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len = 0;
        for (byte[] e : entries) {
            len += e.length;
        }
        len += declaredLenDelta;
        out.write(len >>> 8);
        out.write(len);
        for (byte[] e : entries) {
            out.write(e, 0, e.length);
        }
        return out.toByteArray();
    }

    private static byte[] jdkClientHello() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, null, null);
        SSLEngine client = ctx.createSSLEngine("secure.test", 443);
        client.setUseClientMode(true);
        client.setEnabledProtocols(new String[] {"TLSv1.2"});
        SSLParameters params = client.getSSLParameters();
        params.setServerNames(Collections.<SNIServerName>singletonList(new SNIHostName("secure.test")));
        client.setSSLParameters(params);
        ByteBuffer out = ByteBuffer.allocate(client.getSession().getPacketBufferSize());
        client.wrap(ByteBuffer.allocate(0), out);
        out.flip();
        byte[] b = new byte[out.remaining()];
        out.get(b);
        return b;
    }

    /** Returns the ClientHello body (no record / handshake header) with the SNI extensions replaced. */
    private static byte[] rewrite(byte[] record, byte[]... sniExtensionDatas) {
        ByteBuf in = Unpooled.wrappedBuffer(record);
        int body = 5 + 4;
        int off = body + 2 + 32;
        off += 1 + in.getUnsignedByte(off);
        off += 2 + in.getUnsignedShort(off);
        off += 1 + in.getUnsignedByte(off);
        int extLen = in.getUnsignedShort(off);
        int extStart = off + 2;
        int extEnd = extStart + extLen;
        ByteArrayOutputStream exts = new ByteArrayOutputStream();
        for (byte[] d : sniExtensionDatas) {
            exts.write(0);
            exts.write(0);
            exts.write(d.length >>> 8);
            exts.write(d.length);
            exts.write(d, 0, d.length);
        }
        int p = extStart;
        while (p < extEnd) {
            int type = in.getUnsignedShort(p);
            int len = in.getUnsignedShort(p + 2);
            if (type != 0) {
                exts.write(record, p, 4 + len);
            }
            p += 4 + len;
        }
        byte[] newExts = exts.toByteArray();
        ByteArrayOutputStream bodyOut = new ByteArrayOutputStream();
        bodyOut.write(record, body, off - body);
        bodyOut.write(newExts.length >>> 8);
        bodyOut.write(newExts.length);
        bodyOut.write(newExts, 0, newExts.length);
        return bodyOut.toByteArray();
    }

    private static byte[] toRecord(byte[] chBody) {
        int hsLen = chBody.length;
        int recLen = hsLen + 4;
        byte[] r = new byte[5 + recLen];
        r[0] = 22;
        r[1] = 3;
        r[2] = 1;
        r[3] = (byte) (recLen >>> 8);
        r[4] = (byte) recLen;
        r[5] = 1;
        r[6] = (byte) (hsLen >>> 16);
        r[7] = (byte) (hsLen >>> 8);
        r[8] = (byte) hsLen;
        System.arraycopy(chBody, 0, r, 9, hsLen);
        return r;
    }

    private static String jdk(SslContext serverCtx, byte[] record) {
        SSLEngine server = serverCtx.newEngine(ByteBufAllocator.DEFAULT);
        final List<String> matched = new ArrayList<String>();
        try {
            server.setUseClientMode(false);
            SSLParameters params = server.getSSLParameters();
            params.setSNIMatchers(Collections.<SNIMatcher>singletonList(new SNIMatcher(0) {
                @Override
                public boolean matches(SNIServerName serverName) {
                    matched.add(((SNIHostName) serverName).getAsciiName());
                    return true;
                }
            }));
            server.setSSLParameters(params);
            ByteBuffer dst = ByteBuffer.allocate(server.getSession().getApplicationBufferSize());
            SSLEngineResult r = server.unwrap(ByteBuffer.wrap(record), dst);
            Runnable task;
            while ((task = server.getDelegatedTask()) != null) {
                task.run();
            }
            ByteBuffer out = ByteBuffer.allocate(server.getSession().getPacketBufferSize());
            SSLEngineResult w = server.wrap(ByteBuffer.allocate(0), out);
            SSLSession hs = server.getHandshakeSession();
            String names = hs instanceof ExtendedSSLSession ?
                    String.valueOf(((ExtendedSSLSession) hs).getRequestedServerNames()) : "n/a";
            return "accepted matcher=" + matched + " unwrap=" + r.getStatus() + " wrapBytes=" + w.bytesProduced() +
                    " names=" + names;
        } catch (Exception e) {
            return "rejected: " + e + " matcher=" + matched;
        } finally {
            ReferenceCountUtil.release(server);
        }
    }

    @Test
    public void compare() throws Exception {
        File keyFile = ResourcesUtil.getFile(SniHandlerTest.class, "test_encrypted.pem");
        File crtFile = ResourcesUtil.getFile(SniHandlerTest.class, "test.crt");
        SslContext serverCtx = SslContextBuilder.forServer(crtFile, keyFile, "12345")
                .sslProvider(SslProvider.JDK).build();
        SslContext opensslCtx = OpenSsl.isAvailable() ? SslContextBuilder.forServer(crtFile, keyFile, "12345")
                .sslProvider(SslProvider.OPENSSL).build() : null;
        byte[] ch = jdkClientHello();
        String[] names = {
                "baseline host", "unknown,host", "host a,host b", "unknown(len5),host",
                "list shorter than ext (host outside list)", "list longer than ext",
                "two SNI exts: [unknown] then [host]", "two SNI exts: [host a] then [host b]",
                "unknown,unknown,host",
        };
        byte[][][] cases = {
                {list(0, entry(0, "secure.test"))},
                {list(0, entry(1, ""), entry(0, "secure.test"))},
                {list(0, entry(0, "a.test"), entry(0, "secure.test"))},
                {list(0, entry(7, "xxxxx"), entry(0, "secure.test"))},
                {list(-14, entry(1, ""), entry(0, "secure.test"))},
                {list(3, entry(0, "secure.test"))},
                {list(0, entry(1, "")), list(0, entry(0, "secure.test"))},
                {list(0, entry(0, "a.test")), list(0, entry(0, "secure.test"))},
                {list(0, entry(1, ""), entry(2, ""), entry(0, "secure.test"))},
        };
        List<String> mismatches = new ArrayList<String>();
        try {
            for (int i = 0; i < cases.length; i++) {
                byte[] body = rewrite(ch, cases[i]);
                ByteBuf buf = Unpooled.wrappedBuffer(body);
                String netty = AbstractSniHandler.extractSniHostname(buf);
                String jdk = jdk(serverCtx, toRecord(body));
                String line = "p889-diff [" + names[i] + "] netty=" + netty + " jdk=" + jdk;
                System.err.println(line);
                if (opensslCtx != null) {
                    System.err.println("p889-diff-openssl [" + names[i] + "] " + jdk(opensslCtx, toRecord(body)));
                }
                String expected = "matcher=[" + (netty == null ? "" : netty) + "]";
                if (jdk.startsWith("accepted") && (!jdk.contains(expected) || !jdk.contains("wrapBytes=")
                        || jdk.contains("wrapBytes=0 "))) {
                    mismatches.add(line);
                }
            }
        } finally {
            ReferenceCountUtil.release(serverCtx);
            ReferenceCountUtil.release(opensslCtx);
        }
        assertTrue(mismatches.isEmpty(), mismatches.toString());
    }

    /**
     * End to end through SniHandler with the JDK provider: a ClientHello with two server_name extensions,
     * the first holding only an unknown NameType and the second host_name=secure.test. secure.test maps to a
     * ClientAuth.REQUIRE context, the default context is permissive.
     */
    @Test
    public void duplicateServerNameExtensionEndToEnd() throws Exception {
        File keyFile = ResourcesUtil.getFile(SniHandlerTest.class, "test_encrypted.pem");
        File crtFile = ResourcesUtil.getFile(SniHandlerTest.class, "test.crt");
        SslContext defaultCtx = SslContextBuilder.forServer(crtFile, keyFile, "12345")
                .sslProvider(SslProvider.JDK).build();
        SslContext protectedCtx = SslContextBuilder.forServer(crtFile, keyFile, "12345")
                .sslProvider(SslProvider.JDK).clientAuth(ClientAuth.REQUIRE).trustManager(crtFile).build();
        DomainNameMapping<SslContext> mapping = new DomainNameMappingBuilder<SslContext>(defaultCtx)
                .add("secure.test", protectedCtx).build();
        SniHandler handler = new SniHandler(mapping);
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        try {
            byte[] body = rewrite(jdkClientHello(), list(0, entry(1, "")), list(0, entry(0, "secure.test")));
            Throwable thrown = null;
            try {
                ch.writeInbound(Unpooled.wrappedBuffer(toRecord(body)));
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
            SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
            String engineNames = "n/a";
            if (sslHandler != null && sslHandler.engine().getHandshakeSession() instanceof ExtendedSSLSession) {
                engineNames = String.valueOf(
                        ((ExtendedSSLSession) sslHandler.engine().getHandshakeSession()).getRequestedServerNames());
            }
            String line = "p889-e2e: hostname=" + handler.hostname() + " selected=" + selected +
                    " serverOutboundBytes=" + outboundBytes + " engineNames=" + engineNames + " thrown=" + thrown;
            System.err.println(line);
            assertTrue("protected".equals(selected) || outboundBytes == 0, line);
        } finally {
            ch.finishAndReleaseAll();
            ReferenceCountUtil.release(defaultCtx);
            ReferenceCountUtil.release(protectedCtx);
        }
    }
}
