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
package io.netty.handler.codec.socks;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The legacy decoders read the one-octet ULEN / PLEN / domain-length fields with readByte(), so a spec-legal
 * length in 128..255 becomes negative. Each test sends a valid message with a 200-byte field.
 */
public class SocksSignedLengthFieldTest {
    private static final int LEN = 200;
    // IDN.toASCII in SocksCmdRequest/Response rejects labels over 63 chars: 4 x 49 + 3 dots = 199 bytes.
    private static final String DOMAIN;
    static {
        String label = repeat(49, 'd');
        DOMAIN = label + '.' + label + '.' + label + '.' + label;
    }

    private static void writeAscii(ByteBuf buf, int len, char c) {
        for (int i = 0; i < len; i++) {
            buf.writeByte(c);
        }
    }

    private static String repeat(int len, char c) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    @Test
    public void authRequestUsernameLongerThan127() {
        EmbeddedChannel e = new EmbeddedChannel(new SocksAuthRequestDecoder());
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(SocksSubnegotiationVersion.AUTH_PASSWORD.byteValue());
        buf.writeByte(LEN);
        writeAscii(buf, LEN, 'u');
        buf.writeByte(3);
        writeAscii(buf, 3, 'p');
        e.writeInbound(buf);
        SocksAuthRequest req = assertInstanceOf(SocksAuthRequest.class, e.readInbound());
        assertEquals(repeat(LEN, 'u'), req.username());
        assertEquals("ppp", req.password());
        assertNull(e.readInbound());
        assertFalse(e.finish());
    }

    @Test
    public void authRequestPasswordLongerThan127() {
        EmbeddedChannel e = new EmbeddedChannel(new SocksAuthRequestDecoder());
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(SocksSubnegotiationVersion.AUTH_PASSWORD.byteValue());
        buf.writeByte(3);
        writeAscii(buf, 3, 'u');
        buf.writeByte(LEN);
        writeAscii(buf, LEN, 'p');
        e.writeInbound(buf);
        SocksAuthRequest req = assertInstanceOf(SocksAuthRequest.class, e.readInbound());
        assertEquals(repeat(LEN, 'p'), req.password());
        assertNull(e.readInbound());
        assertFalse(e.finish());
    }

    @Test
    public void cmdRequestDomainLongerThan127() {
        EmbeddedChannel e = new EmbeddedChannel(new SocksCmdRequestDecoder());
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(SocksProtocolVersion.SOCKS5.byteValue());
        buf.writeByte(SocksCmdType.CONNECT.byteValue());
        buf.writeByte(0);
        buf.writeByte(SocksAddressType.DOMAIN.byteValue());
        buf.writeByte(DOMAIN.length());
        buf.writeCharSequence(DOMAIN, io.netty.util.CharsetUtil.US_ASCII);
        buf.writeShort(443);
        e.writeInbound(buf);
        SocksCmdRequest req = assertInstanceOf(SocksCmdRequest.class, e.readInbound());
        assertEquals(DOMAIN, req.host());
        assertEquals(443, req.port());
        assertNull(e.readInbound());
        assertFalse(e.finish());
    }

    @Test
    public void cmdResponseDomainLongerThan127() {
        EmbeddedChannel e = new EmbeddedChannel(new SocksCmdResponseDecoder());
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(SocksProtocolVersion.SOCKS5.byteValue());
        buf.writeByte(SocksCmdStatus.SUCCESS.byteValue());
        buf.writeByte(0);
        buf.writeByte(SocksAddressType.DOMAIN.byteValue());
        buf.writeByte(DOMAIN.length());
        buf.writeCharSequence(DOMAIN, io.netty.util.CharsetUtil.US_ASCII);
        buf.writeShort(443);
        e.writeInbound(buf);
        SocksCmdResponse res = assertInstanceOf(SocksCmdResponse.class, e.readInbound());
        assertEquals(DOMAIN, res.host());
        assertEquals(443, res.port());
        assertNull(e.readInbound());
        assertFalse(e.finish());
    }
}
