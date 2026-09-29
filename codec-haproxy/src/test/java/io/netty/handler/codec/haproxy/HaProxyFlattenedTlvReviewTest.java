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
package io.netty.handler.codec.haproxy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyTLV.Type;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.netty.handler.codec.haproxy.HAProxyMessageEncoder.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Review tests for GHSA-frq3-7pw6-36cg. The first three are chrisvest's tests from his 2026-09-22 review,
 * unchanged apart from dropping the {@code @Nonnull} annotation. A message built through the public
 * constructors holds an SSL TLV whose children are NOT repeated in {@code tlvs}; the PR's count-based skip
 * then drops the TLVs that follow the SSL TLV.
 */
public class HaProxyFlattenedTlvReviewTest {

    private static final int V2_HEADER_BYTES_LENGTH = 16;
    private static final int IPv4_ADDRESS_BYTES_LENGTH = 12;

    @Test
    public void testTlvFollowingSslTlvIsNotDropped() {
        EmbeddedChannel encCh = new EmbeddedChannel(INSTANCE);
        HAProxyMessage message = buildNestedTlvMessage(encCh.alloc());
        assertTrue(encCh.writeOutbound(message));

        ByteBuf byteBuf = encCh.readOutbound();
        try {
            int declaredLength = byteBuf.getUnsignedShort(14);
            assertEquals(byteBuf.readableBytes() - V2_HEADER_BYTES_LENGTH, declaredLength);
            assertEquals(IPv4_ADDRESS_BYTES_LENGTH + 25 + 14, declaredLength);

            ByteBuf tlv = byteBuf.slice(
                V2_HEADER_BYTES_LENGTH + IPv4_ADDRESS_BYTES_LENGTH,
                declaredLength - IPv4_ADDRESS_BYTES_LENGTH);

            assertEquals((byte) 0x20, tlv.readByte());
            assertEquals(22, tlv.readUnsignedShort());
            assertEquals((byte) 0x01, tlv.readByte());
            assertEquals(0, tlv.readInt());

            assertEquals((byte) 0x21, tlv.readByte());
            assertEquals(7, tlv.readUnsignedShort());
            assertEquals("TLSv1.3", tlv.readSlice(7).toString(CharsetUtil.US_ASCII));

            assertEquals((byte) 0x22, tlv.readByte());
            assertEquals(4, tlv.readUnsignedShort());
            assertEquals("LEAF", tlv.readSlice(4).toString(CharsetUtil.US_ASCII));

            assertEquals((byte) 0x02, tlv.readByte());
            assertEquals(11, tlv.readUnsignedShort());
            assertEquals("example.com", tlv.readSlice(11).toString(CharsetUtil.US_ASCII));

            assertFalse(tlv.isReadable());
        } finally {
            byteBuf.release();
            encCh.finishAndReleaseAll();
        }
    }

    @Test
    public void deeplyNestedTlvEncodeDecodeRoundTrip() {
        EmbeddedChannel encCh = new EmbeddedChannel(INSTANCE);
        HAProxyMessage message = buildNestedTlvMessage(encCh.alloc());
        assertTrue(encCh.writeOutbound(message));

        ByteBuf byteBuf = encCh.readOutbound();
        try {
            EmbeddedChannel decCh = EmbeddedChannel.builder()
                .handlers(new HAProxyMessageDecoder())
                .config(encCh.config())
                .build();
            decCh.writeInbound(byteBuf.retainedDuplicate());
            HAProxyMessage decodedMessage = decCh.readInbound();
            HAProxyMessage messageToCompare = buildNestedTlvMessage(encCh.alloc());
            try {
                assertEquals(decodedMessage, messageToCompare);
            } finally {
                messageToCompare.release();
                decodedMessage.release();
                decCh.finish();
            }
        } finally {
            encCh.finish();
        }
    }

    @Test
    public void tlvNumBytesIncludesTlvFollowingSslTlv() {
        HAProxyMessage message = buildNestedTlvMessage(ByteBufAllocator.DEFAULT);
        try {
            assertEquals(25 + 14, message.tlvNumBytes());
        } finally {
            message.release();
        }
    }

    /**
     * Wire-level check without chrisvest's byte-by-byte asserts: re-decode the encoder output and look for the
     * AUTHORITY TLV that the caller put in the message.
     */
    @Test
    public void authorityAfterUserBuiltSslTlvSurvivesEncodeDecode() {
        EmbeddedChannel encCh = new EmbeddedChannel(INSTANCE);
        assertTrue(encCh.writeOutbound(buildNestedTlvMessage(encCh.alloc())));
        ByteBuf encoded = encCh.readOutbound();
        HAProxyMessage decoded = HAProxyMessage.decodeHeader(encoded);
        try {
            boolean found = false;
            for (HAProxyTLV tlv : decoded.tlvs()) {
                if (tlv.type() == Type.PP2_TYPE_AUTHORITY) {
                    found = true;
                    assertEquals("example.com", tlv.content().toString(CharsetUtil.US_ASCII));
                }
            }
            assertTrue(found, "AUTHORITY TLV missing after encode: " + decoded.tlvs());
        } finally {
            decoded.release();
            encoded.release();
            encCh.finishAndReleaseAll();
        }
    }

    /**
     * chrisvest's "BUG??" note: releasing a message built with the public constructors does not release the
     * SSL TLV's children, because releaseTlvs() assumes they were flattened into tlvs(). Pre-existing on 4.2
     * (the PR does not touch releaseTlvs); included to classify it.
     */
    @Test
    public void releasingUserBuiltMessageReleasesSslChildren() {
        HAProxyTLV versionTlv = new HAProxyTLV(Type.PP2_TYPE_SSL_VERSION,
                Unpooled.buffer().writeBytes("TLSv1.3".getBytes(CharsetUtil.US_ASCII)));
        HAProxySSLTLV sslTlv = new HAProxySSLTLV(0, (byte) 0x01,
                Collections.singletonList(versionTlv));
        HAProxyMessage message = new HAProxyMessage(
                HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                "192.168.0.1", "192.168.0.11", 56324, 443, Collections.singletonList(sslTlv));
        assertTrue(message.release());
        int refCnt = versionTlv.refCnt();
        if (refCnt > 0) {
            versionTlv.release(refCnt);
        }
        assertEquals(0, refCnt);
    }

    private static HAProxyMessage buildNestedTlvMessage(ByteBufAllocator alloc) {
        HAProxyTLV versionTlv = new HAProxyTLV(
            Type.PP2_TYPE_SSL_VERSION, alloc.buffer().writeBytes("TLSv1.3".getBytes(CharsetUtil.US_ASCII)));
        HAProxyTLV cnTlv = new HAProxyTLV(
            Type.PP2_TYPE_SSL_CN, alloc.buffer().writeBytes("LEAF".getBytes(CharsetUtil.US_ASCII)));

        HAProxySSLTLV sslTlv = new HAProxySSLTLV(0, (byte) 0x01,
            Arrays.asList(versionTlv, cnTlv));
        HAProxyTLV authorityTlv = new HAProxyTLV(
            Type.PP2_TYPE_AUTHORITY, alloc.buffer().writeBytes("example.com".getBytes(CharsetUtil.US_ASCII)));

        return new HAProxyMessage(
            HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "192.168.0.1", "192.168.0.11", 56324, 443, Arrays.asList(sslTlv, authorityTlv));
    }
}
