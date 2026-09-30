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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * A complete request (HEADERS + FIN) whose HEADERS block on a dynamic-table insert that has not arrived yet.
 * The FIN is not an abandonment: once the insert arrives the request must still be decoded.
 */
public class BlockedStreamFinTest {

    @Test
    public void blockedRequestWithFinIsDecodedOnceInsertArrives() throws Exception {
        long maxTableCapacity = 1024;
        int maxBlockedStreams = 1;
        EmbeddedQuicChannel parent = new EmbeddedQuicChannel(true);
        QpackAttributes qpackAttributes = new QpackAttributes(parent, false);
        Http3.setQpackAttributes(parent, qpackAttributes);
        QpackDecoder decoder = new QpackDecoder(maxTableCapacity, maxBlockedStreams);
        decoder.setDynamicTableCapacity(maxTableCapacity);
        QpackEncoderHandler qpackEncoderHandler = new QpackEncoderHandler(maxTableCapacity, decoder);
        EmbeddedQuicStreamChannel encoderStream = (EmbeddedQuicStreamChannel) parent.createStream(
                QuicStreamType.UNIDIRECTIONAL, new ChannelOutboundHandlerAdapter()).get();
        QpackEncoder encoder = new QpackEncoder(QpackSensitivityDetector.NEVER_SENSITIVE);
        QpackDecoderHandler qpackDecoderHandler = new QpackDecoderHandler(encoder);
        EmbeddedQuicStreamChannel decoderStream = (EmbeddedQuicStreamChannel) parent.createStream(
                QuicStreamType.UNIDIRECTIONAL, new ChannelOutboundHandlerAdapter()).get();
        qpackAttributes.whenEncoderStreamAvailable(f -> {
            if (f.isSuccess()) {
                encoder.configureDynamicTable(qpackAttributes, maxTableCapacity, maxBlockedStreams);
            }
        });
        qpackAttributes.encoderStream(encoderStream);
        ReferenceCountUtil.release(encoderStream.readOutbound());
        qpackAttributes.decoderStream(decoderStream);

        EmbeddedQuicStreamChannel stream = (EmbeddedQuicStreamChannel) parent.createStream(
                QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        Http3RequestStreamEncodeStateValidator enc = new Http3RequestStreamEncodeStateValidator();
                        Http3RequestStreamDecodeStateValidator dec = new Http3RequestStreamDecodeStateValidator();
                        ch.pipeline().addLast(new Http3FrameCodec(Http3FrameTypeValidator.NO_VALIDATION, decoder,
                                Long.MAX_VALUE, Integer.MAX_VALUE, encoder, enc, dec, (id, v) -> false));
                        ch.pipeline().addLast(enc);
                        ch.pipeline().addLast(dec);
                        ch.pipeline().addLast(
                                Http3RequestStreamValidationHandler.newServerValidator(qpackAttributes, decoder,
                                        enc, dec));
                    }
                }).get();

        // HEADERS frame, header block referencing dynamic entry 0 (Required Insert Count 1) plus static
        // :method GET (17), :scheme https (23), :path / (1) and :authority (0) with a literal value.
        ByteBuf block = Unpooled.buffer();
        block.writeByte(0x02).writeByte(0x00);                  // RIC = 1, delta base 0
        block.writeByte(0xC0 | 17).writeByte(0xC0 | 23).writeByte(0xC0 | 1);
        block.writeByte(0x50).writeByte(11).writeBytes("example.com".getBytes(CharsetUtil.US_ASCII));
        block.writeByte(0x80);                                  // dynamic, relative index 0
        ByteBuf wire = Unpooled.buffer();
        Http3CodecUtils.writeVariableLengthInteger(wire, Http3CodecUtils.HTTP3_HEADERS_FRAME_TYPE);
        Http3CodecUtils.writeVariableLengthInteger(wire, block.readableBytes());
        wire.writeBytes(block);
        block.release();

        // HEADERS arrive and block.
        assertFalse(stream.writeInbound(wire), "precondition: HEADERS must block");
        // The peer's FIN for a complete request: input shutdown.
        stream.pipeline().fireUserEventTriggered(ChannelInputShutdownReadComplete.INSTANCE);
        // Now the peer's encoder instruction arrives: Insert With Literal Name "x-a: b".
        ByteBuf insert = Unpooled.buffer();
        insert.writeByte(0x40 | 3).writeBytes("x-a".getBytes(CharsetUtil.US_ASCII));
        insert.writeByte(1).writeBytes("b".getBytes(CharsetUtil.US_ASCII));
        qpackEncoderHandler.channelRead(encoderStream.pipeline().firstContext(), insert);

        Object decoded = stream.readInbound();
        try {
            assertInstanceOf(Http3HeadersFrame.class, decoded, "request lost after FIN while blocked");
        } finally {
            ReferenceCountUtil.release(decoded);
            stream.finishAndReleaseAll();
            decoderStream.finishAndReleaseAll();
            encoderStream.finishAndReleaseAll();
            parent.finishAndReleaseAll();
        }
    }
}
