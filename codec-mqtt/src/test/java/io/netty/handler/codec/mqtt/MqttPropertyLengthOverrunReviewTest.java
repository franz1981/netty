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
package io.netty.handler.codec.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Review test for GHSA-5qc7-jv7r-4744: every MQTT 5 packet type that carries a Properties field must reject a
 * property whose value runs past the declared Property Length, while the same packet with a correct Property
 * Length must still decode. Property used: User Property ("k", "v") = 7 bytes, or a 4-byte integer property.
 */
public class MqttPropertyLengthOverrunReviewTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "CONNECT", "CONNECT_WILL", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
        "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "DISCONNECT", "AUTH"
    })
    public void wellFormedDecodes(String type) {
        // control: proves the packet builder produces valid packets
        MqttMessage msg = decode(type, 0, false);
        try {
            assertFalse(msg.decoderResult().isFailure(), type + ": " + msg.decoderResult());
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CONNECT", "CONNECT_WILL", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
        "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "DISCONNECT", "AUTH"
    })
    public void userPropertyOverrunIsRejected(String type) {
        MqttMessage msg = decode(type, -2, false);
        try {
            assertTrue(msg.decoderResult().isFailure(), type + " accepted overrun: " + msg);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CONNECT", "CONNECT_WILL", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
        "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "DISCONNECT", "AUTH"
    })
    public void fourByteIntegerPropertyOverrunIsRejected(String type) {
        MqttMessage msg = decode(type, -3, true);
        try {
            assertTrue(msg.decoderResult().isFailure(), type + " accepted overrun: " + msg);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private static MqttMessage decode(String type, int declaredDelta, boolean intProperty) {
        EmbeddedChannel ch = new EmbeddedChannel(new MqttDecoder());
        try {
            if (!type.startsWith("CONNECT")) {
                assertTrue(ch.writeInbound(connect(false, Unpooled.buffer().writeByte(0), Unpooled.EMPTY_BUFFER)));
                MqttMessage c = ch.readInbound();
                assertFalse(c.decoderResult().isFailure());
                ReferenceCountUtil.release(c);
            }
            ByteBuf props = Unpooled.buffer();
            ByteBuf content = Unpooled.buffer();
            if (intProperty) {
                content.writeByte(MqttProperties.WILL_DELAY_INTERVAL); // 4-byte int, 5 bytes total
                content.writeInt(7);
            } else {
                content.writeByte(MqttProperties.USER_PROPERTY);
                str(content, "k");
                str(content, "v");
            }
            vbi(props, content.readableBytes() + declaredDelta);
            props.writeBytes(content);
            content.release();

            ByteBuf packet;
            switch (type) {
                case "CONNECT":
                    packet = connect(false, props, Unpooled.EMPTY_BUFFER);
                    props = null;
                    break;
                case "CONNECT_WILL": {
                    packet = connect(true, Unpooled.buffer().writeByte(0), props);
                    props = null;
                    break;
                }
                case "CONNACK": {
                    ByteBuf v = Unpooled.buffer().writeByte(0).writeByte(0).writeBytes(props);
                    packet = fixed(0x20, v);
                    break;
                }
                case "PUBLISH": {
                    ByteBuf v = Unpooled.buffer();
                    str(v, "a");
                    v.writeBytes(props);
                    v.writeBytes("payload".getBytes(CharsetUtil.US_ASCII));
                    packet = fixed(0x30, v);
                    break;
                }
                case "PUBACK":
                case "PUBREC":
                case "PUBREL":
                case "PUBCOMP": {
                    int b = "PUBACK".equals(type) ? 0x40 : "PUBREC".equals(type) ? 0x50
                            : "PUBREL".equals(type) ? 0x62 : 0x70;
                    ByteBuf v = Unpooled.buffer().writeShort(1).writeByte(0).writeBytes(props);
                    packet = fixed(b, v);
                    break;
                }
                case "SUBSCRIBE": {
                    ByteBuf v = Unpooled.buffer().writeShort(1).writeBytes(props);
                    str(v, "a/b");
                    v.writeByte(0);
                    packet = fixed(0x82, v);
                    break;
                }
                case "UNSUBSCRIBE": {
                    ByteBuf v = Unpooled.buffer().writeShort(1).writeBytes(props);
                    str(v, "a/b");
                    packet = fixed(0xA2, v);
                    break;
                }
                case "SUBACK": {
                    ByteBuf v = Unpooled.buffer().writeShort(1).writeBytes(props).writeByte(0);
                    packet = fixed(0x90, v);
                    break;
                }
                case "UNSUBACK": {
                    ByteBuf v = Unpooled.buffer().writeShort(1).writeBytes(props).writeByte(0);
                    packet = fixed(0xB0, v);
                    break;
                }
                case "DISCONNECT": {
                    ByteBuf v = Unpooled.buffer().writeByte(0).writeBytes(props);
                    packet = fixed(0xE0, v);
                    break;
                }
                case "AUTH": {
                    ByteBuf v = Unpooled.buffer().writeByte(0).writeBytes(props);
                    packet = fixed(0xF0, v);
                    break;
                }
                default:
                    throw new AssertionError(type);
            }
            if (props != null) {
                props.release();
            }
            ch.writeInbound(packet);
            MqttMessage msg = ch.readInbound();
            assertNotNull(msg, type);
            return msg;
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    // MQTT 5 CONNECT. connectProps is the full properties block (length + content); willProps likewise.
    private static ByteBuf connect(boolean will, ByteBuf connectProps, ByteBuf willProps) {
        ByteBuf v = Unpooled.buffer();
        str(v, "MQTT");
        v.writeByte(5);
        v.writeByte(will ? 0x06 : 0x02); // clean start (+ will flag, will QoS 0)
        v.writeShort(0);
        v.writeBytes(connectProps);
        str(v, "c");                      // client id
        if (will) {
            v.writeBytes(willProps);
            str(v, "w/t");                // will topic
            v.writeShort(1).writeByte('m'); // will payload
        }
        connectProps.release();
        willProps.release();
        return fixed(0x10, v);
    }

    private static ByteBuf fixed(int firstByte, ByteBuf variable) {
        ByteBuf out = Unpooled.buffer();
        out.writeByte(firstByte);
        vbi(out, variable.readableBytes());
        out.writeBytes(variable);
        variable.release();
        return out;
    }

    private static void str(ByteBuf buf, String s) {
        byte[] b = s.getBytes(CharsetUtil.UTF_8);
        buf.writeShort(b.length);
        buf.writeBytes(b);
    }

    private static void vbi(ByteBuf buf, int value) {
        do {
            int digit = value & 0x7F;
            value >>>= 7;
            if (value > 0) {
                digit |= 0x80;
            }
            buf.writeByte(digit);
        } while (value > 0);
    }
}
