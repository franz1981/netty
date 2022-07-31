/*
 * Copyright 2012 The Netty Project
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
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.StringUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static io.netty.buffer.Unpooled.directBuffer;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 * Encodes an {@link HttpMessage} or an {@link HttpContent} into
 * a {@link ByteBuf}.
 *
 * <h3>Extensibility</h3>
 *
 * Please note that this encoder is designed to be extended to implement
 * a protocol derived from HTTP, such as
 * <a href="https://en.wikipedia.org/wiki/Real_Time_Streaming_Protocol">RTSP</a> and
 * <a href="https://en.wikipedia.org/wiki/Internet_Content_Adaptation_Protocol">ICAP</a>.
 * To implement the encoder of such a derived protocol, extend this class and
 * implement all abstract methods properly.
 */
public abstract class HttpObjectEncoder<H extends HttpMessage> extends MessageToMessageEncoder<Object> {
    static final int CRLF_SHORT = (CR << 8) | LF;
    private static final int ZERO_CRLF_MEDIUM = ('0' << 16) | CRLF_SHORT;
    private static final byte[] ZERO_CRLF_CRLF = { '0', CR, LF, CR, LF };
    private static final ByteBuf CRLF_BUF = unreleasableBuffer(
            directBuffer(2).writeByte(CR).writeByte(LF)).asReadOnly();
    private static final ByteBuf ZERO_CRLF_CRLF_BUF = unreleasableBuffer(
            directBuffer(ZERO_CRLF_CRLF.length).writeBytes(ZERO_CRLF_CRLF)).asReadOnly();
    private static final float HEADERS_WEIGHT_NEW = 1 / 5f;
    private static final float HEADERS_WEIGHT_HISTORICAL = 1 - HEADERS_WEIGHT_NEW;
    private static final float TRAILERS_WEIGHT_NEW = HEADERS_WEIGHT_NEW;
    private static final float TRAILERS_WEIGHT_HISTORICAL = HEADERS_WEIGHT_HISTORICAL;

    private static final int ST_INIT = 0;
    private static final int ST_CONTENT_NON_CHUNK = 1;
    private static final int ST_CONTENT_CHUNK = 2;
    private static final int ST_CONTENT_ALWAYS_EMPTY = 3;

    @SuppressWarnings("RedundantFieldInitialization")
    private int state = ST_INIT;

    /**
     * Used to calculate an exponential moving average of the encoded size of the initial line and the headers for
     * a guess for future buffer allocations.
     */
    private float headersEncodedSizeAccumulator = 256;

    /**
     * Used to calculate an exponential moving average of the encoded size of the trailers for
     * a guess for future buffer allocations.
     */
    private float trailersEncodedSizeAccumulator = 256;

    protected H safeCastToHttpMessage(Object msg) {
        if (msg instanceof HttpMessage) {
            return (H) msg;
        }
        return null;
    }

    private static boolean checkContentState(int state) {
        return state == ST_CONTENT_CHUNK || state == ST_CONTENT_NON_CHUNK || state == ST_CONTENT_ALWAYS_EMPTY;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        // fast-path for common idiom that doesn't require class-checks
        if (msg == Unpooled.EMPTY_BUFFER) {
            out.add(Unpooled.EMPTY_BUFFER);
            return;
        }
        if (state == ST_INIT) {
            final H m = safeCastToHttpMessage(msg);
            if (m == null) {
                if (msg instanceof ByteBuf && bypassEncoderIfEmpty((ByteBuf) msg, out)) {
                    return;
                }
                throwUnexpectedMessageTypeEx(msg, ST_INIT);
            }
            final ByteBuf buf = encodeInitHttpMessage(ctx, m);

            assert checkContentState(state);

            if (m instanceof HttpContent) {
                encodeHttpMessageContent(ctx, (HttpContent) m, buf, out);
            } else {
                out.add(buf);
            }
        } else {
            if (msg == LastHttpContent.EMPTY_LAST_CONTENT) {
                encodeEmptyLastHttpContent(out);
                return;
            }
            if (msg instanceof HttpContent) {
                encodeHttpContent(ctx, (HttpContent) msg, out);
                return;
            }
            if (msg instanceof ByteBuf) {
                encodeByteBufContent(ctx, (ByteBuf) msg, out);
                return;
            }
            if (msg instanceof FileRegion) {
                encodeFileRegionContent(ctx, (FileRegion) msg, out);
                return;
            }
            throwUnexpectedMessageTypeEx(msg, state);
        }
    }

    private static void throwUnexpectedMessageTypeEx(Object msg, int state) {
        throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg)
                + ", state: " + state);
    }

    private void encodeFileRegionContent(ChannelHandlerContext ctx, FileRegion msg, List<Object> out) {
        assert state != ST_INIT;
        switch (state) {
            case ST_CONTENT_NON_CHUNK:
                final long contentLength = msg.count();
                if (contentLength > 0) {
                    out.add(msg.retain());
                    break;
                }

                // fall-through!
            case ST_CONTENT_ALWAYS_EMPTY:
                // Need to produce some output otherwise an
                // IllegalStateException will be thrown as we did not write anything
                // Its ok to just write an EMPTY_BUFFER as if there are reference count issues these will be
                // propagated as the caller of the encode(...) method will release the original
                // buffer.
                // Writing an empty buffer will not actually write anything on the wire, so if there is a user
                // error with msg it will not be visible externally
                out.add(Unpooled.EMPTY_BUFFER);
                break;
            case ST_CONTENT_CHUNK:
                encodedChunkedFileRegionContent(ctx, msg, msg.count(), out);
                break;
            default:
                throw new Error();
        }
    }

    // Bypass the encoder in case of an empty buffer, so that the following idiom works:
    //
    //     ch.write(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    //
    // See https://github.com/netty/netty/issues/2983 for more information.
    private static boolean bypassEncoderIfEmpty(ByteBuf msg, List<Object> out) {
        if (!msg.isReadable()) {
            out.add(msg.retain());
            return true;
        }
        return false;
    }

    private void encodeByteBufContent(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        assert state != ST_INIT;
        if (bypassEncoderIfEmpty(msg, out)) {
            return;
        }
        switch (state) {
            case ST_CONTENT_NON_CHUNK:
                final long contentLength = msg.readableBytes();
                if (contentLength > 0) {
                    out.add(msg.retain());
                    break;
                }
                // fall-through!
            case ST_CONTENT_ALWAYS_EMPTY:
                // Need to produce some output otherwise an
                // IllegalStateException will be thrown as we did not write anything
                // Its ok to just write an EMPTY_BUFFER as if there are reference count issues these will be
                // propagated as the caller of the encode(...) method will release the original
                // buffer.
                // Writing an empty buffer will not actually write anything on the wire, so if there is a user
                // error with msg it will not be visible externally
                out.add(Unpooled.EMPTY_BUFFER);
                break;
            case ST_CONTENT_CHUNK:
                encodeChunkedByteBufContent(ctx, msg, msg.readableBytes(), out);
                break;
            default:
                throw new Error();
        }
    }

    private void encodeEmptyLastHttpContent(List<Object> out) {
        assert state != ST_INIT;

        switch (state) {
            case ST_CONTENT_NON_CHUNK:
            case ST_CONTENT_ALWAYS_EMPTY:
                out.add(Unpooled.EMPTY_BUFFER);
                break;
            case ST_CONTENT_CHUNK:
                out.add(ZERO_CRLF_CRLF_BUF.duplicate());
                break;
            default:
                throw new Error();
        }
        state = ST_INIT;
    }

    private void encodeHttpContent(ChannelHandlerContext ctx, HttpContent msg, List<Object> out) {
        assert state != ST_INIT;
        assert !(msg instanceof HttpMessage);
        switch (state) {
            case ST_CONTENT_NON_CHUNK:
                final long contentLength = msg.content().readableBytes();
                if (contentLength > 0) {
                    out.add(msg.content().retain());
                    break;
                }
                // fall-through!
            case ST_CONTENT_ALWAYS_EMPTY:
                // Need to produce some output otherwise an
                // IllegalStateException will be thrown as we did not write anything
                // Its ok to just write an EMPTY_BUFFER as if there are reference count issues these will be
                // propagated as the caller of the encode(...) method will release the original
                // buffer.
                // Writing an empty buffer will not actually write anything on the wire, so if there is a user
                // error with msg it will not be visible externally
                out.add(Unpooled.EMPTY_BUFFER);
                break;
            case ST_CONTENT_CHUNK:
                encodeChunkedHttpContent(ctx, msg, msg.content().readableBytes(), out);
                break;
            default:
                throw new Error();
        }
        if (msg instanceof LastHttpContent) {
            state = ST_INIT;
        }
    }

    private void encodeHttpMessageContent(ChannelHandlerContext ctx, HttpContent msg,
                                          ByteBuf buf, List<Object> out) {
        assert state != ST_INIT;
        // intersection types are not allowed on Java 6 :(
        assert msg instanceof HttpMessage;
        switch (state) {
            case ST_CONTENT_NON_CHUNK:
                final long contentLength = msg.content().readableBytes();
                if (contentLength > 0) {
                    if (buf.writableBytes() >= contentLength) {
                        // merge into other buffer for performance reasons
                        buf.writeBytes(msg.content());
                        out.add(buf);
                    } else {
                        out.add(buf);
                        out.add(msg.content().retain());
                    }
                    break;
                }
                // fall-through!
            case ST_CONTENT_ALWAYS_EMPTY:
                // We allocated a buffer so add it now.
                out.add(buf);
                break;
            case ST_CONTENT_CHUNK:
                // We allocated a buffer so add it now.
                out.add(buf);
                encodeChunkedHttpContent(ctx, msg, msg.content().readableBytes(), out);
                break;
            default:
                throw new Error();
        }
        if (msg instanceof LastHttpContent) {
            state = ST_INIT;
        }
    }

    private void encodeChunkedHttpContent(ChannelHandlerContext ctx, HttpContent msg,
                                          long contentLength, List<Object> out) {
        if (contentLength > 0) {
            addEncodedLengthHex(ctx, contentLength, out);
            out.add(msg.content().retain());
            out.add(CRLF_BUF.duplicate());
        }
        if (msg instanceof LastHttpContent) {
            encodeLastHttpContent(ctx, (LastHttpContent) msg, out);
        } else if (contentLength == 0) {
            // Need to produce some output otherwise an
            // IllegalStateException will be thrown
            out.add(msg.content().retain());
        }
    }

    private void encodeLastHttpContent(ChannelHandlerContext ctx, LastHttpContent msg, List<Object> out) {
        HttpHeaders headers = msg.trailingHeaders();
        if (headers.isEmpty()) {
            out.add(ZERO_CRLF_CRLF_BUF.duplicate());
        } else {
            ByteBuf buf = ctx.alloc().buffer((int) trailersEncodedSizeAccumulator);
            ByteBufUtil.writeMediumBE(buf, ZERO_CRLF_MEDIUM);
            encodeHeaders(headers, buf);
            ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
            trailersEncodedSizeAccumulator = TRAILERS_WEIGHT_NEW * padSizeForAccumulation(buf.readableBytes()) +
                    TRAILERS_WEIGHT_HISTORICAL * trailersEncodedSizeAccumulator;
            out.add(buf);
        }
    }

    private ByteBuf encodeInitHttpMessage(ChannelHandlerContext ctx, H m) throws Exception {
        assert state == ST_INIT;

        ByteBuf buf = ctx.alloc().buffer((int) headersEncodedSizeAccumulator);
        // Encode the message.
        encodeInitialLine(buf, m);
        state = isContentAlwaysEmpty(m) ? ST_CONTENT_ALWAYS_EMPTY :
                HttpUtil.isTransferEncodingChunked(m) ? ST_CONTENT_CHUNK : ST_CONTENT_NON_CHUNK;

        sanitizeHeadersBeforeEncode(m, state == ST_CONTENT_ALWAYS_EMPTY);

        encodeHeaders(m.headers(), buf);
        ByteBufUtil.writeShortBE(buf, CRLF_SHORT);

        headersEncodedSizeAccumulator = HEADERS_WEIGHT_NEW * padSizeForAccumulation(buf.readableBytes()) +
                HEADERS_WEIGHT_HISTORICAL * headersEncodedSizeAccumulator;
        return buf;
    }

    /**
     * Encode the {@link HttpHeaders} into a {@link ByteBuf}.
     */
    protected void encodeHeaders(HttpHeaders headers, ByteBuf buf) {
        Iterator<Entry<CharSequence, CharSequence>> iter = headers.iteratorCharSequence();
        while (iter.hasNext()) {
            Entry<CharSequence, CharSequence> header = iter.next();
            HttpHeadersEncoder.encoderHeader(header.getKey(), header.getValue(), buf);
        }
    }

    private static void encodedChunkedFileRegionContent(ChannelHandlerContext ctx, FileRegion msg,
                                                        long contentLength, List<Object> out) {
        if (contentLength > 0) {
            addEncodedLengthHex(ctx, contentLength, out);
            out.add(msg.retain());
            out.add(CRLF_BUF.duplicate());
        } else if (contentLength == 0) {
            // Need to produce some output otherwise an
            // IllegalStateException will be thrown
            out.add(msg.retain());
        }
    }

    private static void encodeChunkedByteBufContent(ChannelHandlerContext ctx, ByteBuf msg,
                                                    long contentLength, List<Object> out) {
        if (contentLength > 0) {
            addEncodedLengthHex(ctx, contentLength, out);
            out.add(msg.retain());
            out.add(CRLF_BUF.duplicate());
        } else if (contentLength == 0) {
            // Need to produce some output otherwise an
            // IllegalStateException will be thrown
            out.add(msg.retain());
        }
    }

    private static void addEncodedLengthHex(ChannelHandlerContext ctx, long contentLength, List<Object> out) {
        String lengthHex = Long.toHexString(contentLength);
        ByteBuf buf = ctx.alloc().buffer(lengthHex.length() + 2);
        buf.writeCharSequence(lengthHex, CharsetUtil.US_ASCII);
        ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
        out.add(buf);
    }

    /**
     * Allows to sanitize headers of the message before encoding these.
     */
    protected void sanitizeHeadersBeforeEncode(@SuppressWarnings("unused") H msg, boolean isAlwaysEmpty) {
        // noop
    }

    /**
     * Determine whether a message has a content or not. Some message may have headers indicating
     * a content without having an actual content, e.g the response to an HEAD or CONNECT request.
     *
     * @param msg the message to test
     * @return {@code true} to signal the message has no content
     */
    protected boolean isContentAlwaysEmpty(@SuppressWarnings("unused") H msg) {
        return false;
    }

    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return msg instanceof HttpMessage || msg instanceof HttpContent ||
                msg instanceof ByteBuf || msg instanceof FileRegion;
    }

    /**
     * Add some additional overhead to the buffer. The rational is that it is better to slightly over allocate and waste
     * some memory, rather than under allocate and require a resize/copy.
     * @param readableBytes The readable bytes in the buffer.
     * @return The {@code readableBytes} with some additional padding.
     */
    private static int padSizeForAccumulation(int readableBytes) {
        return (readableBytes << 2) / 3;
    }

    @Deprecated
    protected static void encodeAscii(String s, ByteBuf buf) {
        buf.writeCharSequence(s, CharsetUtil.US_ASCII);
    }

    protected abstract void encodeInitialLine(ByteBuf buf, H message) throws Exception;
}
