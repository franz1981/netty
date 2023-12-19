/*
 * Copyright 2023 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName;
import io.netty.util.AsciiString;
import org.junit.jupiter.api.Test;

import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.AUTHORITY;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.AUTHORITY_STRING_HASHCODE;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.METHOD;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.METHOD_STRING_HASHCODE;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.PATH;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.PATH_STRING_HASHCODE;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.PROTOCOL;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.PROTOCOL_STRING_HASHCODE;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.SCHEME;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.SCHEME_STRING_HASHCODE;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.STATUS;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.STATUS_STRING_HASHCODE;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.getPseudoHeader;
import static io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.isPseudoHeader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Http2HeadersTest {

    @Test
    public void testGetPseudoHeader() {
        for (PseudoHeaderName pseudoHeaderName : PseudoHeaderName.values()) {
            assertSame(pseudoHeaderName, getPseudoHeader(pseudoHeaderName.value()));
            assertSame(pseudoHeaderName, getPseudoHeader(new AsciiString(pseudoHeaderName.value().array())));
            assertSame(pseudoHeaderName, getPseudoHeader(pseudoHeaderName.value().toString()));
            assertSame(pseudoHeaderName, getPseudoHeader(new String(pseudoHeaderName.value().toCharArray())));
            assertSame(pseudoHeaderName, getPseudoHeader(new StringBuilder(pseudoHeaderName.value())));
        }
    }

    @Test
    public void testIsPseudoHeader() {
        // same as before but for isPseudoHeader
        for (PseudoHeaderName pseudoHeaderName : PseudoHeaderName.values()) {
            assertTrue(isPseudoHeader(pseudoHeaderName.value()));
            assertTrue(isPseudoHeader(new AsciiString(pseudoHeaderName.value().array())));
            assertTrue(isPseudoHeader(pseudoHeaderName.value().toString()));
            assertTrue(isPseudoHeader(new String(pseudoHeaderName.value().toCharArray())));
            assertTrue(isPseudoHeader(new StringBuilder(pseudoHeaderName.value())));
        }
    }

    @Test
    public void testPseudonameStringHashCodeIsValid() {
        // do the same but throw a runtime exception here
        // this is to ensure that the hashcode of the strings are the same as the hashcode of the enum
        // convert the rest of the code is a proper JUnit test
        assertEquals(METHOD.value().toString().hashCode(), METHOD_STRING_HASHCODE);
        assertEquals(SCHEME.value().toString().hashCode(), SCHEME_STRING_HASHCODE);
        assertEquals(STATUS.value().toString().hashCode(), STATUS_STRING_HASHCODE);
        assertEquals(PATH.value().toString().hashCode(), PATH_STRING_HASHCODE);
        assertEquals(AUTHORITY.value().toString().hashCode(), AUTHORITY_STRING_HASHCODE);
        assertEquals(PROTOCOL.value().toString().hashCode(), PROTOCOL_STRING_HASHCODE);
        // do the same with a new String, which hashcode is not computed yet
        assertEquals(new String(METHOD.value().toString().toCharArray()).hashCode(), METHOD_STRING_HASHCODE);
        assertEquals(new String(SCHEME.value().toString().toCharArray()).hashCode(), SCHEME_STRING_HASHCODE);
        assertEquals(new String(STATUS.value().toString().toCharArray()).hashCode(), STATUS_STRING_HASHCODE);
        assertEquals(new String(PATH.value().toString().toCharArray()).hashCode(), PATH_STRING_HASHCODE);
        assertEquals(new String(AUTHORITY.value().toString().toCharArray()).hashCode(), AUTHORITY_STRING_HASHCODE);
        assertEquals(new String(PROTOCOL.value().toString().toCharArray()).hashCode(), PROTOCOL_STRING_HASHCODE);
    }
}
