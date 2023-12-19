/*
 * Copyright 2014 The Netty Project
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

import io.netty.handler.codec.Headers;
import io.netty.util.AsciiString;
import io.netty.util.internal.UnstableApi;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * A collection of headers sent or received via HTTP/2.
 */
@UnstableApi
public interface Http2Headers extends Headers<CharSequence, CharSequence, Http2Headers> {

    /**
     * HTTP/2 pseudo-headers names.
     */
    enum PseudoHeaderName {
        /**
         * {@code :method}.
         */
        METHOD(":method", true),

        /**
         * {@code :scheme}.
         */
        SCHEME(":scheme", true),

        /**
         * {@code :authority}.
         */
        AUTHORITY(":authority", true),

        /**
         * {@code :path}.
         */
        PATH(":path", true),

        /**
         * {@code :status}.
         */
        STATUS(":status", false),

        /**
         * {@code :protocol}, as defined in <a href="https://datatracker.ietf.org/doc/rfc8441/">RFC 8441,
         * Bootstrapping WebSockets with HTTP/2</a>.
         */
        PROTOCOL(":protocol", true);

        private static final char PSEUDO_HEADER_PREFIX = ':';
        private static final byte PSEUDO_HEADER_PREFIX_BYTE = (byte) PSEUDO_HEADER_PREFIX;

        private final AsciiString value;
        private final boolean requestOnly;

        PseudoHeaderName(String value, boolean requestOnly) {
            this.value = AsciiString.cached(value);
            this.requestOnly = requestOnly;
        }

        public AsciiString value() {
            // Return a slice so that the buffer gets its own reader index.
            return value;
        }

        /**
         * Indicates whether the specified header follows the pseudo-header format (begins with ':' character)
         *
         * @return {@code true} if the header follow the pseudo-header format
         */
        public static boolean hasPseudoHeaderFormat(CharSequence headerName) {
            if (headerName instanceof AsciiString) {
                final AsciiString asciiHeaderName = (AsciiString) headerName;
                return asciiHeaderName.length() > 0 && asciiHeaderName.byteAt(0) == PSEUDO_HEADER_PREFIX_BYTE;
            } else {
                return headerName.length() > 0 && headerName.charAt(0) == PSEUDO_HEADER_PREFIX;
            }
        }

        /**
         * Indicates whether the given header name is a valid HTTP/2 pseudo header.
         */
        public static boolean isPseudoHeader(CharSequence header) {
            return getPseudoHeader(header) != null;
        }

        /**
         * Indicates whether the given header name is a valid HTTP/2 pseudo header.
         */
        public static boolean isPseudoHeader(AsciiString header) {
            return getPseudoHeader(header) != null;
        }

        /**
         * Indicates whether the given header name is a valid HTTP/2 pseudo header.
         */
        public static boolean isPseudoHeader(String header) {
            return getPseudoHeader(header) != null;
        }

        /**
         * Returns the {@link PseudoHeaderName} corresponding to the specified header name.
         *
         * @return corresponding {@link PseudoHeaderName} if any, {@code null} otherwise.
         */
        public static PseudoHeaderName getPseudoHeader(CharSequence header) {
            if (header instanceof AsciiString) {
                return getPseudoHeader((AsciiString) header);
            }
            if (PSEUDO_STRING_HASHCODES_VALIDATED && header instanceof String) {
                return getPseudoHeader((String) header);
            }
            return getPseudoHeaderName(header);
        }

        private static PseudoHeaderName getPseudoHeaderName(CharSequence header) {
            if (header.length() > 0 && header.charAt(0) == PSEUDO_HEADER_PREFIX) {
                switch (header.length()) {
                case 5:
                    // :path
                    if (":path".contentEquals(header)) {
                        return PATH;
                    }
                    return null;
                case 7:
                    // :method, :scheme, :status
                    if (":method".contentEquals(header)) {
                        return METHOD;
                    }
                    if (":scheme".contentEquals(header)) {
                        return SCHEME;
                    }
                    if (":status".contentEquals(header)) {
                        return STATUS;
                    }
                    return null;
                case 9:
                    // :protocol
                    if (":protocol".contentEquals(header)) {
                        return PROTOCOL;
                    }
                    return null;
                case 10:
                    // :authority
                    if (":authority".contentEquals(header)) {
                        return AUTHORITY;
                    }
                    return null;
                }
            }
            return null;
        }

        // These methods below are factorized separately to allow the JIT to inline them and its caller
        private static PseudoHeaderName getPathIfEqualsNoPrefix(byte[] array, int offset) {
            if (array[offset + 1] == 'p' && array[offset + 2] == 'a' &&
                array[offset + 3] == 't' && array[offset + 4] == 'h') {
                return PATH;
            }
            return null;
        }

        private static PseudoHeaderName getMethodSchemeStatusIfEqualsNoPrefix(byte[] array, int offset) {
            byte first = array[offset + 1];
            if (first == 'm') {
                if (array[offset + 2] == 'e' && array[offset + 3] == 't' && array[offset + 4] == 'h' &&
                    array[offset + 5] == 'o' && array[offset + 6] == 'd') {
                    return METHOD;
                }
            } else if (first == 's') {
                if (array[offset + 2] == 'c' && array[offset + 3] == 'h' && array[offset + 4] == 'e' &&
                    array[offset + 5] == 'm' && array[offset + 6] == 'e') {
                    return SCHEME;
                } else if (array[offset + 2] == 't' && array[offset + 3] == 'a' && array[offset + 4] == 't' &&
                           array[offset + 5] == 'u' && array[offset + 6] == 's') {
                    return STATUS;
                }
            }
            return null;
        }

        private static PseudoHeaderName getAuthorityIfEqualsNoPrefix(byte[] array, int offset) {
            if (array[offset + 1] == 'a' && array[offset + 2] == 'u' && array[offset + 3] == 't' &&
                array[offset + 4] == 'h' && array[offset + 5] == 'o' && array[offset + 6] == 'r' &&
                array[offset + 7] == 'i' && array[offset + 8] == 't' && array[offset + 9] == 'y') {
                return AUTHORITY;
            }
            return null;
        }

        private static PseudoHeaderName getProtocolIfEqualsNoPrefix(byte[] array, int offset) {
            if (array[offset + 1] == 'p' && array[offset + 2] == 'r' && array[offset + 3] == 'o' &&
                array[offset + 4] == 't' && array[offset + 5] == 'o' && array[offset + 6] == 'c' &&
                array[offset + 7] == 'o' && array[offset + 8] == 'l') {
                return PROTOCOL;
            }
            return null;
        }

        /**
         * Returns the {@link PseudoHeaderName} corresponding to the specified header name.
         *
         * @return corresponding {@link PseudoHeaderName} if any, {@code null} otherwise.
         */
        public static PseudoHeaderName getPseudoHeader(AsciiString header) {
            int length = header.length();
            if (length == 0) {
                return null;
            }
            byte[] array = header.array();
            int offset = header.arrayOffset();
            if (array[offset] == PSEUDO_HEADER_PREFIX_BYTE) {
                switch (length) {
                case 5:
                    if (header == PATH.value()) {
                        return PATH;
                    }
                    // :path
                    return getPathIfEqualsNoPrefix(array, offset);
                case 7:
                    if (header == METHOD.value()) {
                        return METHOD;
                    }
                    if (header == SCHEME.value()) {
                        return SCHEME;
                    }
                    if (header == STATUS.value()) {
                        return STATUS;
                    }
                    // :method, :scheme, :status
                    return getMethodSchemeStatusIfEqualsNoPrefix(array, offset);
                case 9:
                    if (header == PROTOCOL.value()) {
                        return PROTOCOL;
                    }
                    // :protocol
                    return getProtocolIfEqualsNoPrefix(array, offset);
                case 10:
                    if (header == AUTHORITY.value()) {
                        return AUTHORITY;
                    }
                    // :authority
                    return getAuthorityIfEqualsNoPrefix(array, offset);
                }
            }
            return null;
        }
        static final int METHOD_STRING_HASHCODE = -1141949029;
        static final int SCHEME_STRING_HASHCODE = -972381601;
        static final int STATUS_STRING_HASHCODE = -956875604;
        static final int PATH_STRING_HASHCODE = 56997727;
        static final int AUTHORITY_STRING_HASHCODE = -1332238263;
        static final int PROTOCOL_STRING_HASHCODE = 1552659666;
        private static final boolean PSEUDO_STRING_HASHCODES_VALIDATED;

        static {
            boolean trustHashCode = true;
            // hashCode is trust if the subsequent checks work as expected
            trustHashCode = trustHashCode && METHOD.value().toString().hashCode() == METHOD_STRING_HASHCODE;
            trustHashCode = trustHashCode && SCHEME.value().toString().hashCode() == SCHEME_STRING_HASHCODE;
            trustHashCode = trustHashCode && STATUS.value().toString().hashCode() == STATUS_STRING_HASHCODE;
            trustHashCode = trustHashCode && PATH.value().toString().hashCode() == PATH_STRING_HASHCODE;
            trustHashCode = trustHashCode && AUTHORITY.value().toString().hashCode() == AUTHORITY_STRING_HASHCODE;
            trustHashCode = trustHashCode && PROTOCOL.value().toString().hashCode() == PROTOCOL_STRING_HASHCODE;
            PSEUDO_STRING_HASHCODES_VALIDATED = trustHashCode;
        }

        /**
         * Returns the {@link PseudoHeaderName} corresponding to the specified header name.
         *
         * @return corresponding {@link PseudoHeaderName} if any, {@code null} otherwise.
         */
        public static PseudoHeaderName getPseudoHeader(String header) {
            if (!PSEUDO_STRING_HASHCODES_VALIDATED) {
                return getPseudoHeaderName(header);
            }
            // String::hashCode is intrinsified
            // the cases uses String's literal which are the same used in the enums
            // String::hashCode is a contract which is not going to change
            switch (header.hashCode()) {
            case PATH_STRING_HASHCODE:
                return ":path".equals(header) ? PATH : null;
            case METHOD_STRING_HASHCODE:
                return ":method".equals(header) ? METHOD : null;
            case SCHEME_STRING_HASHCODE:
                return ":scheme".equals(header) ? SCHEME : null;
            case STATUS_STRING_HASHCODE:
                return ":status".equals(header) ? STATUS : null;
            case AUTHORITY_STRING_HASHCODE:
                return ":authority".equals(header) ? AUTHORITY : null;
            case PROTOCOL_STRING_HASHCODE:
                return ":protocol".equals(header) ? PROTOCOL : null;
            }
            return null;
        }

        /**
         * Indicates whether the pseudo-header is to be used in a request context.
         *
         * @return {@code true} if the pseudo-header is to be used in a request context
         */
        public boolean isRequestOnly() {
            return requestOnly;
        }
    }

    /**
     * Returns an iterator over all HTTP/2 headers. The iteration order is as follows:
     *   1. All pseudo headers (order not specified).
     *   2. All non-pseudo headers (in insertion order).
     */
    @Override
    Iterator<Entry<CharSequence, CharSequence>> iterator();

    /**
     * Equivalent to {@link #getAll(Object)} but no intermediate list is generated.
     * @param name the name of the header to retrieve
     * @return an {@link Iterator} of header values corresponding to {@code name}.
     */
    Iterator<CharSequence> valueIterator(CharSequence name);

    /**
     * Sets the {@link PseudoHeaderName#METHOD} header
     */
    Http2Headers method(CharSequence value);

    /**
     * Sets the {@link PseudoHeaderName#SCHEME} header
     */
    Http2Headers scheme(CharSequence value);

    /**
     * Sets the {@link PseudoHeaderName#AUTHORITY} header
     */
    Http2Headers authority(CharSequence value);

    /**
     * Sets the {@link PseudoHeaderName#PATH} header
     */
    Http2Headers path(CharSequence value);

    /**
     * Sets the {@link PseudoHeaderName#STATUS} header
     */
    Http2Headers status(CharSequence value);

    /**
     * Gets the {@link PseudoHeaderName#METHOD} header or {@code null} if there is no such header
     */
    CharSequence method();

    /**
     * Gets the {@link PseudoHeaderName#SCHEME} header or {@code null} if there is no such header
     */
    CharSequence scheme();

    /**
     * Gets the {@link PseudoHeaderName#AUTHORITY} header or {@code null} if there is no such header
     */
    CharSequence authority();

    /**
     * Gets the {@link PseudoHeaderName#PATH} header or {@code null} if there is no such header
     */
    CharSequence path();

    /**
     * Gets the {@link PseudoHeaderName#STATUS} header or {@code null} if there is no such header
     */
    CharSequence status();

    /**
     * Returns {@code true} if a header with the {@code name} and {@code value} exists, {@code false} otherwise.
     * <p>
     * If {@code caseInsensitive} is {@code true} then a case insensitive compare is done on the value.
     *
     * @param name the name of the header to find
     * @param value the value of the header to find
     * @param caseInsensitive {@code true} then a case insensitive compare is run to compare values.
     * otherwise a case sensitive compare is run to compare values.
     */
    boolean contains(CharSequence name, CharSequence value, boolean caseInsensitive);
}
