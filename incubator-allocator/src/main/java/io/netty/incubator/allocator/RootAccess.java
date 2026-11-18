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
package io.netty.incubator.allocator;

/**
 * The root buffer's protected accessors made reachable from this package, as the mimalloc port's adapter does: the
 * root buffers are subclasses of Netty's {@code Unpooled*ByteBuf} that implement this interface, and the pooled
 * buffer casts its root to it on every access. No bounds check, no accessibility check: the caller has done both.
 */
interface RootAccess {
    byte _getByte(int index);
    short _getShort(int index);
    short _getShortLE(int index);
    int _getUnsignedMedium(int index);
    int _getUnsignedMediumLE(int index);
    int _getInt(int index);
    int _getIntLE(int index);
    long _getLong(int index);
    long _getLongLE(int index);
    void _setByte(int index, int value);
    void _setShort(int index, int value);
    void _setShortLE(int index, int value);
    void _setMedium(int index, int value);
    void _setMediumLE(int index, int value);
    void _setInt(int index, int value);
    void _setIntLE(int index, int value);
    void _setLong(int index, long value);
    void _setLongLE(int index, long value);

    /** The address of the root's first byte; 0 for a heap root. Fixed when the root is made. */
    long address();
}
