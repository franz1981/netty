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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;

/** See {@link RootAccess}. */
final class RootHeapByteBuf extends UnpooledHeapByteBuf implements RootAccess {

    RootHeapByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        super(alloc, initialCapacity, maxCapacity);
    }

    @Override
    public byte _getByte(int index) {
        return super._getByte(index);
    }

    @Override
    public short _getShort(int index) {
        return super._getShort(index);
    }

    @Override
    public short _getShortLE(int index) {
        return super._getShortLE(index);
    }

    @Override
    public int _getUnsignedMedium(int index) {
        return super._getUnsignedMedium(index);
    }

    @Override
    public int _getUnsignedMediumLE(int index) {
        return super._getUnsignedMediumLE(index);
    }

    @Override
    public int _getInt(int index) {
        return super._getInt(index);
    }

    @Override
    public int _getIntLE(int index) {
        return super._getIntLE(index);
    }

    @Override
    public long _getLong(int index) {
        return super._getLong(index);
    }

    @Override
    public long _getLongLE(int index) {
        return super._getLongLE(index);
    }

    @Override
    public void _setByte(int index, int value) {
        super._setByte(index, value);
    }

    @Override
    public void _setShort(int index, int value) {
        super._setShort(index, value);
    }

    @Override
    public void _setShortLE(int index, int value) {
        super._setShortLE(index, value);
    }

    @Override
    public void _setMedium(int index, int value) {
        super._setMedium(index, value);
    }

    @Override
    public void _setMediumLE(int index, int value) {
        super._setMediumLE(index, value);
    }

    @Override
    public void _setInt(int index, int value) {
        super._setInt(index, value);
    }

    @Override
    public void _setIntLE(int index, int value) {
        super._setIntLE(index, value);
    }

    @Override
    public void _setLong(int index, long value) {
        super._setLong(index, value);
    }

    @Override
    public void _setLongLE(int index, long value) {
        super._setLongLE(index, value);
    }

    @Override
    public long address() {
        return 0;
    }
}
