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

package io.netty.buffer.internal;

import io.netty.buffer.AbstractByteBuf;
import io.netty.util.internal.UnstableApi;

import java.nio.ByteBuffer;

/**
 * Access for the user-defined allocator of {@code netty-incubator-allocator} only. Not an API: it can change or
 * disappear in any release, and on the module path it is exported only to that module.
 */
@UnstableApi
public abstract class CustomAllocatorAccess {

    // Not volatile: the only intended write is in AbstractByteBuf's static initializer, and a class is initialized
    // before any subclass's initializer or any use of it (JLS 12.4.2), so those readers see it.
    private static CustomAllocatorAccess access;

    protected CustomAllocatorAccess() {
    }

    /**
     * Called once by {@link AbstractByteBuf}'s static initializer; a second call throws.
     */
    public static void set(CustomAllocatorAccess newAccess) {
        if (newAccess == null) {
            throw new NullPointerException("newAccess");
        }
        if (access != null) {
            throw new IllegalStateException("CustomAllocatorAccess is already set");
        }
        access = newAccess;
    }

    /**
     * Not null once {@link AbstractByteBuf} is initialized, so not null when read from the static initializer of one
     * of its subclasses or later. A thread that does not initialize or use {@link AbstractByteBuf} first may see null.
     */
    public static CustomAllocatorAccess get() {
        return access;
    }

    /**
     * The internal NIO buffer of {@code buffer} over its whole capacity, cached, with no state change. Only valid when
     * {@code buffer} is an {@code UnpooledDirectByteBuf} or an {@code UnpooledHeapByteBuf}, or a subclass of one: for
     * any other buffer the result is undefined (it can be a view at another offset, and its position and limit are
     * changed). It is shared by all callers: use absolute get and put only, never change its position, limit or mark.
     */
    public abstract ByteBuffer internalNioBuffer(AbstractByteBuf buffer);
}
