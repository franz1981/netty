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

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A real {@link MmapRegionSource} that lets a test act at the points where the store calls it: the mapping of a
 * region, the purge of free slices or of a released run, and the unmap of a region. A callback runs before the call,
 * on the calling thread, which holds what the store holds there; a test uses it to make another thread meet that
 * thread at that point. It records the first failure of a hook, which the store would report as a failed mapping or
 * purge: a test calls {@link #assertNoHookFailure()} when it is over.
 */
final class HookedRegionSource extends MmapRegionSource {
    private final AtomicReference<Throwable> failNextMap = new AtomicReference<Throwable>();
    private final AtomicReference<Throwable> failNextPurge = new AtomicReference<Throwable>();
    private final AtomicReference<Throwable> failNextUnmap = new AtomicReference<Throwable>();
    private final AtomicReference<Throwable> hookFailure = new AtomicReference<Throwable>();
    private volatile Runnable onMap;
    private volatile Runnable onPurge;

    HookedRegionSource(ByteBufAllocator allocator) {
        super(allocator);
    }

    /** The next mapping of a region fails with a {@link NativeCallException}, once. */
    void failNextMap() {
        failNextMap.set(new NativeCallException("mapping failed on purpose"));
    }

    /** The next mapping of a region fails with {@code error}, once; that is not a hook failure. */
    void failNextMap(Error error) {
        failNextMap.set(error);
    }

    /** The next purge call fails with a {@link NativeCallException}, once, before it is made. */
    void failNextPurge() {
        failNextPurge.set(new NativeCallException("purge failed on purpose"));
    }

    /** The next unmap of a region is done, then fails with a {@link NativeCallException}, once. */
    void failNextUnmap() {
        failNextUnmap.set(new NativeCallException("unmapping failed on purpose"));
    }

    /** {@code hook}, or {@code null}, runs inside every mapping of a region before it is made. */
    void onMap(Runnable hook) {
        onMap = hook;
    }

    /** {@code hook}, or {@code null}, runs inside every purge call before it is made. */
    void onPurge(Runnable hook) {
        onPurge = hook;
    }

    /** Fails with the first {@link Throwable} a hook threw, if one did. */
    void assertNoHookFailure() {
        Throwable t = hookFailure.get();
        if (t != null) {
            throw new AssertionError("a hook threw", t);
        }
    }

    private void run(Runnable hook) {
        try {
            hook.run();
        } catch (Throwable t) {
            hookFailure.compareAndSet(null, t);
            throw t;
        }
    }

    @Override
    AbstractByteBuf allocateRegion(int size, int alignment) {
        Runnable hook = onMap;
        if (hook != null) {
            run(hook);
        }
        Throwable failure = failNextMap.getAndSet(null);
        if (failure != null) {
            PlatformDependent.throwException(failure);
        }
        return super.allocateRegion(size, alignment);
    }

    @Override
    void purge(long address, int length) {
        Runnable hook = onPurge;
        if (hook != null) {
            run(hook);
        }
        Throwable failure = failNextPurge.getAndSet(null);
        if (failure != null) {
            PlatformDependent.throwException(failure);
        }
        super.purge(address, length);
    }

    @Override
    void unmap(long address, int length) {
        super.unmap(address, length);
        Throwable failure = failNextUnmap.getAndSet(null);
        if (failure != null) {
            PlatformDependent.throwException(failure);
        }
    }
}
