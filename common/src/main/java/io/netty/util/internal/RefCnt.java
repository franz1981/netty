/*
 * Copyright 2025 The Netty Project
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
package io.netty.util.internal;

import io.netty.util.IllegalReferenceCountException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.internal.ObjectUtil.checkPositive;

/**
 * Abstract base class for reference counting implementations.
 * Provides a factory method to create instances using the most efficient available atomic updater.
 */
public abstract class RefCnt {

    /*
     * Implementation notes:
     *
     * For the updated int field:
     *   Even => "real" refcount is (refCnt >>> 1)
     *   Odd  => "real" refcount is 0
     */

    volatile int value;

    private RefCnt() {
    }

    /**
     * Creates a new reference counting instance using the best available updater.
     * The implementation is chosen based on platform capabilities: Unsafe, VarHandle, or Atomic.
     *
     * @return a new RefCnt instance
     */
    public static RefCnt create() {
        long fieldOffset = getUnsafeOffset(RefCnt.class, "value");
        if (fieldOffset >= 0) {
            return new UnsafeRefCnt();
        }
        if (PlatformDependent.hasVarHandle()) {
            return new VarHandleRefCnt();
        }
        return new AtomicRefCnt();
    }

    private static long getUnsafeOffset(Class<?> clz, String fieldName) {
        try {
            if (PlatformDependent.hasUnsafe()) {
                return PlatformDependent.objectFieldOffset(clz.getDeclaredField(fieldName));
            }
        } catch (Throwable ignore) {
            // fall-back
        }
        return -1;
    }

    /**
     * Returns the current reference count of this object with a load acquire semantic.
     *
     * @return the reference count
     */
    public abstract int refCnt();

    /**
     * Increases the reference count by 1.
     */
    public abstract void retain();

    /**
     * Increases the reference count by the specified increment.
     *
     * @param increment the amount to increase the reference count by
     * @throws IllegalArgumentException if increment is not positive
     */
    public abstract void retain(int increment);

    /**
     * Decreases the reference count by 1.
     *
     * @return true if the reference count became 0 and the object should be deallocated
     */
    public abstract boolean release();

    /**
     * Decreases the reference count by the specified decrement.
     *
     * @param decrement the amount to decrease the reference count by
     * @return true if the reference count became 0 and the object should be deallocated
     * @throws IllegalArgumentException if decrement is not positive
     */
    public abstract boolean release(int decrement);

    /**
     * Returns {@code true} if and only if this reference counter is alive.
     * This method is useful to check if the object is alive without incurring the cost of a volatile read.
     */
    public abstract boolean isLiveNonVolatile();

    /**
     * <strong>WARNING:</strong>
     * An unsafe operation that sets the reference count directly.
     */
    public abstract void setRefCnt(int refCnt);

    /**
     * Resets the reference count to 1.
     * <p>
     * <strong>Warning:</strong> This method uses release memory semantics, meaning the change may not be
     * immediately visible to other threads. It should only be used in quiescent states where no other
     * threads are accessing the reference count.
     */
    public abstract void resetRefCnt();

    private static final class AtomicRefCnt extends RefCnt {
        private static final AtomicIntegerFieldUpdater<RefCnt> UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(RefCnt.class, "value");

        AtomicRefCnt() {
            UPDATER.set(this, 2);
        }

        @Override
        public int refCnt() {
            return UPDATER.get(this) >>> 1;
        }

        @Override
        public void retain() {
            retain0(2);
        }

        @Override
        public void retain(int increment) {
            retain0(checkPositive(increment, "increment") << 1);
        }

        private void retain0(int increment) {
            int oldRef = UPDATER.getAndAdd(this, increment);
            if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
                UPDATER.getAndAdd(this, -increment);
                throw new IllegalReferenceCountException(0, increment >>> 1);
            }
        }

        @Override
        public boolean release() {
            return release0(2);
        }

        @Override
        public boolean release(int decrement) {
            return release0(checkPositive(decrement, "decrement") << 1);
        }

        private boolean release0(int decrement) {
            int curr, next;
            do {
                curr = UPDATER.get(this);
                if (curr == decrement) {
                    next = 1;
                } else {
                    if (curr < decrement || (curr & 1) == 1) {
                        throw new IllegalReferenceCountException(curr >>> 1, -(decrement >>> 1));
                    }
                    next = curr - decrement;
                }
            } while (!UPDATER.compareAndSet(this, curr, next));
            return (next & 1) == 1;
        }

        @Override
        void setRefCnt(int refCnt) {
            int rawRefCnt = refCnt > 0? refCnt << 1 : 1;
            UPDATER.lazySet(this, rawRefCnt);
        }

        @Override
        void resetRefCnt() {
            UPDATER.lazySet(this, 2);
        }

        @Override
        public boolean isLiveNonVolatile() {
            final int rawCnt = value;
            if (rawCnt == 2) {
                return true;
            }
            return (rawCnt & 1) == 0;
        }
    }

    private static final class VarHandleRefCnt extends RefCnt {

        private static final VarHandle VH;

        static {
            VH = PlatformDependent.findVarHandleOfIntField(MethodHandles.lookup(), RefCnt.class, "value");
        }

        VarHandleRefCnt() {
            VH.set(this, 2);
            VarHandle.storeStoreFence();
        }

        @Override
        public int refCnt() {
            return (int) VH.getAcquire(this) >>> 1;
        }

        @Override
        public void retain() {
            retain0(2);
        }

        @Override
        public void retain(int increment) {
            retain0(checkPositive(increment, "increment") << 1);
        }

        private void retain0(int increment) {
            int oldRef = (int) VH.getAndAdd(this, increment);
            if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
                VH.getAndAdd(this, -increment);
                throw new IllegalReferenceCountException(0, increment >>> 1);
            }
        }

        @Override
        public boolean release() {
            return release0(2);
        }

        @Override
        public boolean release(int decrement) {
            return release0(checkPositive(decrement, "decrement") << 1);
        }

        private boolean release0(int decrement) {
            int curr, next;
            do {
                curr = (int) VH.get(this);
                if (curr == decrement) {
                    next = 1;
                } else {
                    if (curr < decrement || (curr & 1) == 1) {
                        throw new IllegalReferenceCountException(curr >>> 1, -(decrement >>> 1));
                    }
                    next = curr - decrement;
                }
            } while (!(boolean) VH.compareAndSet(this, curr, next));
            return (next & 1) == 1;
        }

        @Override
        void setRefCnt(int refCnt) {
            int rawRefCnt = refCnt > 0? refCnt << 1 : 1;
            VH.setRelease(this, rawRefCnt);
        }

        @Override
        void resetRefCnt() {
            VH.setRelease(this, 2);
        }

        @Override
        public boolean isLiveNonVolatile() {
            final int rawCnt = value;
            if (rawCnt == 2) {
                return true;
            }
            return (rawCnt & 1) == 0;
        }
    }

    private static final class UnsafeRefCnt extends RefCnt {
        private static final long OFFSET;

        static {
            try {
                OFFSET = PlatformDependent.objectFieldOffset(RefCnt.class.getDeclaredField("value"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        UnsafeRefCnt() {
            PlatformDependent.safeConstructPutInt(this, OFFSET, 2);
        }

        @Override
        public int refCnt() {
            return PlatformDependent.getVolatileInt(this, OFFSET) >>> 1;
        }

        @Override
        public void retain() {
            retain0(2);
        }

        @Override
        public void retain(int increment) {
            retain0(checkPositive(increment, "increment") << 1);
        }

        private void retain0(int increment) {
            int oldRef = PlatformDependent.getAndAddInt(this, OFFSET, increment);
            if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
                PlatformDependent.getAndAddInt(this, OFFSET, -increment);
                throw new IllegalReferenceCountException(0, increment >>> 1);
            }
        }

        @Override
        public boolean release() {
            return release0(2);
        }

        @Override
        public boolean release(int decrement) {
            return release0(checkPositive(decrement, "decrement") << 1);
        }

        private boolean release0(int decrement) {
            int curr, next;
            do {
                curr = PlatformDependent.getInt(this, OFFSET);
                if (curr == decrement) {
                    next = 1;
                } else {
                    if (curr < decrement || (curr & 1) == 1) {
                        throw new IllegalReferenceCountException(curr >>> 1, -(decrement >>> 1));
                    }
                    next = curr - decrement;
                }
            } while (!PlatformDependent.compareAndSwapInt(this, OFFSET, curr, next));
            return (next & 1) == 1;
        }

        @Override
        void setRefCnt(int refCnt) {
            int rawRefCnt = refCnt > 0? refCnt << 1 : 1;
            PlatformDependent.putOrderedInt(this, OFFSET, rawRefCnt);
        }

        @Override
        void resetRefCnt() {
            PlatformDependent.putOrderedInt(this, OFFSET, 2);
        }

        @Override
        public boolean isLiveNonVolatile() {
            final int rawCnt = PlatformDependent.getInt(this, OFFSET);
            if (rawCnt == 2) {
                return true;
            }
            return (rawCnt & 1) == 0;
        }
    }
}
