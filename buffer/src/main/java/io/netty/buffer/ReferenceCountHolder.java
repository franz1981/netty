package io.netty.buffer;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReferenceCountUpdater;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.internal.ObjectUtil.checkPositive;

abstract class ReferenceCountHolder {
    // TODO: inline
    private static final ReferenceCountUpdater.Configuration<ReferenceCountHolder> config = ReferenceCountUpdater.Configuration.of(
            ReferenceCountHolder.class,
            "refCnt",
            MethodHandles::lookup,
            AtomicIntegerFieldUpdater::newUpdater
    );
    private static final AtomicIntegerFieldUpdater<ReferenceCountHolder> UPDATER = config.updater(); // todo: fixed initializer for NI
    private static final long OFFSET = config.fieldOffset(); // todo: recompute offset for NI
    private static final ReferenceCountUpdater.Configuration.UpdaterType TYPE = config.updaterType();
    private static final int INITIAL_VALUE = 2;

    private static class VarHandleHolder {
        static final VarHandle VH = config.varHandle(); // todo: fixed initializer for NI
    }

    private volatile int refCnt;

    private void safeInitializeRawRefCnt(int value) {
        switch (TYPE) {
            case Unsafe:
                PlatformDependent.safeConstructPutInt(this, OFFSET, value);
                break;
            case VarHandle:
                VarHandleHolder.VH.set(this, value);
                break;
            case Atomic:
                UPDATER.set(this, value);
                break;
            default:
                throw new AssertionError();
        }
    }

    private int getAndAddRawRefCnt(int increment) {
        switch (TYPE) {
            case Unsafe:
                return PlatformDependent.getAndAddInt(this, OFFSET, increment);
            case VarHandle:
                return (int) VarHandleHolder.VH.getAndAdd(this, increment);
            case Atomic:
                return UPDATER.getAndAdd(this, increment);
            default:
                throw new AssertionError();
        }
    }

    private int getRawRefCnt() {
        switch (TYPE) {
            case Unsafe:
                return PlatformDependent.getInt(this, OFFSET);
            case VarHandle:
                return (int) VarHandleHolder.VH.get(this);
            case Atomic:
                return UPDATER.get(this);
            default:
                throw new AssertionError();
        }
    }

    private int getAcquireRawRefCnt() {
        switch (TYPE) {
            case Unsafe:
                return PlatformDependent.getVolatileInt(this, OFFSET);
            case VarHandle:
                return (int) VarHandleHolder.VH.getAcquire(this);
            case Atomic:
                return UPDATER.get(this);
            default:
                throw new AssertionError();
        }
    }

    private void setReleaseRawRefCnt(int value) {
        switch (TYPE) {
            case Unsafe:
                PlatformDependent.putOrderedInt(this, OFFSET, value);
                break;
            case VarHandle:
                VarHandleHolder.VH.setRelease(this, value);
                break;
            case Atomic:
                UPDATER.lazySet(this, value);
                break;
            default:
                throw new AssertionError();
        }
    }

    private boolean casRawRefCnt(int expected, int value) {
        switch (TYPE) {
            case Unsafe:
                return PlatformDependent.compareAndSwapInt(this, OFFSET, expected, value);
            case VarHandle:
                return VarHandleHolder.VH.compareAndSet(this, expected, value);
            case Atomic:
                return UPDATER.compareAndSet(this, expected, value);
            default:
                throw new AssertionError();
        }
    }

    final void refCnt_setInitialValue() {
        safeInitializeRawRefCnt(INITIAL_VALUE);
    }

    private static int realRefCnt(int rawCnt) {
        return rawCnt != 2 && rawCnt != 4 && (rawCnt & 1) != 0 ? 0 : rawCnt >>> 1;
    }

    /**
     * Like {@link #realRefCnt(int)} but throws if refCnt == 0
     */
    private static int toLiveRealRefCnt(int rawCnt, int decrement) {
        if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) {
            return rawCnt >>> 1;
        }
        // odd rawCnt => already deallocated
        throw new IllegalReferenceCountException(0, -decrement);
    }

    final int refCnt_refCnt() {
        return realRefCnt(getAcquireRawRefCnt());
    }

    final boolean refCnt_isLiveNonVolatile() {
        final int rawCnt = getRawRefCnt();
        // The "real" ref count is > 0 if the rawCnt is even.
        return rawCnt == 2 || rawCnt == 4 || rawCnt == 6 || rawCnt == 8 || (rawCnt & 1) == 0;
    }

    /**
     * An unsafe operation that sets the reference count directly
     */
    final void refCnt_setRefCnt(int refCnt) {
        int rawRefCnt = refCnt > 0 ? refCnt << 1 : 1; // overflow OK here
        setReleaseRawRefCnt(rawRefCnt);
    }

    /**
     * Resets the reference count to 1
     */
    final void refCnt_resetRefCnt() {
        // no need of a volatile set, it should happen in a quiescent state
        setReleaseRawRefCnt(INITIAL_VALUE);
    }

    final void refCnt_retain() {
        retain0(1, 2);
    }

    final void refCnt_retain(int increment) {
        // all changes to the raw count are 2x the "real" change - overflow is OK
        int rawIncrement = checkPositive(increment, "increment") << 1;
        retain0(increment, rawIncrement);
    }

    // rawIncrement == increment << 1
    private void retain0(final int increment, final int rawIncrement) {
        int oldRef = getAndAddRawRefCnt(rawIncrement);
        if (oldRef != 2 && oldRef != 4 && (oldRef & 1) != 0) {
            throw new IllegalReferenceCountException(0, increment);
        }
        // don't pass 0!
        if ((oldRef <= 0 && oldRef + rawIncrement >= 0)
                || (oldRef >= 0 && oldRef + rawIncrement < oldRef)) {
            // overflow case
            getAndAddRawRefCnt(-rawIncrement);
            throw new IllegalReferenceCountException(realRefCnt(oldRef), increment);
        }
    }

    final boolean refCnt_release() {
        int rawCnt = getRawRefCnt();
        return rawCnt == 2 ? tryFinalRelease0(2) || retryRelease0(1)
                : nonFinalRelease0(1, rawCnt, toLiveRealRefCnt(rawCnt, 1));
    }

    final boolean refCnt_release(int decrement) {
        int rawCnt = getRawRefCnt();
        int realCnt = toLiveRealRefCnt(rawCnt, checkPositive(decrement, "decrement"));
        return decrement == realCnt ? tryFinalRelease0(rawCnt) || retryRelease0(decrement)
                : nonFinalRelease0(decrement, rawCnt, realCnt);
    }

    private boolean tryFinalRelease0(int expectRawCnt) {
        return casRawRefCnt(expectRawCnt, 1); // any odd number will work
    }

    private boolean nonFinalRelease0(int decrement, int rawCnt, int realCnt) {
        if (decrement < realCnt
                // all changes to the raw count are 2x the "real" change - overflow is OK
                && casRawRefCnt(rawCnt, rawCnt - (decrement << 1))) {
            return false;
        }
        return retryRelease0(decrement);
    }

    private boolean retryRelease0(int decrement) {
        for (;;) {
            int rawCnt = getRawRefCnt(), realCnt = toLiveRealRefCnt(rawCnt, decrement);
            if (decrement == realCnt) {
                if (tryFinalRelease0(rawCnt)) {
                    return true;
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                if (casRawRefCnt(rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
            } else {
                throw new IllegalReferenceCountException(realCnt, -decrement);
            }
            Thread.yield(); // this benefits throughput under high contention
        }
    }
}
