package io.netty.buffer;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

@SuppressWarnings("unused")
abstract class PaddedSpinLockPad0 {
    long p00, p01, p02, p03, p04, p05, p06;
}

abstract class PaddedSpinLockValue extends PaddedSpinLockPad0 {
    private static final AtomicIntegerFieldUpdater<PaddedSpinLockValue> STATE =
            AtomicIntegerFieldUpdater.newUpdater(PaddedSpinLockValue.class, "state");
    volatile int state;

    boolean tryAcquire() {
        return STATE.compareAndSet(this, 0, 1);
    }

    void release() {
        STATE.lazySet(this, 0);
    }

    void acquireYielding() {
        while (!STATE.compareAndSet(this, 0, 1)) {
            Thread.yield();
        }
    }
}

@SuppressWarnings("unused")
final class PaddedSpinLock extends PaddedSpinLockValue {
    long p10, p11, p12, p13, p14, p15, p16;
}
