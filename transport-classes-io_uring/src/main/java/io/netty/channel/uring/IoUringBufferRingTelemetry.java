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
package io.netty.channel.uring;

import io.netty.util.internal.SystemPropertyUtil;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Opt-in instrumentation for {@link IoUringBufferRing}, used to answer two questions about step 6/7
 * of the provided-buffer lifecycle ("retire" and "re-add") with counters instead of code reading.
 * Everything here is off by default and the flags are {@code static final}, so the JIT folds the
 * call sites away when they are not set.
 *
 * <ul>
 *   <li>{@code -Dio.netty.iouring.bufferRing.refCntTele=true} records the reference count of the
 *       ring buffer at the moment the ring drops its own reference. It answers "could the ring reuse
 *       the retiring buffer object instead of asking the allocator for a new one?": the ring can only
 *       do that when it is the LAST holder, i.e. when the count is 1 at that point.</li>
 *   <li>{@code -Dio.netty.iouring.bufferRing.noSliceHandoff=true} changes what
 *       {@link IoUringBufferRing#useBuffer(short, int, boolean)} returns when the read retires the
 *       bid: instead of a {@code retainedSlice} plus a {@code release} of the ring's own reference,
 *       the ring hands the buffer itself to the caller, transferring its reference. That removes one
 *       {@code UnpooledSlicedByteBuf} allocation and one retain/release pair per consumed buffer, at
 *       the price of handing out a buffer whose capacity is the whole ring chunk rather than the
 *       bytes read. When the bid is NOT retired (incremental consumption with more data coming) the
 *       slice is still the only correct answer and is taken as before.</li>
 * </ul>
 */
public final class IoUringBufferRingTelemetry {

    /** Record {@code refCnt()} at the point the ring releases its own reference. */
    public static final boolean REFCNT_TELE =
            SystemPropertyUtil.getBoolean("io.netty.iouring.bufferRing.refCntTele", false);

    /** Hand the retiring buffer itself to the pipeline instead of a {@code retainedSlice} of it. */
    public static final boolean NO_SLICE_HANDOFF =
            SystemPropertyUtil.getBoolean("io.netty.iouring.bufferRing.noSliceHandoff", false);

    /** True when any of the instruments above is on; every call site is guarded by it. */
    static final boolean ENABLED = REFCNT_TELE || NO_SLICE_HANDOFF;

    /** Bucket i = "refCnt was i" for i in 1..6, bucket 7 = "7 or more". Index 0 is unused. */
    private static final AtomicLongArray REFCNT = new AtomicLongArray(8);
    /** Reads that retired the bid and were handed the buffer itself. */
    private static final AtomicLong HANDOFFS = new AtomicLong();
    /** Reads that were given a slice: every read when the handoff is off, the incremental ones when on. */
    private static final AtomicLong SLICES = new AtomicLong();

    private IoUringBufferRingTelemetry() { }

    static void recordRetireRefCnt(int refCnt) {
        REFCNT.incrementAndGet(refCnt >= 7 ? 7 : Math.max(refCnt, 1));
    }

    static void countHandoff() {
        HANDOFFS.incrementAndGet();
    }

    static void countSlice() {
        SLICES.incrementAndGet();
    }

    /**
     * One line, process wide. {@code retireRefCnt=1:<n>} is the only bucket in which the ring could
     * have reused the retiring buffer object.
     */
    public static String counters() {
        StringBuilder sb = new StringBuilder("BUFRINGTELE refCntTele=").append(REFCNT_TELE)
                .append(" noSliceHandoff=").append(NO_SLICE_HANDOFF)
                .append(" handoffs=").append(HANDOFFS.get())
                .append(" slices=").append(SLICES.get())
                .append(" retireRefCnt=");
        for (int i = 1; i < 8; i++) {
            sb.append(i == 7 ? ">=7" : Integer.toString(i)).append(':').append(REFCNT.get(i));
            if (i < 7) {
                sb.append(',');
            }
        }
        return sb.toString();
    }
}
