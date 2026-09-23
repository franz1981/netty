/*
 * EXPERIMENT ONLY (do not merge): a workload where pooling should pay for itself.
 * Each pooled object owns a 512-byte payload and a live set of LIVE objects is retained,
 * so a young collection has to copy real data instead of finding everything dead.
 */
package io.netty.microbench.util;

import io.netty.util.Recycler;
import io.netty.util.Recycler.EnhancedHandle;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
public class RecyclerLiveSetBenchmark extends AbstractMicrobenchmark {

    static final int LIVE = Integer.getInteger("bench.live", 1_000_000);
    static final int PAYLOAD = 512;

    @State(Scope.Thread)
    public static class LiveSetState {
        Recycler<Payload> recycler;
        Payload[] live;
        int idx;

        @Setup
        public void init() {
            recycler = new Recycler<Payload>(false, 0) { // interval 0: pool every recycled object
                @Override
                protected Payload newObject(Recycler.Handle<Payload> handle) {
                    return new Payload((EnhancedHandle<Payload>) handle);
                }
            };
            live = new Payload[LIVE];
            for (int i = 0; i < LIVE; i++) {
                live[i] = recycler.get();
            }
        }
    }

    @Benchmark
    public Payload churnLiveSet(LiveSetState state) {
        int i = state.idx;
        if (++i == LIVE) {
            i = 0;
        }
        state.idx = i;
        Payload old = state.live[i];
        state.live[i] = null;
        old.recycle();
        Payload fresh = state.recycler.get();
        fresh.data[0] = (byte) i;
        state.live[i] = fresh;
        return fresh;
    }

    public static final class Payload {
        final EnhancedHandle<Payload> handle;
        final byte[] data = new byte[PAYLOAD];

        Payload(EnhancedHandle<Payload> handle) {
            this.handle = handle;
        }

        void recycle() {
            handle.recycle(this);
        }
    }
}
