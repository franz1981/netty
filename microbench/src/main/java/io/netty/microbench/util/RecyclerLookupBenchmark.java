/*
 * EXPERIMENT ONLY (do not merge): attribute the per-op cost of the Recycler's thread-local lookup.
 */
package io.netty.microbench.util;

import io.netty.util.Recycler;
import io.netty.util.Recycler.EnhancedHandle;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
public class RecyclerLookupBenchmark extends AbstractMicrobenchmark {

    @State(Scope.Thread)
    public static class PoolState {
        Recycler<DummyObject> recycler;
        Object pool;

        @Setup
        public void init() {
            recycler = new Recycler<DummyObject>(false) {
                @Override
                protected DummyObject newObject(Recycler.Handle<DummyObject> handle) {
                    return new DummyObject((EnhancedHandle<DummyObject>) handle);
                }
            };
            // warm the pool up on this very thread, then hoist its LocalPool out of the loop
            DummyObject o = recycler.get();
            o.recycle();
            pool = recycler.currentThreadPool();
        }
    }

    /** baseline: the full path, thread-local lookup included. */
    @Benchmark
    public DummyObject lookupGetAndRecycle(PoolState state) {
        DummyObject o = state.recycler.get();
        o.recycle();
        return o;
    }

    /** (i) same pooling work, the thread-local lookup hoisted into @Setup. */
    @Benchmark
    public DummyObject hoistedGetAndRecycle(PoolState state) {
        DummyObject o = state.recycler.getFromPool(state.pool);
        o.recycle();
        return o;
    }

    @SuppressWarnings("unused")
    public static final class DummyObject {
        private final EnhancedHandle<DummyObject> handle;
        private long l1, l2, l3, l4, l5;
        private Object o1, o2, o3, o4, o5;

        DummyObject(EnhancedHandle<DummyObject> handle) {
            this.handle = handle;
        }

        public void recycle() {
            handle.recycle(this);
        }
    }
}
