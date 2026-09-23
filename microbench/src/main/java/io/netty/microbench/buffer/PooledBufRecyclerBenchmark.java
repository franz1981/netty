/*
 * EXPERIMENT ONLY (do not merge): what the Recycler's platform-thread behaviour costs a
 * PooledByteBufAllocator user, which is the mainstream case (PooledByteBuf instances come
 * from a Recycler).
 */
package io.netty.microbench.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.microbench.util.AbstractMicrobenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class PooledBufRecyclerBenchmark extends AbstractMicrobenchmark {

    private final PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

    @Benchmark
    public boolean directBuffer1024() {
        ByteBuf buf = allocator.directBuffer(1024);
        return buf.release();
    }

    @Benchmark
    public boolean heapBuffer1024() {
        ByteBuf buf = allocator.heapBuffer(1024);
        return buf.release();
    }
}
