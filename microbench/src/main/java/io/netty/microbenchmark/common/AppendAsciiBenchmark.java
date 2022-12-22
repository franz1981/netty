package io.netty.microbenchmark.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.microbench.util.AbstractMicrobenchmark;
import io.netty.util.ByteProcessor;
import io.netty.util.internal.AppendableCharSequence;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
public class AppendAsciiBenchmark extends AbstractMicrobenchmark {

    @Param({"7", "31", "513", "4097"})
    int length;
    @Param({"true"})
    boolean direct;
    private ByteProcessor appendToAsciiProcessor;
    private AppendableCharSequence seq;
    private int srcIndex;
    private ByteBuf src;

    @Setup
    public void init() {
        final AppendableCharSequence seq = new AppendableCharSequence(length);
        appendToAsciiProcessor = new ByteProcessor() {
            @Override
            public boolean process(byte value) {
                seq.append((char) value);
                return true;
            }
        };
        this.seq = seq;
        this.srcIndex = 0;
        this.src = direct ? Unpooled.directBuffer(length, length) : Unpooled.buffer(length, length);
        this.src.ensureWritable(length);
        // this is not important really
        for (int i = 0; i < length; i++) {
            src.setByte(i, 'a');
        }
        this.src.writerIndex(length);
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public AppendableCharSequence appendWithExternalIteration() {
        final AppendableCharSequence seq = this.seq;
        appendWithExternalIteration(seq, src, srcIndex, length);
        return seq;
    }

    private static void appendWithExternalIteration(AppendableCharSequence seq, ByteBuf src, int srcIndex, int len) {
        seq.reset();
        seq.ensureCapacity(len);
        for (int i = 0; i < len; i++) {
            seq.append((char) src.getByte(srcIndex + i));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public AppendableCharSequence appendWithInternalIteration() {
        final AppendableCharSequence seq = this.seq;
        appendWithInternalIteration(seq, appendToAsciiProcessor, src, srcIndex, length);
        return seq;
    }

    private static void appendWithInternalIteration(AppendableCharSequence seq, ByteProcessor copyProcessor, ByteBuf src, int srcIndex, int len) {
        seq.reset();
        seq.ensureCapacity(len);
        src.forEachByte(srcIndex, len, copyProcessor);
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public AppendableCharSequence setUnsafeNonBatchAscii() {
        final AppendableCharSequence seq = this.seq;
        setUnsafeNonBatchAscii(seq, src, srcIndex, length);
        return seq;
    }

    private static void setUnsafeNonBatchAscii(AppendableCharSequence seq, ByteBuf src, int srcIndex, int len) {
        seq.reset();
        seq.ensureCapacity(len);
        for (int i = 0; i < len; i++) {
            seq.charAtUnsafe(i, (char) src.getByte(srcIndex + i));
        }
        seq.setLengthUnsafe(len);
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public AppendableCharSequence setUnsafeBatchAscii() {
        final AppendableCharSequence seq = this.seq;
        setUnsafeBatchAscii(seq, src, srcIndex, length);
        return seq;
    }

    private static void setUnsafeBatchAscii(AppendableCharSequence seq, ByteBuf src, int srcIndex, int len) {
        seq.reset();
        seq.ensureCapacity(len);
        final int remaining = len & 7;
        for (int i = 0; i < remaining; i++) {
            seq.charAtUnsafe(i, (char) src.getByte(srcIndex + i));
        }
        final int longCount = len >> 3;
        if (longCount > 0) {
            srcIndex += remaining;
            for (int i = 0; i < longCount; i++) {
                final long octet = src.getLongLE(srcIndex);
                seq.charsAtUnsafe(remaining + (i << 3),
                        (char) (octet & 0xFF),
                        (char) (octet >> 8 & 0xFF),
                        (char) (octet >> 16 & 0xFF),
                        (char) (octet >> 24 & 0xFF),
                        (char) (octet >> 32 & 0xFF),
                        (char) (octet >> 40 & 0xFF),
                        (char) (octet >> 48 & 0xFF),
                        (char) (octet >> 56 & 0xFF));
                srcIndex+=8;
            }
        }
        seq.setLengthUnsafe(len);
    }

}
