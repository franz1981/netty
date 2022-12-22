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
public class MarkAndAppendAsciiBenchmark extends AbstractMicrobenchmark implements ByteProcessor {
    @Param({"7", "31", "513", "4097"})
    int length;
    @Param({"true"})
    boolean direct;
    private AppendableCharSequence seq;
    private int srcIndex;
    private ByteBuf src;

    @Setup
    public void init() {
        this.seq = new AppendableCharSequence(length);
        this.srcIndex = 0;
        final int cap = length + 1;
        this.src = direct ? Unpooled.directBuffer(cap, cap) : Unpooled.buffer(cap, cap);
        this.src.ensureWritable(cap);
        // this is not important really
        for (int i = 0; i < length; i++) {
            src.setByte(i, 'a');
        }
        src.setByte(length, '\0');
        this.src.writerIndex(cap);
    }

    @Override
    public boolean process(byte value) {
        final char c = (char) (value & 0xFF);
        if (c == '\0') {
            return false;
        }
        seq.append(c);
        return true;
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public AppendableCharSequence fusion() {
        seq.reset();
        src.forEachByte(srcIndex, length + 1, this);
        return seq;
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public AppendableCharSequence fission() {
        final int srcIndex = this.srcIndex;
        final ByteBuf src = this.src;
        final AppendableCharSequence seq = this.seq;
        final int len = src.indexOf(srcIndex, srcIndex + (length + 1), (byte) '\0');
        setUnsafeBatchAscii(seq, src, srcIndex, len);
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
                srcIndex += 8;
            }
        }
        seq.setLengthUnsafe(len);
    }

}
