/*
 * Copyright 2023 The Netty Project
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
package io.netty.handler.codec.http2;

import io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName;
import io.netty.microbench.util.AbstractMicrobenchmark;
import io.netty.util.AsciiString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Http2PseudoHeadersLookupBenchmark extends AbstractMicrobenchmark {

    @Param({ "true", "false" })
    public boolean hits;

    @Param({ "true", "false" })
    public boolean same;

    private AsciiString[] asciiStrings;
    private String[] strings;
    @Setup
    public void init() {
        // this benchmark is assuming a good happy path:
        // 1. ascii strings have hashCode cached
        // 2. String doesn't have AsciiString::hashCode cached -> cannot be compared directly with AsciiStrings!
        // 3. the call-sites are never observing the 2 types together
        PseudoHeaderName[] pseudoHeaderNames = PseudoHeaderName.values();
        asciiStrings = new AsciiString[pseudoHeaderNames.length];
        strings = new String[pseudoHeaderNames.length];
        if (same && !hits) {
            System.exit(0);
        }
        for (int i = 0; i < pseudoHeaderNames.length; i++) {
            PseudoHeaderName pseudoHeaderName = pseudoHeaderNames[i];
            asciiStrings[i] = same? pseudoHeaderName.value() : new AsciiString(pseudoHeaderName.value().array(), true);
            byte[] chars = asciiStrings[i].array();
            if (!hits) {
                // change the last char in `_`
                chars[chars.length - 1] = '_';
            }
            strings[i] = new String(chars, 0, 0, chars.length);
        }
    }

    @Benchmark
    public void getAsciiStringPseudoHeader(Blackhole bh) {
        for (AsciiString asciiString : asciiStrings) {
            bh.consume(PseudoHeaderName.getPseudoHeader(asciiString));
        }
    }

    @Benchmark
    public void getNewAsciiStringPseudoHeader(Blackhole bh) {
        // this try to trade the creation of a new AsciiString, reusing the internal byte[] data
        // in order to force a new hashCode computation
        for (AsciiString asciiString : asciiStrings) {
            bh.consume(PseudoHeaderName.getPseudoHeader(new AsciiString(asciiString.array(), false)));
        }
    }

    @Benchmark
    public void getStringPseudoHeader(Blackhole bh) {
        for (String string : strings) {
            bh.consume(PseudoHeaderName.getPseudoHeader(string));
        }
    }

}
