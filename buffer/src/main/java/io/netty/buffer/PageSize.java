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
package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static java.lang.invoke.MethodType.methodType;

/**
 * The size of a page of the OS in bytes: libc {@code getpagesize()} through the JDK 22+ foreign linker, bound
 * reflectively so that no class here has a compile-time dependency on {@code java.lang.foreign}; then, on Linux, the
 * {@code AT_PAGESZ} entry of {@code /proc/self/auxv}; else 4096, the smallest page size of the supported platforms.
 */
final class PageSize {
    /** The {@code auxv} entry type of the page size (Linux {@code elf.h}: {@code AT_PAGESZ}). */
    private static final long AT_PAGESZ = 6;
    /** The {@code auxv} entry type that ends the vector (Linux {@code elf.h}: {@code AT_NULL}). */
    private static final long AT_NULL = 0;

    static final int PAGE_SIZE = get();

    private PageSize() {
    }

    private static int get() {
        int fromLinker = fromNativeLinker();
        if (fromLinker > 0) {
            return fromLinker;
        }
        int fromAuxv = fromAuxv();
        if (fromAuxv > 0) {
            return fromAuxv;
        }
        return 4096;
    }

    /** {@code getpagesize()}, or -1 when it is unavailable: below Java 22, or without native access. */
    private static int fromNativeLinker() {
        if (PlatformDependent.javaVersion() < 22) {
            return -1;
        }
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class<?> moduleCls = Class.forName("java.lang.Module");
            MethodHandle getModule = lookup.findVirtual(Class.class, "getModule", methodType(moduleCls));
            MethodHandle isNativeAccessEnabled = lookup.findVirtual(
                    moduleCls, "isNativeAccessEnabled", methodType(boolean.class));
            Object module = getModule.invoke(PageSize.class);
            if (!(Boolean) isNativeAccessEnabled.invoke(module)) {
                return -1;
            }

            Class<?> linkerCls = Class.forName("java.lang.foreign.Linker");
            Class<?> linkerOptionCls = Class.forName("java.lang.foreign.Linker$Option");
            Class<?> linkerOptionArrayCls = Class.forName("[Ljava.lang.foreign.Linker$Option;");
            Class<?> symbolLookupCls = Class.forName("java.lang.foreign.SymbolLookup");
            Class<?> memSegCls = Class.forName("java.lang.foreign.MemorySegment");
            Class<?> funcDescCls = Class.forName("java.lang.foreign.FunctionDescriptor");
            Class<?> memoryLayoutCls = Class.forName("java.lang.foreign.MemoryLayout");
            Class<?> memoryLayoutArrayCls = Class.forName("[Ljava.lang.foreign.MemoryLayout;");
            Class<?> valueLayoutCls = Class.forName("java.lang.foreign.ValueLayout");
            Class<?> ofIntLayoutCls = Class.forName("java.lang.foreign.ValueLayout$OfInt");

            Object nativeLinker = lookup.findStatic(linkerCls, "nativeLinker", methodType(linkerCls)).invoke();
            Object symbolLookup = lookup.findVirtual(linkerCls, "defaultLookup", methodType(symbolLookupCls))
                    .invoke(nativeLinker);
            Optional<?> symbol = (Optional<?>) lookup.findVirtual(
                    symbolLookupCls, "find", methodType(Optional.class, String.class))
                    .invoke(symbolLookup, "getpagesize");
            if (!symbol.isPresent()) {
                return -1;
            }

            Object intLayout = lookup.findStaticGetter(valueLayoutCls, "JAVA_INT", ofIntLayoutCls).invoke();
            Object noLayouts = Array.newInstance(memoryLayoutCls, 0);
            Object descriptor = lookup.findStatic(funcDescCls, "of",
                    methodType(funcDescCls, memoryLayoutCls, memoryLayoutArrayCls)).invoke(intLayout, noLayouts);
            Object noOptions = Array.newInstance(linkerOptionCls, 0);
            MethodHandle downcall = (MethodHandle) lookup.findVirtual(linkerCls, "downcallHandle",
                    methodType(MethodHandle.class, memSegCls, funcDescCls, linkerOptionArrayCls))
                    .invoke(nativeLinker, symbol.get(), descriptor, noOptions);
            return (int) downcall.invoke();
        } catch (Throwable ignore) {
            return -1;
        }
    }

    /** {@code AT_PAGESZ} of {@code /proc/self/auxv}, or -1 when it cannot be read or found. */
    private static int fromAuxv() {
        try {
            return parseAuxv(readFully("/proc/self/auxv"));
        } catch (Throwable ignore) {
            return -1;
        }
    }

    /**
     * The value of the {@link #AT_PAGESZ} entry of a Linux {@code auxv} vector: pairs of native-endian, 64-bit
     * {@code (type, value)} longs, ending at an {@link #AT_NULL} entry. -1 if there is no {@link #AT_PAGESZ} entry
     * before the vector ends or runs out of whole entries.
     */
    static int parseAuxv(byte[] auxv) {
        ByteBuffer buffer = ByteBuffer.wrap(auxv).order(ByteOrder.nativeOrder());
        while (buffer.remaining() >= 16) {
            long type = buffer.getLong();
            long value = buffer.getLong();
            if (type == AT_NULL) {
                break;
            }
            if (type == AT_PAGESZ) {
                return (int) value;
            }
        }
        return -1;
    }

    private static byte[] readFully(String path) throws IOException {
        FileInputStream in = new FileInputStream(path);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(512);
            byte[] chunk = new byte[512];
            int read;
            while ((read = in.read(chunk)) != -1) {
                out.write(chunk, 0, read);
            }
            return out.toByteArray();
        } finally {
            in.close();
        }
    }
}
