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
package io.netty.incubator.allocator;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;

import static java.lang.invoke.MethodType.methodType;

/**
 * {@code mmap}, {@code munmap} and {@code madvise}, linked through the Foreign Function &amp; Memory API: on Java 23+
 * (for {@code SymbolLookup.findOrThrow}), on 64-bit Linux x86_64 or aarch64 (the {@code PROT_*}, {@code MAP_*} and
 * {@code MADV_*} values below are those of both), with native access enabled for this module, never in a native
 * image.
 * <p>
 * The FFM API is compiled against the {@code netty-ffm-stub} module. It and the signature-polymorphic
 * {@link MethodHandle} calls are only used from {@link Linked}, which is only loaded on Java versions that have the
 * FFM API, so this class still loads and verifies where it is missing. Why the class is, or is not, usable is logged
 * once at debug level.
 * <p>
 * Its static methods ({@code isAvailable}, {@code unavailabilityCause}, {@code mmap}, {@code munmap},
 * {@code madviseDontNeed} and {@code wrap}) take and return primitives, a {@link Throwable} or a {@link ByteBuffer}
 * only, so that a native-image integration can substitute them, as is done for the FFM cleaner of netty-common, and
 * must stay that way.
 */
final class LinuxMmapLinker {
    private static final InternalLogger logger;
    private static final boolean SUPPORTED;
    private static final Throwable UNAVAILABILITY_CAUSE;

    static {
        Throwable error;
        if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
            // A native image can only link the downcalls registered when the image was built: never here.
            logger = null;
            error = new UnsupportedOperationException("mmap(2) is not linked in a native image");
        } else {
            logger = InternalLoggerFactory.getInstance(LinuxMmapLinker.class);
            String os = PlatformDependent.normalizedOs();
            String arch = PlatformDependent.normalizedArch();
            if (PlatformDependent.javaVersion() < 23) {
                error = new UnsupportedOperationException("java.lang.foreign.SymbolLookup.findOrThrow needs Java 23+,"
                        + " this is Java " + PlatformDependent.javaVersion());
            } else if (!"linux".equals(os) || !("x86_64".equals(arch) || "aarch_64".equals(arch))) {
                error = new UnsupportedOperationException("Linux x86_64 or aarch64 only, not " + os + ' ' + arch);
            } else {
                try {
                    error = Linked.UNAVAILABILITY_CAUSE;
                } catch (Throwable t) {
                    error = t;
                }
            }
        }
        SUPPORTED = error == null;
        UNAVAILABILITY_CAUSE = error;
        if (logger != null) {
            if (error == null) {
                logger.debug("mmap(2) regions: available");
            } else {
                logger.debug("mmap(2) regions: unavailable, regions are one block each from malloc: {}",
                        error.toString(), error);
            }
        }
    }

    private LinuxMmapLinker() {
    }

    static boolean isAvailable() {
        return SUPPORTED;
    }

    /** Why {@link #isAvailable()} is false; {@code null} when it is true. */
    static Throwable unavailabilityCause() {
        return UNAVAILABILITY_CAUSE;
    }

    static long mmap(long length) {
        long address = Linked.mmap(length);
        if (address == Linked.MAP_FAILED) {
            throw new NativeCallException("mmap(2) failed to map " + length + " bytes");
        }
        return address;
    }

    static void munmap(long address, long length) {
        if (Linked.munmap(address, length) != 0) {
            throw new NativeCallException("munmap(2) failed for " + length + " bytes at 0x"
                    + Long.toHexString(address));
        }
    }

    static void madviseDontNeed(long address, long length) {
        if (Linked.madvise(address, length) != 0) {
            throw new NativeCallException("madvise(MADV_DONTNEED) failed for " + length + " bytes at 0x"
                    + Long.toHexString(address));
        }
    }

    /** {@code size} bytes at {@code address}, already mapped, as a direct {@link ByteBuffer}. */
    static ByteBuffer wrap(long address, int size) {
        return Linked.wrap(address, size);
    }

    /** Checks the module and the platform, and links the three calls; loaded only where the FFM API exists. */
    private static final class Linked {
        static final int PROT_READ = 0x1;
        static final int PROT_WRITE = 0x2;
        static final int MAP_PRIVATE = 0x02;
        static final int MAP_ANONYMOUS = 0x20;
        /** No swap reserved up front: the memory is committed page by page, as it is touched. */
        static final int MAP_NORESERVE = 0x4000;
        static final int MADV_DONTNEED = 4;
        static final long MAP_FAILED = -1L;

        static final MethodHandle MMAP;
        static final MethodHandle MUNMAP;
        static final MethodHandle MADVISE;
        static final Throwable UNAVAILABILITY_CAUSE;

        static {
            MethodHandle mmap = null;
            MethodHandle munmap = null;
            MethodHandle madvise = null;
            Throwable error = null;
            try {
                // Class.getModule() is not part of the Java 8 API we compile against, so we look it up.
                MethodHandle getModule = MethodHandles.lookup().findVirtual(
                        Class.class, "getModule", methodType(Module.class));
                Module module = (Module) getModule.invokeExact(LinuxMmapLinker.class);
                if (!module.isNativeAccessEnabled()) {
                    throw new UnsupportedOperationException("Native access (restricted methods) is not enabled for "
                            + "the io.netty.incubator.allocator module: pass --enable-native-access=ALL-UNNAMED on the "
                            + "class path, --enable-native-access=io.netty.incubator.allocator on the module path.");
                }
                if (ValueLayout.ADDRESS.byteSize() != Long.BYTES) {
                    throw new UnsupportedOperationException("64-bit platforms only");
                }
                Linker linker = Linker.nativeLinker();
                SymbolLookup symbols = linker.defaultLookup();
                mmap = linker.downcallHandle(symbols.findOrThrow("mmap"), FunctionDescriptor.of(
                        ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
                munmap = linker.downcallHandle(symbols.findOrThrow("munmap"), FunctionDescriptor.of(
                        ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
                madvise = linker.downcallHandle(symbols.findOrThrow("madvise"), FunctionDescriptor.of(
                        ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
            } catch (Throwable t) {
                mmap = null;
                munmap = null;
                madvise = null;
                error = t;
            }
            MMAP = mmap;
            MUNMAP = munmap;
            MADVISE = madvise;
            UNAVAILABILITY_CAUSE = error;
        }

        static ByteBuffer wrap(long address, int size) {
            return MemorySegment.ofAddress(address).reinterpret(size).asByteBuffer();
        }

        static long mmap(long length) {
            try {
                return (long) MMAP.invokeExact(0L, length, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0L);
            } catch (Throwable t) {
                throw new Error(t); // Should not happen.
            }
        }

        static int munmap(long address, long length) {
            try {
                return (int) MUNMAP.invokeExact(address, length);
            } catch (Throwable t) {
                throw new Error(t); // Should not happen.
            }
        }

        static int madvise(long address, long length) {
            try {
                return (int) MADVISE.invokeExact(address, length, MADV_DONTNEED);
            } catch (Throwable t) {
                throw new Error(t); // Should not happen.
            }
        }
    }
}
