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
package io.netty.util.internal;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.Optional;

import static java.lang.invoke.MethodType.methodType;

/**
 * {@code mmap}, {@code munmap} and {@code madvise}, bound through {@code java.lang.foreign.Linker}, found by
 * reflection so that nothing here references {@code java.lang.foreign} at compile time: available on Java 22+,
 * 64-bit Linux x86_64 or aarch64 (the {@code PROT_*}, {@code MAP_*} and {@code MADV_*} values below are those of
 * both), with native access enabled for this class's module, never in a native image.
 */
final class LinuxMmapLinker {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(LinuxMmapLinker.class);

    private static final int PROT_READ = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_PRIVATE = 0x02;
    private static final int MAP_ANONYMOUS = 0x20;
    /** No swap reserved up front: the memory is committed page by page, as it is touched. */
    private static final int MAP_NORESERVE = 0x4000;
    private static final int MADV_DONTNEED = 4;
    private static final long MAP_FAILED = -1L;

    // Plain downcalls: nothing but the call on the way. See Capturing for errno after a failure.
    private static final MethodHandle MMAP;
    private static final MethodHandle MUNMAP;
    private static final MethodHandle MADVISE;
    /** What {@link Capturing} links with; {@code null} when unavailable. */
    private static final Linking LINKING;

    static {
        MethodHandle mmap = null;
        MethodHandle munmap = null;
        MethodHandle madvise = null;
        Linking linking = null;
        Throwable error = null;
        try {
            if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
                throw new UnsupportedOperationException("not in native images");
            }
            if (PlatformDependent.javaVersion() < 22) {
                throw new UnsupportedOperationException("java.lang.foreign.Linker needs Java 22+");
            }
            String os = PlatformDependent.normalizedOs();
            String arch = PlatformDependent.normalizedArch();
            if (!"linux".equals(os) || !("x86_64".equals(arch) || "aarch_64".equals(arch))) {
                throw new UnsupportedOperationException("Linux x86_64 or aarch64 only, not " + os + ' ' + arch);
            }
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class<?> moduleCls = Class.forName("java.lang.Module");
            Object module = lookup.findVirtual(Class.class, "getModule", methodType(moduleCls))
                    .invoke(LinuxMmapLinker.class);
            if (!(Boolean) lookup.findVirtual(moduleCls, "isNativeAccessEnabled", methodType(boolean.class))
                    .invoke(module)) {
                throw new UnsupportedOperationException("native access is not enabled for " + module);
            }
            linking = new Linking(lookup);
            mmap = linking.mmap(linking.noOptions);
            munmap = linking.munmap(linking.noOptions);
            madvise = linking.madvise(linking.noOptions);
        } catch (Throwable t) {
            mmap = null;
            munmap = null;
            madvise = null;
            linking = null;
            error = t;
        }
        MMAP = mmap;
        MUNMAP = munmap;
        MADVISE = madvise;
        LINKING = linking;
        if (error == null) {
            logger.debug("mmap(2): available");
        } else {
            logger.debug("mmap(2): unavailable", error);
        }
    }

    private LinuxMmapLinker() {
    }

    /** The reflective pieces of {@code java.lang.foreign} the downcalls are linked with. */
    private static final class Linking {
        final MethodHandles.Lookup lookup;
        final Class<?> memoryLayoutCls;
        final Class<?> linkerOptionCls;
        final Class<?> memSegCls;
        final Class<?> ofIntCls;
        final Object linker;
        final Object symbols;
        final MethodHandle find;
        final MethodHandle downcallHandle;
        final MethodHandle descriptorOf;
        /** {@code Linker.Option[0]}. */
        final Object noOptions;
        /** Pointers, size_t and off_t are passed as Java longs. */
        final Object j;
        final Object i;

        Linking(MethodHandles.Lookup lookup) throws Throwable {
            this.lookup = lookup;
            memoryLayoutCls = Class.forName("java.lang.foreign.MemoryLayout");
            Class<?> valueLayoutCls = Class.forName("java.lang.foreign.ValueLayout");
            Class<?> addressLayoutCls = Class.forName("java.lang.foreign.AddressLayout");
            Class<?> linkerCls = Class.forName("java.lang.foreign.Linker");
            linkerOptionCls = Class.forName("java.lang.foreign.Linker$Option");
            Class<?> symbolLookupCls = Class.forName("java.lang.foreign.SymbolLookup");
            memSegCls = Class.forName("java.lang.foreign.MemorySegment");
            Class<?> funcDescCls = Class.forName("java.lang.foreign.FunctionDescriptor");
            ofIntCls = Class.forName("java.lang.foreign.ValueLayout$OfInt");

            Object addressLayout = lookup.findStaticGetter(valueLayoutCls, "ADDRESS", addressLayoutCls).invoke();
            long addressSize = (Long) lookup.findVirtual(addressLayoutCls, "byteSize", methodType(long.class))
                    .invoke(addressLayout);
            if (addressSize != Long.BYTES) {
                throw new UnsupportedOperationException("64-bit addresses only, not " + addressSize + " bytes");
            }
            linker = lookup.findStatic(linkerCls, "nativeLinker", methodType(linkerCls)).invoke();
            symbols = lookup.findVirtual(linkerCls, "defaultLookup", methodType(symbolLookupCls)).invoke(linker);
            // SymbolLookup.find(String) is Java 22; findOrThrow is Java 23.
            find = lookup.findVirtual(symbolLookupCls, "find", methodType(Optional.class, String.class));
            downcallHandle = lookup.findVirtual(linkerCls, "downcallHandle", methodType(
                    MethodHandle.class, memSegCls, funcDescCls, Array.newInstance(linkerOptionCls, 0).getClass()))
                    .asFixedArity();
            descriptorOf = lookup.findStatic(funcDescCls, "of", methodType(funcDescCls,
                    memoryLayoutCls, Array.newInstance(memoryLayoutCls, 0).getClass())).asFixedArity();
            noOptions = Array.newInstance(linkerOptionCls, 0);
            j = lookup.findStaticGetter(valueLayoutCls, "JAVA_LONG",
                    Class.forName("java.lang.foreign.ValueLayout$OfLong")).invoke();
            i = lookup.findStaticGetter(valueLayoutCls, "JAVA_INT", ofIntCls).invoke();
        }

        /** {@code void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)}. */
        MethodHandle mmap(Object options) throws Throwable {
            return link(options, "mmap", j, j, j, i, i, i, j);
        }

        /** {@code int munmap(void *addr, size_t length)}. */
        MethodHandle munmap(Object options) throws Throwable {
            return link(options, "munmap", i, j, j);
        }

        /** {@code int madvise(void *addr, size_t length, int advice)}. */
        MethodHandle madvise(Object options) throws Throwable {
            return link(options, "madvise", i, j, j, i);
        }

        /** The downcall of {@code name}; with a capture option, its leading capture-state segment typed Object. */
        private MethodHandle link(Object options, String name, Object returnLayout, Object... argumentLayouts)
                throws Throwable {
            Object arguments = Array.newInstance(memoryLayoutCls, argumentLayouts.length);
            for (int k = 0; k < argumentLayouts.length; k++) {
                Array.set(arguments, k, argumentLayouts[k]);
            }
            Object descriptor = descriptorOf.invoke(returnLayout, arguments);
            Optional<?> symbol = (Optional<?>) find.invoke(symbols, name);
            if (!symbol.isPresent()) {
                throw new UnsupportedOperationException(name + " is not in the default lookup");
            }
            MethodHandle handle = (MethodHandle) downcallHandle.invoke(linker, symbol.get(), descriptor, options);
            return Array.getLength(options) == 0 ? handle
                    : handle.asType(handle.type().changeParameterType(0, Object.class));
        }
    }

    /**
     * The same downcalls, capturing errno in a leading capture-state segment, and what that segment needs: linked on
     * the first failed call only, so that a process whose calls succeed never links them. A failed call is re-issued
     * once through them: a failed {@code mmap} or {@code munmap} changed no mapping, and {@code madvise} is
     * idempotent, so the retry is safe; if it succeeds the call did, else its errno is the one reported.
     */
    private static final class Capturing {
        static final MethodHandle MMAP;
        static final MethodHandle MUNMAP;
        static final MethodHandle MADVISE;
        /** {@code () -> Arena}, confined: holds one call's capture state. */
        static final MethodHandle OPEN_ARENA;
        /** {@code (Arena) -> MemorySegment}: a capture state. */
        static final MethodHandle NEW_CALL_STATE;
        /** {@code (Arena) -> void}. */
        static final MethodHandle CLOSE_ARENA;
        /** {@code (MemorySegment) -> int}: the errno of a capture state. */
        static final MethodHandle ERRNO;

        static {
            Linking l = LINKING;
            try {
                MethodHandles.Lookup lookup = l.lookup;
                Class<?> arenaCls = Class.forName("java.lang.foreign.Arena");
                Class<?> segmentAllocatorCls = Class.forName("java.lang.foreign.SegmentAllocator");
                Class<?> pathElementCls = Class.forName("java.lang.foreign.MemoryLayout$PathElement");
                Object captureErrno = Array.newInstance(l.linkerOptionCls, 1);
                Array.set(captureErrno, 0, lookup.findStatic(l.linkerOptionCls, "captureCallState",
                        methodType(l.linkerOptionCls, String[].class)).asFixedArity()
                        .invoke(new String[] {"errno"}));
                MMAP = l.mmap(captureErrno);
                MUNMAP = l.munmap(captureErrno);
                MADVISE = l.madvise(captureErrno);
                // Arena.ofConfined(), arena.allocate(Linker.Option.captureStateLayout()), arena.close(), and
                // state.get(JAVA_INT, <offset of errno>).
                Object stateLayout = lookup.findStatic(l.linkerOptionCls, "captureStateLayout",
                        methodType(Class.forName("java.lang.foreign.StructLayout"))).invoke();
                Object errnoPath = Array.newInstance(pathElementCls, 1);
                Array.set(errnoPath, 0, lookup.findStatic(pathElementCls, "groupElement",
                        methodType(pathElementCls, String.class)).invoke("errno"));
                long errnoOffset = (Long) lookup.findVirtual(l.memoryLayoutCls, "byteOffset",
                        methodType(long.class, errnoPath.getClass())).asFixedArity().invoke(stateLayout, errnoPath);
                OPEN_ARENA = lookup.findStatic(arenaCls, "ofConfined", methodType(arenaCls))
                        .asType(methodType(Object.class));
                NEW_CALL_STATE = MethodHandles.insertArguments(lookup.findVirtual(segmentAllocatorCls, "allocate",
                        methodType(l.memSegCls, l.memoryLayoutCls)), 1, stateLayout)
                        .asType(methodType(Object.class, Object.class));
                CLOSE_ARENA = lookup.findVirtual(arenaCls, "close", methodType(void.class))
                        .asType(methodType(void.class, Object.class));
                ERRNO = MethodHandles.insertArguments(lookup.findVirtual(l.memSegCls, "get",
                        methodType(int.class, l.ofIntCls, long.class)), 1, l.i, errnoOffset)
                        .asType(methodType(int.class, Object.class));
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        private Capturing() {
        }
    }

    static boolean isAvailable() {
        return MMAP != null;
    }

    // A call that succeeds is the plain downcall alone. One that fails is re-issued once through Capturing, with a
    // capture state of its own, allocated and freed around it, for its errno: failures are rare, from any thread.

    static long mmap(long length) {
        long address;
        try {
            address = (long) MMAP.invokeExact(0L, length, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0L);
        } catch (Throwable t) {
            throw new Error(t);
        }
        return address != MAP_FAILED ? address : mmapCapturing(length);
    }

    private static long mmapCapturing(long length) {
        Object arena = openArena();
        try {
            Object state = newCallState(arena);
            long address;
            try {
                address = (long) Capturing.MMAP.invokeExact(state, 0L, length, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0L);
            } catch (Throwable t) {
                throw new Error(t);
            }
            if (address == MAP_FAILED) {
                int errno = errno(state);
                throw new NativeCallException("mmap(2) failed to map " + length + " bytes: errno " + errno, errno);
            }
            return address;
        } finally {
            closeArena(arena);
        }
    }

    static void munmap(long address, long length) {
        int result;
        try {
            result = (int) MUNMAP.invokeExact(address, length);
        } catch (Throwable t) {
            throw new Error(t);
        }
        if (result != 0) {
            munmapCapturing(address, length);
        }
    }

    private static void munmapCapturing(long address, long length) {
        Object arena = openArena();
        try {
            Object state = newCallState(arena);
            int result;
            try {
                result = (int) Capturing.MUNMAP.invokeExact(state, address, length);
            } catch (Throwable t) {
                throw new Error(t);
            }
            if (result != 0) {
                int errno = errno(state);
                throw new NativeCallException("munmap(2) failed for " + length + " bytes at 0x"
                        + Long.toHexString(address) + ": errno " + errno, errno);
            }
        } finally {
            closeArena(arena);
        }
    }

    static void madviseDontNeed(long address, long length) {
        int result;
        try {
            result = (int) MADVISE.invokeExact(address, length, MADV_DONTNEED);
        } catch (Throwable t) {
            throw new Error(t);
        }
        if (result != 0) {
            madviseDontNeedCapturing(address, length);
        }
    }

    private static void madviseDontNeedCapturing(long address, long length) {
        Object arena = openArena();
        try {
            Object state = newCallState(arena);
            int result;
            try {
                result = (int) Capturing.MADVISE.invokeExact(state, address, length, MADV_DONTNEED);
            } catch (Throwable t) {
                throw new Error(t);
            }
            if (result != 0) {
                int errno = errno(state);
                throw new NativeCallException("madvise(MADV_DONTNEED) failed for " + length + " bytes at 0x"
                        + Long.toHexString(address) + ": errno " + errno, errno);
            }
        } finally {
            closeArena(arena);
        }
    }

    private static Object openArena() {
        try {
            return (Object) Capturing.OPEN_ARENA.invokeExact();
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static Object newCallState(Object arena) {
        try {
            return (Object) Capturing.NEW_CALL_STATE.invokeExact(arena);
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static void closeArena(Object arena) {
        try {
            Capturing.CLOSE_ARENA.invokeExact(arena);
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static int errno(Object state) {
        try {
            return (int) Capturing.ERRNO.invokeExact(state);
        } catch (Throwable t) {
            throw new Error(t);
        }
    }
}
