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

    // Each syscall is bound twice: the plain downcall a successful call pays for alone, and one that also
    // captures errno in a leading capture-state segment, typed Object, used only once a call fails.
    private static final MethodHandle MMAP;
    private static final MethodHandle MUNMAP;
    private static final MethodHandle MADVISE;
    private static final MethodHandle MMAP_CAPTURING;
    private static final MethodHandle MUNMAP_CAPTURING;
    private static final MethodHandle MADVISE_CAPTURING;
    private static final MethodHandle OPEN_ARENA;
    private static final MethodHandle NEW_CALL_STATE;
    private static final MethodHandle CLOSE_ARENA;
    private static final MethodHandle ERRNO;

    static {
        MethodHandle mmap = null;
        MethodHandle munmap = null;
        MethodHandle madvise = null;
        MethodHandle mmapCapturing = null;
        MethodHandle munmapCapturing = null;
        MethodHandle madviseCapturing = null;
        MethodHandle openArena = null;
        MethodHandle newCallState = null;
        MethodHandle closeArena = null;
        MethodHandle errno = null;
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

            Class<?> memoryLayoutCls = Class.forName("java.lang.foreign.MemoryLayout");
            Class<?> valueLayoutCls = Class.forName("java.lang.foreign.ValueLayout");
            Class<?> ofIntCls = Class.forName("java.lang.foreign.ValueLayout$OfInt");
            Class<?> linkerCls = Class.forName("java.lang.foreign.Linker");
            Class<?> linkerOptionCls = Class.forName("java.lang.foreign.Linker$Option");
            Class<?> symbolLookupCls = Class.forName("java.lang.foreign.SymbolLookup");
            Class<?> memSegCls = Class.forName("java.lang.foreign.MemorySegment");
            Class<?> funcDescCls = Class.forName("java.lang.foreign.FunctionDescriptor");
            Class<?> arenaCls = Class.forName("java.lang.foreign.Arena");
            Class<?> pathElementCls = Class.forName("java.lang.foreign.MemoryLayout$PathElement");

            Class<?> addressLayoutCls = Class.forName("java.lang.foreign.AddressLayout");
            Object addressLayout = lookup.findStaticGetter(valueLayoutCls, "ADDRESS", addressLayoutCls).invoke();
            long addressSize = (Long) lookup.findVirtual(addressLayoutCls, "byteSize", methodType(long.class))
                    .invoke(addressLayout);
            if (addressSize != Long.BYTES) {
                throw new UnsupportedOperationException("64-bit addresses only, not " + addressSize + " bytes");
            }
            Object linker = lookup.findStatic(linkerCls, "nativeLinker", methodType(linkerCls)).invoke();
            Object symbols = lookup.findVirtual(linkerCls, "defaultLookup", methodType(symbolLookupCls))
                    .invoke(linker);
            // SymbolLookup.find(String) is Java 22; findOrThrow is Java 23.
            MethodHandle find = lookup.findVirtual(symbolLookupCls, "find", methodType(Optional.class, String.class))
                    .bindTo(symbols);
            MethodHandle downcall = lookup.findVirtual(linkerCls, "downcallHandle", methodType(MethodHandle.class,
                    memSegCls, funcDescCls, Array.newInstance(linkerOptionCls, 0).getClass()))
                    .asFixedArity().bindTo(linker);
            MethodHandle descriptorOf = lookup.findStatic(funcDescCls, "of", methodType(funcDescCls,
                    memoryLayoutCls, Array.newInstance(memoryLayoutCls, 0).getClass())).asFixedArity();
            Object noOptions = Array.newInstance(linkerOptionCls, 0);
            Object captureErrno = Array.newInstance(linkerOptionCls, 1);
            Array.set(captureErrno, 0, lookup.findStatic(linkerOptionCls, "captureCallState",
                    methodType(linkerOptionCls, String[].class)).asFixedArity().invoke(new String[] {"errno"}));
            Object longLayout = lookup.findStaticGetter(valueLayoutCls, "JAVA_LONG",
                    Class.forName("java.lang.foreign.ValueLayout$OfLong")).invoke();
            Object intLayout = lookup.findStaticGetter(valueLayoutCls, "JAVA_INT", ofIntCls).invoke();

            mmap = link(memoryLayoutCls, descriptorOf, find, downcall, noOptions,
                    "mmap", longLayout, longLayout, longLayout, intLayout, intLayout, intLayout, longLayout);
            mmapCapturing = link(memoryLayoutCls, descriptorOf, find, downcall, captureErrno,
                    "mmap", longLayout, longLayout, longLayout, intLayout, intLayout, intLayout, longLayout);
            munmap = link(memoryLayoutCls, descriptorOf, find, downcall, noOptions,
                    "munmap", intLayout, longLayout, longLayout);
            munmapCapturing = link(memoryLayoutCls, descriptorOf, find, downcall, captureErrno,
                    "munmap", intLayout, longLayout, longLayout);
            madvise = link(memoryLayoutCls, descriptorOf, find, downcall, noOptions,
                    "madvise", intLayout, longLayout, longLayout, intLayout);
            madviseCapturing = link(memoryLayoutCls, descriptorOf, find, downcall, captureErrno,
                    "madvise", intLayout, longLayout, longLayout, intLayout);

            // The errno rule: a capture state is a MemorySegment, allocated per call in a confined Arena; its
            // "errno" field sits at a fixed offset in Linker.Option.captureStateLayout().
            Object stateLayout = lookup.findStatic(linkerOptionCls, "captureStateLayout",
                    methodType(Class.forName("java.lang.foreign.StructLayout"))).invoke();
            Object errnoPath = Array.newInstance(pathElementCls, 1);
            Array.set(errnoPath, 0, lookup.findStatic(pathElementCls, "groupElement",
                    methodType(pathElementCls, String.class)).invoke("errno"));
            long errnoOffset = (Long) lookup.findVirtual(memoryLayoutCls, "byteOffset",
                    methodType(long.class, errnoPath.getClass())).asFixedArity().invoke(stateLayout, errnoPath);
            openArena = lookup.findStatic(arenaCls, "ofConfined", methodType(arenaCls))
                    .asType(methodType(Object.class));
            newCallState = MethodHandles.insertArguments(lookup.findVirtual(
                    Class.forName("java.lang.foreign.SegmentAllocator"), "allocate",
                    methodType(memSegCls, memoryLayoutCls)), 1, stateLayout)
                    .asType(methodType(Object.class, Object.class));
            closeArena = lookup.findVirtual(arenaCls, "close", methodType(void.class))
                    .asType(methodType(void.class, Object.class));
            errno = MethodHandles.insertArguments(lookup.findVirtual(memSegCls, "get",
                    methodType(int.class, ofIntCls, long.class)), 1, intLayout, errnoOffset)
                    .asType(methodType(int.class, Object.class));
        } catch (Throwable t) {
            error = t;
        }
        MMAP = mmap;
        MUNMAP = munmap;
        MADVISE = madvise;
        MMAP_CAPTURING = mmapCapturing;
        MUNMAP_CAPTURING = munmapCapturing;
        MADVISE_CAPTURING = madviseCapturing;
        OPEN_ARENA = openArena;
        NEW_CALL_STATE = newCallState;
        CLOSE_ARENA = closeArena;
        ERRNO = errno;
        if (error == null) {
            logger.debug("mmap(2): available");
        } else {
            logger.debug("mmap(2): unavailable", error);
        }
    }

    private LinuxMmapLinker() {
    }

    /** The downcall of {@code name}; with a capture option, its leading capture-state segment typed {@code Object}. */
    private static MethodHandle link(Class<?> memoryLayoutCls, MethodHandle descriptorOf, MethodHandle find,
            MethodHandle downcall, Object options, String name, Object returnLayout, Object... argumentLayouts)
            throws Throwable {
        Object arguments = Array.newInstance(memoryLayoutCls, argumentLayouts.length);
        for (int k = 0; k < argumentLayouts.length; k++) {
            Array.set(arguments, k, argumentLayouts[k]);
        }
        Object descriptor = descriptorOf.invoke(returnLayout, arguments);
        Optional<?> symbol = (Optional<?>) find.invoke(name);
        if (!symbol.isPresent()) {
            throw new UnsupportedOperationException(name + " is not in the default lookup");
        }
        MethodHandle handle = (MethodHandle) downcall.invoke(symbol.get(), descriptor, options);
        return Array.getLength(options) == 0 ? handle
                : handle.asType(handle.type().changeParameterType(0, Object.class));
    }

    static boolean isAvailable() {
        return MMAP != null;
    }

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

    static void madviseDontNeed(long address, long length) {
        int result;
        try {
            result = (int) MADVISE.invokeExact(address, length, MADV_DONTNEED);
        } catch (Throwable t) {
            throw new Error(t);
        }
        if (result != 0) {
            madviseCapturing(address, length);
        }
    }

    // A failed mmap/munmap changed no mapping and madvise(DONTNEED) is idempotent, so the retry is safe.
    private static long mmapCapturing(long length) {
        Object arena = openArena();
        try {
            Object state = newCallState(arena);
            long address;
            try {
                address = (long) MMAP_CAPTURING.invokeExact(state, 0L, length, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0L);
            } catch (Throwable t) {
                throw new Error(t);
            }
            if (address == MAP_FAILED) {
                throw fail("mmap(2) failed to map " + length + " bytes", state);
            }
            return address;
        } finally {
            closeArena(arena);
        }
    }

    private static void munmapCapturing(long address, long length) {
        Object arena = openArena();
        try {
            Object state = newCallState(arena);
            int result;
            try {
                result = (int) MUNMAP_CAPTURING.invokeExact(state, address, length);
            } catch (Throwable t) {
                throw new Error(t);
            }
            if (result != 0) {
                throw fail("munmap(2) failed for " + length + " bytes at 0x" + Long.toHexString(address), state);
            }
        } finally {
            closeArena(arena);
        }
    }

    private static void madviseCapturing(long address, long length) {
        Object arena = openArena();
        try {
            Object state = newCallState(arena);
            int result;
            try {
                result = (int) MADVISE_CAPTURING.invokeExact(state, address, length, MADV_DONTNEED);
            } catch (Throwable t) {
                throw new Error(t);
            }
            if (result != 0) {
                throw fail("madvise(MADV_DONTNEED) failed for " + length + " bytes at 0x"
                        + Long.toHexString(address), state);
            }
        } finally {
            closeArena(arena);
        }
    }

    /** Built only once a call has failed: {@code what} plus the errno the capture state holds for it. */
    private static NativeCallException fail(String what, Object state) {
        int errno = errno(state);
        return new NativeCallException(what + ": errno " + errno, errno);
    }

    private static Object openArena() {
        try {
            return (Object) OPEN_ARENA.invokeExact();
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static Object newCallState(Object arena) {
        try {
            return (Object) NEW_CALL_STATE.invokeExact(arena);
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static void closeArena(Object arena) {
        try {
            CLOSE_ARENA.invokeExact(arena);
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static int errno(Object state) {
        try {
            return (int) ERRNO.invokeExact(state);
        } catch (Throwable t) {
            throw new Error(t);
        }
    }
}
