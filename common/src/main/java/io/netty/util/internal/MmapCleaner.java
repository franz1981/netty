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
import java.nio.ByteBuffer;
import java.util.Optional;

import static java.lang.invoke.MethodType.methodType;

/**
 * A {@link Cleaner} that maps every buffer with its own anonymous {@code mmap(2)} and gives it back with
 * {@code munmap(2)}, so the memory returns to the OS when the buffer is cleaned, whatever the C allocator's
 * thresholds are.
 * <p>
 * Not a process-wide cleaner: a mapping costs two system calls and at least a page, which is only worth it for big,
 * long-lived buffers such as the chunks of a pooling allocator. See {@link PlatformDependent#allocateDirectMmap(int)}.
 * <p>
 * Links {@code mmap}, {@code munmap}, {@code madvise} and {@code getpagesize} from libc through
 * {@code java.lang.foreign.Linker}, the same way as {@link CleanerJava24Linker}, so it is only available on Java 22+
 * (where that API is final), on 64-bit Linux x86_64 or aarch64 (the {@code PROT_*}, {@code MAP_*} and
 * {@code MADV_*} values below are those of both), and when native access is enabled for this module.
 */
final class MmapCleaner implements Cleaner {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MmapCleaner.class);

    private static final int PROT_READ = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_PRIVATE = 0x02;
    private static final int MAP_ANONYMOUS = 0x20;
    private static final int MADV_DONTNEED = 4;
    private static final long MAP_FAILED = -1L;

    private static final MethodHandle INVOKE_MMAP;
    private static final MethodHandle INVOKE_MUNMAP;
    private static final MethodHandle INVOKE_MADVISE;
    private static final MethodHandle INVOKE_CREATE_BYTEBUFFER;
    private static final long PAGE_SIZE;

    static final MmapCleaner INSTANCE;

    static {
        MethodHandle mmap = null;
        MethodHandle munmap = null;
        MethodHandle madvise = null;
        MethodHandle wrap = null;
        long pageSize = 0;
        Throwable error = null;
        try {
            if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
                throw new UnsupportedOperationException("Not supported in native images");
            }
            if (PlatformDependent0.javaVersion() < 22) {
                throw new UnsupportedOperationException("java.lang.foreign.Linker needs Java 22+");
            }
            String os = PlatformDependent.normalizedOs();
            String arch = PlatformDependent.normalizedArch();
            if (!"linux".equals(os) || !("x86_64".equals(arch) || "aarch_64".equals(arch))) {
                throw new UnsupportedOperationException("Only supported on Linux x86_64 and aarch64, not " +
                        os + ' ' + arch);
            }

            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class<?> moduleCls = Class.forName("java.lang.Module");
            Object module = lookup.findVirtual(Class.class, "getModule", methodType(moduleCls))
                    .invoke(MmapCleaner.class);
            if (!(boolean) lookup.findVirtual(moduleCls, "isNativeAccessEnabled", methodType(boolean.class))
                    .invoke(module)) {
                throw new UnsupportedOperationException(
                        "Native access (restricted methods) is not enabled for the io.netty.common module.");
            }

            Class<?> memoryLayoutCls = Class.forName("java.lang.foreign.MemoryLayout");
            Class<?> valueLayoutCls = Class.forName("java.lang.foreign.ValueLayout");
            Class<?> addressLayoutCls = Class.forName("java.lang.foreign.AddressLayout");
            Class<?> linkerCls = Class.forName("java.lang.foreign.Linker");
            Class<?> linkerOptionCls = Class.forName("java.lang.foreign.Linker$Option");
            Class<?> symbolLookupCls = Class.forName("java.lang.foreign.SymbolLookup");
            Class<?> memSegCls = Class.forName("java.lang.foreign.MemorySegment");
            Class<?> funcDescCls = Class.forName("java.lang.foreign.FunctionDescriptor");

            // Pointers and size_t/off_t are passed as Java longs, so require 64-bit addresses.
            Object addressLayout = lookup.findStaticGetter(valueLayoutCls, "ADDRESS", addressLayoutCls).invoke();
            long addressSize = (long) lookup.findVirtual(addressLayoutCls, "byteSize", methodType(long.class))
                    .invoke(addressLayout);
            if (addressSize != Long.BYTES) {
                throw new UnsupportedOperationException("Only supported on 64-bit platforms.");
            }

            Object linker = lookup.findStatic(linkerCls, "nativeLinker", methodType(linkerCls)).invoke();
            Object symbols = lookup.findVirtual(linkerCls, "defaultLookup", methodType(symbolLookupCls))
                    .invoke(linker);
            Linking linking = new Linking(
                    linker, symbols,
                    // SymbolLookup.findOrThrow(String) is Java 23+: use find(String), which is Java 22.
                    lookup.findVirtual(symbolLookupCls, "find", methodType(Optional.class, String.class)),
                    lookup.findVirtual(linkerCls, "downcallHandle", methodType(MethodHandle.class,
                            memSegCls, funcDescCls, Array.newInstance(linkerOptionCls, 0).getClass()))
                            .asFixedArity(),
                    lookup.findStatic(funcDescCls, "of", methodType(funcDescCls, memoryLayoutCls,
                            Array.newInstance(memoryLayoutCls, 0).getClass())).asFixedArity(),
                    memoryLayoutCls,
                    Array.newInstance(linkerOptionCls, 0));
            Object longLayout = lookup.findStaticGetter(valueLayoutCls, "JAVA_LONG",
                    Class.forName("java.lang.foreign.ValueLayout$OfLong")).invoke();
            Object intLayout = lookup.findStaticGetter(valueLayoutCls, "JAVA_INT",
                    Class.forName("java.lang.foreign.ValueLayout$OfInt")).invoke();

            // void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
            mmap = linking.link("mmap", longLayout, longLayout, longLayout, intLayout, intLayout, intLayout,
                    longLayout);
            // int munmap(void *addr, size_t length)
            munmap = linking.link("munmap", intLayout, longLayout, longLayout);
            // int madvise(void *addr, size_t length, int advice)
            madvise = linking.link("madvise", intLayout, longLayout, longLayout, intLayout);
            // int getpagesize(void)
            pageSize = (int) linking.link("getpagesize", intLayout).invokeExact();
            if (pageSize <= 0 || (pageSize & pageSize - 1) != 0) {
                throw new UnsupportedOperationException("Unexpected page size: " + pageSize);
            }

            // (long address, long size) -> MemorySegment.ofAddress(address).reinterpret(size).asByteBuffer(),
            // exactly as CleanerJava24Linker wraps malloc'd memory.
            MethodHandle ofAddress = lookup.findStatic(memSegCls, "ofAddress", methodType(memSegCls, long.class));
            MethodHandle reinterpret = lookup.findVirtual(memSegCls, "reinterpret",
                    methodType(memSegCls, long.class));
            MethodHandle asByteBuffer = lookup.findVirtual(memSegCls, "asByteBuffer", methodType(ByteBuffer.class));
            wrap = MethodHandles.filterReturnValue(
                    MethodHandles.filterArguments(reinterpret, 0, ofAddress), asByteBuffer);
        } catch (Throwable t) {
            mmap = null;
            munmap = null;
            madvise = null;
            wrap = null;
            pageSize = 0;
            error = t;
        }
        INVOKE_MMAP = mmap;
        INVOKE_MUNMAP = munmap;
        INVOKE_MADVISE = madvise;
        INVOKE_CREATE_BYTEBUFFER = wrap;
        PAGE_SIZE = pageSize;
        INSTANCE = error == null ? new MmapCleaner() : null;
        if (error == null) {
            logger.debug("mmap(2)/munmap(2) direct buffers: available (page size: {})", pageSize);
        } else {
            logger.debug("mmap(2)/munmap(2) direct buffers: unavailable", error);
        }
    }

    private static final class Linking {
        private final Object linker;
        private final Object symbols;
        private final MethodHandle find;
        private final MethodHandle downcallHandle;
        private final MethodHandle functionDescriptorOf;
        private final Class<?> memoryLayoutCls;
        private final Object noOptions;

        Linking(Object linker, Object symbols, MethodHandle find, MethodHandle downcallHandle,
                MethodHandle functionDescriptorOf, Class<?> memoryLayoutCls, Object noOptions) {
            this.linker = linker;
            this.symbols = symbols;
            this.find = find;
            this.downcallHandle = downcallHandle;
            this.functionDescriptorOf = functionDescriptorOf;
            this.memoryLayoutCls = memoryLayoutCls;
            this.noOptions = noOptions;
        }

        MethodHandle link(final String name, Object returnLayout, Object... argumentLayouts) throws Throwable {
            Object arguments = Array.newInstance(memoryLayoutCls, argumentLayouts.length);
            for (int i = 0; i < argumentLayouts.length; i++) {
                Array.set(arguments, i, argumentLayouts[i]);
            }
            Object descriptor = functionDescriptorOf.invoke(returnLayout, arguments);
            Optional<?> symbol = (Optional<?>) find.invoke(symbols, name);
            if (!symbol.isPresent()) {
                throw new UnsupportedOperationException(name + " not found in the default lookup");
            }
            return (MethodHandle) downcallHandle.invoke(linker, symbol.get(), descriptor, noOptions);
        }
    }

    private MmapCleaner() {
    }

    static boolean isSupported() {
        return INSTANCE != null;
    }

    static long pageSize() {
        return PAGE_SIZE;
    }

    @Override
    public CleanableDirectBuffer allocate(int capacity) {
        return new MappedDirectBuffer(capacity);
    }

    @Override
    public void freeDirectBuffer(ByteBuffer buffer) {
        throw new UnsupportedOperationException("Cannot clean arbitrary ByteBuffer instances");
    }

    @Override
    public boolean hasExpensiveClean() {
        // Two system calls per buffer: only for callers that pool what they allocate.
        return true;
    }

    private static long roundUpToPage(long size) {
        return size + PAGE_SIZE - 1 & -PAGE_SIZE;
    }

    private static long mmap(long length) {
        final long addr;
        try {
            addr = (long) INVOKE_MMAP.invokeExact(0L, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                    -1, 0L);
        } catch (Throwable e) {
            throw new Error(e); // Should not happen.
        }
        if (addr == MAP_FAILED) {
            throw new OutOfMemoryError("mmap(2) failed to map " + length + " bytes");
        }
        return addr;
    }

    private static void munmap(long addr, long length) {
        final int result;
        try {
            result = (int) INVOKE_MUNMAP.invokeExact(addr, length);
        } catch (Throwable e) {
            throw new Error(e); // Should not happen.
        }
        if (result != 0) {
            // Only EINVAL is possible for a range we mapped ourselves: a bug, not a resource problem.
            throw new IllegalStateException("munmap(2) failed for " + length + " bytes at 0x" +
                    Long.toHexString(addr));
        }
    }

    private static final class MappedDirectBuffer implements CleanableDirectBuffer {
        private final ByteBuffer buffer;
        private final long memoryAddress;
        private final long mappedLength;

        MappedDirectBuffer(int capacity) {
            // Account for the requested capacity, like every other Cleaner; the mapping is rounded up to pages.
            PlatformDependent.incrementMemoryCounter(capacity);
            long length = roundUpToPage(Math.max(capacity, 1));
            long addr;
            try {
                addr = mmap(length);
            } catch (Throwable e) {
                PlatformDependent.decrementMemoryCounter(capacity);
                throw e;
            }
            try {
                buffer = (ByteBuffer) INVOKE_CREATE_BYTEBUFFER.invokeExact(addr, (long) capacity);
            } catch (Throwable throwable) {
                PlatformDependent.decrementMemoryCounter(capacity);
                Error error = new Error(throwable);
                try {
                    munmap(addr, length);
                } catch (Throwable e) {
                    error.addSuppressed(e);
                }
                throw error;
            }
            memoryAddress = addr;
            mappedLength = length;
        }

        @Override
        public ByteBuffer buffer() {
            return buffer;
        }

        @Override
        public void clean() {
            munmap(memoryAddress, mappedLength);
            PlatformDependent.decrementMemoryCounter(buffer.capacity());
        }

        @Override
        public boolean hasMemoryAddress() {
            return true;
        }

        @Override
        public long memoryAddress() {
            return memoryAddress;
        }

        @Override
        public boolean purge(int offset, int length) {
            if (offset < 0 || length < 0 || offset > buffer.capacity() - length) {
                throw new IndexOutOfBoundsException("offset: " + offset + ", length: " + length +
                        ", capacity: " + buffer.capacity());
            }
            // Only the pages wholly inside the range: MADV_DONTNEED zeroes whole pages.
            long start = roundUpToPage(memoryAddress + offset);
            long end = memoryAddress + offset + length & -PAGE_SIZE;
            if (start >= end) {
                return true;
            }
            try {
                return (int) INVOKE_MADVISE.invokeExact(start, end - start, MADV_DONTNEED) == 0;
            } catch (Throwable e) {
                throw new Error(e); // Should not happen.
            }
        }
    }
}
