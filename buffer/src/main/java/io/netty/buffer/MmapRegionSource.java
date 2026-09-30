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

import io.netty.util.internal.CleanableDirectBuffer;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Optional;

import static java.lang.invoke.MethodType.methodType;

/**
 * Regions mapped with their own anonymous {@code mmap(2)}, trimmed to start at a multiple of the alignment, unmapped
 * when released, and purged page-wise with {@code madvise(MADV_DONTNEED)}: the memory of a purged range goes back to
 * the OS at once, and reads zero when touched again.
 * <p>
 * libc's {@code mmap}, {@code munmap} and {@code madvise} are bound through {@code java.lang.foreign.Linker}, found by
 * reflection so that nothing here references {@code java.lang.foreign} at compile time: available on Java 22+, 64-bit
 * Linux x86_64 or aarch64 (the {@code PROT_*}, {@code MAP_*} and {@code MADV_*} values below are those of both), with
 * native access enabled for this class's module, never in a native image.
 * <p>
 * Neither the mapping nor a purge is charged to or credited from {@code PlatformDependent}'s direct memory counter:
 * {@link PageStore} charges the slots it commits.
 */
final class MmapRegionSource implements RegionSource {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MmapRegionSource.class);

    private static final int PROT_READ = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_PRIVATE = 0x02;
    private static final int MAP_ANONYMOUS = 0x20;
    /** No swap reserved up front: the memory is committed page by page, as it is touched. */
    private static final int MAP_NORESERVE = 0x4000;
    private static final int MADV_DONTNEED = 4;
    private static final long MAP_FAILED = -1L;

    private static final MethodHandle MMAP;
    private static final MethodHandle MUNMAP;
    private static final MethodHandle MADVISE;
    /** {@code (long address, long size) -> ByteBuffer}. */
    private static final MethodHandle WRAP;

    static {
        MethodHandle mmap = null;
        MethodHandle munmap = null;
        MethodHandle madvise = null;
        MethodHandle wrap = null;
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
                    .invoke(MmapRegionSource.class);
            if (!(Boolean) lookup.findVirtual(moduleCls, "isNativeAccessEnabled", methodType(boolean.class))
                    .invoke(module)) {
                throw new UnsupportedOperationException("native access is not enabled for " + module);
            }

            Class<?> memoryLayoutCls = Class.forName("java.lang.foreign.MemoryLayout");
            Class<?> valueLayoutCls = Class.forName("java.lang.foreign.ValueLayout");
            Class<?> addressLayoutCls = Class.forName("java.lang.foreign.AddressLayout");
            Class<?> linkerCls = Class.forName("java.lang.foreign.Linker");
            Class<?> linkerOptionCls = Class.forName("java.lang.foreign.Linker$Option");
            Class<?> symbolLookupCls = Class.forName("java.lang.foreign.SymbolLookup");
            Class<?> memSegCls = Class.forName("java.lang.foreign.MemorySegment");
            Class<?> funcDescCls = Class.forName("java.lang.foreign.FunctionDescriptor");

            // Pointers, size_t and off_t are passed as Java longs.
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
            MethodHandle find = lookup.findVirtual(symbolLookupCls, "find", methodType(Optional.class, String.class));
            MethodHandle downcallHandle = lookup.findVirtual(linkerCls, "downcallHandle", methodType(
                    MethodHandle.class, memSegCls, funcDescCls, Array.newInstance(linkerOptionCls, 0).getClass()))
                    .asFixedArity();
            MethodHandle descriptorOf = lookup.findStatic(funcDescCls, "of", methodType(funcDescCls,
                    memoryLayoutCls, Array.newInstance(memoryLayoutCls, 0).getClass())).asFixedArity();
            Object noOptions = Array.newInstance(linkerOptionCls, 0);
            Object j = lookup.findStaticGetter(valueLayoutCls, "JAVA_LONG",
                    Class.forName("java.lang.foreign.ValueLayout$OfLong")).invoke();
            Object i = lookup.findStaticGetter(valueLayoutCls, "JAVA_INT",
                    Class.forName("java.lang.foreign.ValueLayout$OfInt")).invoke();

            // void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
            mmap = link(linker, symbols, find, downcallHandle, descriptorOf, memoryLayoutCls, noOptions,
                    "mmap", j, j, j, i, i, i, j);
            // int munmap(void *addr, size_t length)
            munmap = link(linker, symbols, find, downcallHandle, descriptorOf, memoryLayoutCls, noOptions,
                    "munmap", i, j, j);
            // int madvise(void *addr, size_t length, int advice)
            madvise = link(linker, symbols, find, downcallHandle, descriptorOf, memoryLayoutCls, noOptions,
                    "madvise", i, j, j, i);

            // MemorySegment.ofAddress(address).reinterpret(size).asByteBuffer(), as CleanerJava24Linker wraps memory.
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
            error = t;
        }
        MMAP = mmap;
        MUNMAP = munmap;
        MADVISE = madvise;
        WRAP = wrap;
        if (error == null) {
            logger.debug("mmap(2) regions: available");
        } else {
            logger.debug("mmap(2) regions: unavailable", error);
        }
    }

    private static MethodHandle link(Object linker, Object symbols, MethodHandle find, MethodHandle downcallHandle,
                                     MethodHandle descriptorOf, Class<?> memoryLayoutCls, Object noOptions,
                                     String name, Object returnLayout, Object... argumentLayouts) throws Throwable {
        Object arguments = Array.newInstance(memoryLayoutCls, argumentLayouts.length);
        for (int k = 0; k < argumentLayouts.length; k++) {
            Array.set(arguments, k, argumentLayouts[k]);
        }
        Object descriptor = descriptorOf.invoke(returnLayout, arguments);
        Optional<?> symbol = (Optional<?>) find.invoke(symbols, name);
        if (!symbol.isPresent()) {
            throw new UnsupportedOperationException(name + " is not in the default lookup");
        }
        return (MethodHandle) downcallHandle.invoke(linker, symbol.get(), descriptor, noOptions);
    }

    static boolean isAvailable() {
        return MMAP != null;
    }

    private final ByteBufAllocator allocator;

    MmapRegionSource(ByteBufAllocator allocator) {
        if (!isAvailable()) {
            throw new UnsupportedOperationException("mmap(2) regions are not available");
        }
        this.allocator = allocator;
    }

    /**
     * Maps {@code size + alignment} bytes, then unmaps the head and the tail around the first multiple of
     * {@code alignment}: three system calls, no memory touched. Of the class {@link UnsafeByteBufUtil#newDirectByteBuf}
     * picks, so that the spans of a region are of the class of a segment allocated on its own; its release unmaps it.
     */
    @Override
    public AbstractByteBuf allocateRegion(int size, int alignment) {
        if (size <= 0 || alignment < 0 || (alignment & alignment - 1) != 0) {
            throw new IllegalArgumentException("size: " + size + ", alignment: " + alignment);
        }
        long mapped = (long) size + alignment;
        long address = mmap(mapped);
        long start = alignment == 0 ? address : address + alignment - 1 & -alignment;
        long head = start - address;
        long tail = mapped - head - size;
        if (head > 0) {
            munmap(address, head);
        }
        if (tail > 0) {
            munmap(start + size, tail);
        }
        ByteBuffer buffer;
        try {
            buffer = (ByteBuffer) WRAP.invokeExact(start, (long) size);
        } catch (Throwable t) {
            munmap(start, size);
            throw new IllegalStateException("cannot wrap a mapping of " + size + " bytes", t);
        }
        return UnsafeByteBufUtil.newDirectByteBuf(allocator, new Mapping(buffer, start));
    }

    /**
     * {@code madvise(MADV_DONTNEED)} on {@code length} bytes of {@code region} from {@code offset}, whole pages: one
     * system call, and one TLB shootdown round on the CPUs that ran this process. The range reads zero afterwards.
     */
    @Override
    public void purge(AbstractByteBuf region, int offset, int length) {
        if (offset < 0 || length <= 0 || offset > region.capacity() - length
                || ((offset | length) & PageStoreConfig.PAGE_SIZE_BYTES - 1) != 0) {
            throw new IllegalArgumentException("offset: " + offset + ", length: " + length + " of "
                    + region.capacity() + " bytes, pages of " + PageStoreConfig.PAGE_SIZE_BYTES);
        }
        long address = mappingOf(region).address + offset;
        int result;
        try {
            result = (int) MADVISE.invokeExact(address, (long) length, MADV_DONTNEED);
        } catch (Throwable t) {
            throw new Error(t);
        }
        if (result != 0) {
            throw new IllegalStateException("madvise(MADV_DONTNEED) failed for " + length + " bytes at 0x"
                    + Long.toHexString(address));
        }
    }

    /** The start of {@code region}'s mapping. */
    static long addressOf(AbstractByteBuf region) {
        return mappingOf(region).address;
    }

    private static Mapping mappingOf(AbstractByteBuf region) {
        return (Mapping) ((UnpooledDirectByteBuf) region).cleanable;
    }

    private static long mmap(long length) {
        long address;
        try {
            address = (long) MMAP.invokeExact(0L, length, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0L);
        } catch (Throwable t) {
            throw new Error(t);
        }
        if (address == MAP_FAILED) {
            throw new OutOfMemoryError("mmap(2) failed to map " + length + " bytes");
        }
        return address;
    }

    private static void munmap(long address, long length) {
        int result;
        try {
            result = (int) MUNMAP.invokeExact(address, length);
        } catch (Throwable t) {
            throw new Error(t);
        }
        if (result != 0) {
            // Only EINVAL is possible for a range of our own mapping: a bug.
            throw new IllegalStateException("munmap(2) failed for " + length + " bytes at 0x"
                    + Long.toHexString(address));
        }
    }

    /** One region's mapping: {@link #clean()} unmaps it. */
    private static final class Mapping implements CleanableDirectBuffer {
        private final ByteBuffer buffer;
        final long address;

        Mapping(ByteBuffer buffer, long address) {
            this.buffer = buffer;
            this.address = address;
        }

        @Override
        public ByteBuffer buffer() {
            return buffer;
        }

        @Override
        public void clean() {
            munmap(address, buffer.capacity());
        }

        @Override
        public boolean hasMemoryAddress() {
            return true;
        }

        @Override
        public long memoryAddress() {
            return address;
        }
    }
}
