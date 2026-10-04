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
import io.netty.util.internal.NativeCallException;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;

/**
 * Regions mapped with their own anonymous {@code mmap(2)}, trimmed to start at a multiple of the alignment, unmapped
 * when released, and purged page-wise with {@code madvise(MADV_DONTNEED)}: the memory of a purged range goes back to
 * the OS at once, and reads zero when touched again.
 * <p>
 * The three calls are {@link PlatformDependent#mmapAnonymous}, {@link PlatformDependent#munmap} and
 * {@link PlatformDependent#madviseDontNeed}, bound once for the whole JVM; {@link #isAvailable()} is
 * {@link PlatformDependent#hasMmap()}. Wrapping a mapped address as a {@link ByteBuffer} is
 * {@link PlatformDependent#mappedBuffer(long, int)}, independent of {@code sun.misc.Unsafe}: unlike
 * {@link PlatformDependent#directBuffer(long, int)}, it still works with {@code --sun-misc-unsafe-memory-access=deny}.
 * <p>
 * Neither the mapping nor a purge is charged to or credited from {@code PlatformDependent}'s direct memory counter:
 * {@link PageStore} charges the slots it commits.
 */
class MmapRegionSource {
    static boolean isAvailable() {
        return PlatformDependent.hasMmap();
    }

    private final ByteBufAllocator allocator;

    MmapRegionSource(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Maps {@code size + alignment} bytes, then unmaps the head and the tail around the first multiple of
     * {@code alignment}: three system calls, no memory touched. Of the class {@link UnsafeByteBufUtil#newDirectByteBuf}
     * picks, so that the spans of a region are of the class of a segment allocated on its own; its release unmaps it.
     */
    AbstractByteBuf allocateRegion(int size, int alignment) {
        if (!isAvailable()) {
            throw new UnsupportedOperationException("mmap(2) regions are not available");
        }
        if (size <= 0 || alignment < 0 || (alignment & alignment - 1) != 0) {
            throw new IllegalArgumentException("size: " + size + ", alignment: " + alignment);
        }
        long mapped = (long) size + alignment;
        long address = PlatformDependent.mmapAnonymous(mapped);
        long start = alignment == 0 ? address : address + alignment - 1 & -alignment;
        long head = start - address;
        long tail = mapped - head - size;
        if (head > 0) {
            try {
                PlatformDependent.munmap(address, head);
            } catch (NativeCallException e) {
                throw unmapAfter(e, address, mapped);
            }
        }
        if (tail > 0) {
            try {
                PlatformDependent.munmap(start + size, tail);
            } catch (NativeCallException e) {
                throw unmapAfter(e, start, size + tail);
            }
        }
        ByteBuffer buffer;
        try {
            buffer = PlatformDependent.mappedBuffer(start, size);
        } catch (Throwable t) {
            throw unmapAfter(new IllegalStateException("cannot wrap a mapping of " + size + " bytes", t), start, size);
        }
        return UnsafeByteBufUtil.newDirectByteBuf(allocator, new Mapping(buffer, start));
    }

    /** After {@code failure}, unmaps what is left of a new mapping, or records in {@code failure} that it leaked. */
    private static RuntimeException unmapAfter(RuntimeException failure, long address, long length) {
        try {
            PlatformDependent.munmap(address, length);
        } catch (NativeCallException e) {
            failure.addSuppressed(new IllegalStateException(length + " bytes at 0x" + Long.toHexString(address)
                    + " stay mapped, leaked", e));
        }
        return failure;
    }

    /**
     * {@code madvise(MADV_DONTNEED)} on {@code length} bytes of {@code region} from {@code offset}, whole pages: the
     * kernel rejects an unaligned start but rounds a partial last page up, discarding the next page's data. One
     * system call, and one TLB shootdown round on the CPUs that ran this process. The range reads zero afterwards.
     */
    void purge(AbstractByteBuf region, int offset, int length) {
        if (offset < 0 || length <= 0 || offset > region.capacity() - length) {
            throw new IllegalArgumentException("offset: " + offset + ", length: " + length + " of "
                    + region.capacity() + " bytes");
        }
        PlatformDependent.madviseDontNeed(mappingOf(region).address + offset, length);
    }

    /** An empty region, out of use for good, goes back. */
    void releaseRegion(AbstractByteBuf region) {
        region.release();
    }

    /** The start of {@code region}'s mapping. */
    static long addressOf(AbstractByteBuf region) {
        return mappingOf(region).address;
    }

    private static Mapping mappingOf(AbstractByteBuf region) {
        return (Mapping) ((UnpooledDirectByteBuf) region).cleanable;
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
            PlatformDependent.munmap(address, buffer.capacity());
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
