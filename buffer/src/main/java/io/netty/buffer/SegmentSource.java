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

/**
 * Where the {@link Segment}s of a direct allocator come from: the memory the allocator is built on, as for any
 * chunk buffer (libc {@code malloc} behind {@link UnsafeByteBufUtil#newDirectByteBuf}). A segment goes back by
 * {@link AbstractByteBuf#release()}.
 */
interface SegmentSource {
    /** A new segment buffer of {@code size} bytes. */
    AbstractByteBuf allocateSegment(int size);

    /**
     * A buffer over {@code length} bytes of {@code segment} from {@code offset}, of the same class as
     * {@code segment} (the chunks of a size class and every other chunk then look alike to the buffers that read
     * them) and never freeing any memory: releasing it, or not, changes nothing to the segment.
     */
    AbstractByteBuf span(AbstractByteBuf segment, int offset, int length);

    /** Where regions of segments come from, or {@code null} when this source has none. */
    default RegionSource regionSource() {
        return null;
    }
}
