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

import java.util.concurrent.locks.StampedLock;

/**
 * A heap's link to the {@link PageStore}: {@link #claim} takes a run of the store's shared slices, and
 * {@link #release} gives it back, from any thread, heap freed or not. The heap holds no block.
 * <p>
 * Single writer for {@link #claim}: the holder of the stripe lock, or the thread of a thread-local heap. Chunk
 * creation and deallocation only.
 */
final class HeapSegments {
    private final PageStore store;
    /** For assertions only. */
    private final StampedLock stripeLock;
    private final Thread ownerThread;
    /** Where this heap starts looking among a region's blocks, as mimalloc's thread sequence. */
    final int seq;
    private int claimedStart = -1;

    HeapSegments(PageStore store, StampedLock stripeLock, Thread ownerThread) {
        assert (stripeLock == null) != (ownerThread == null);
        this.store = store;
        this.stripeLock = stripeLock;
        this.ownerThread = ownerThread;
        seq = store.nextHeapSequence();
    }

    /** {@link PageStore#STRIPE} or {@link PageStore#THREAD_LOCAL}. */
    String kind() {
        return ownerThread != null ? PageStore.THREAD_LOCAL : PageStore.STRIPE;
    }

    private boolean inOwnerContext() {
        return ownerThread != null ? Thread.currentThread() == ownerThread : stripeLock.isWriteLocked();
    }

    /** Returns the block of the span; {@link #claimedStart()} is its first slice. */
    Segment claim(int slices) {
        assert inOwnerContext();
        long run = store.claimSlices(slices, seq, kind());
        int perBlock = store.config.slicesPerSegment();
        int slice = (int) run;
        claimedStart = slice % perBlock;
        return store.region((int) (run >>> 32)).block(slice / perBlock);
    }

    int claimedStart() {
        return claimedStart;
    }

    /** Any thread. */
    void release(Segment segment, int start, int slices) {
        store.releaseSlices(segment, start, slices);
    }
}
