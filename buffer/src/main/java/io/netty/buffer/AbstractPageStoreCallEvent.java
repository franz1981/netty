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

import jdk.jfr.Description;

/**
 * A system call of the {@link PageStore}: the event's duration is the call's. Begun before the call, committed after.
 */
@SuppressWarnings("Since15")
abstract class AbstractPageStoreCallEvent extends AbstractPageStoreEvent {
    @Description("errno of the failed call, 0 when it succeeded, -1 when it failed with no errno")
    public int errno;

    /** {@code event} is one of this class, begun: fill and commit it. */
    static void end(Object event, long address, long length, int region, Throwable failure) {
        AbstractPageStoreCallEvent e = (AbstractPageStoreCallEvent) event;
        e.end();
        if (e.shouldCommit()) {
            e.address = address;
            e.length = length;
            e.region = region;
            e.errno = failure == null ? 0 : MmapRegionSource.errnoOf(failure);
            e.commit();
        }
    }
}
