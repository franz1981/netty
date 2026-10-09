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

/** {@link PageStoreMapEvent} / {@link PageStorePurgeEvent} / {@link PageStoreUnmapEvent}: one around each syscall. */
final class PageStoreEvents {
    private PageStoreEvents() {
    }

    /** A begun {@link PageStoreMapEvent}, or {@code null} when it is disabled. */
    static AbstractPageStoreEvent beginMap() {
        return PlatformDependent.isJfrEnabled() && PageStoreMapEvent.isEventEnabled() ? PageStoreMapEvent.start()
                : null;
    }

    /** A begun {@link PageStorePurgeEvent}, or {@code null} when it is disabled. */
    static AbstractPageStoreEvent beginPurge() {
        return PlatformDependent.isJfrEnabled() && PageStorePurgeEvent.isEventEnabled() ? PageStorePurgeEvent.start()
                : null;
    }

    /** A begun {@link PageStoreUnmapEvent}, or {@code null} when it is disabled. */
    static AbstractPageStoreEvent beginUnmap() {
        return PlatformDependent.isJfrEnabled() && PageStoreUnmapEvent.isEventEnabled() ? PageStoreUnmapEvent.start()
                : null;
    }

    /** Fill and commit {@code event}; a no-op when it is {@code null} (disabled at {@code begin*}). */
    static void end(AbstractPageStoreEvent event, long address, long length, int region, Throwable failure) {
        if (event == null) {
            return;
        }
        event.end();
        if (event.shouldCommit()) {
            event.address = address;
            event.length = length;
            event.region = region;
            event.failed = failure != null;
            event.commit();
        }
    }
}
