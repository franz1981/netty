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
import jdk.jfr.Label;
import jdk.jfr.Name;

@SuppressWarnings("Since15")
@Name(PageStoreUnmapEvent.NAME)
@Label("Page Store Unmap")
@Description("A region of the page store was unmapped, when its allocator was closed")
final class PageStoreUnmapEvent extends AbstractPageStoreEvent {
    static final String NAME = "io.netty.PageStoreUnmap";
    private static final PageStoreUnmapEvent INSTANCE = new PageStoreUnmapEvent();

    static boolean isEventEnabled() {
        return INSTANCE.isEnabled();
    }

    /** A begun event, as an {@link Object} so that callers need not name this class outside a guarded branch. */
    static Object start() {
        PageStoreUnmapEvent event = new PageStoreUnmapEvent();
        event.begin();
        return event;
    }
}
