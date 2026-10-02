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
@Name(PageStorePurgeEvent.NAME)
@Label("Page Store Purge")
@Description("One madvise(MADV_DONTNEED) call of the page store: a run of idle free slices of one block")
final class PageStorePurgeEvent extends AbstractPageStoreEvent {
    static final String NAME = "io.netty.PageStorePurge";
    private static final PageStorePurgeEvent INSTANCE = new PageStorePurgeEvent();

    static boolean isEventEnabled() {
        return INSTANCE.isEnabled();
    }

    /** A begun event, as an {@link Object} so that callers need not name this class outside a guarded branch. */
    static Object start() {
        PageStorePurgeEvent event = new PageStorePurgeEvent();
        event.begin();
        return event;
    }
}
