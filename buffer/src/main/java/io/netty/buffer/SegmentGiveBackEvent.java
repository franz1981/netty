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
@Name(SegmentGiveBackEvent.NAME)
@Label("Segment Give Back")
@Description("A segment, or a run of region slots, was given back")
final class SegmentGiveBackEvent extends AbstractPageStoreEvent {
    static final String NAME = "io.netty.SegmentGiveBack";
    private static final SegmentGiveBackEvent INSTANCE = new SegmentGiveBackEvent();

    static boolean isEventEnabled() {
        return INSTANCE.isEnabled();
    }

    @Description("Segments of the range: 1, or the slots of a run")
    public int segments;
    @Description("Where it went: free-slot (its region, purged after the purge delay), heap-reserve, own-freed "
            + "(its own allocation, freed), slot-run")
    public String destination;
    @Description("The heap on whose behalf: stripe, thread-local, or none (a buffer of its own, or any thread)")
    public String heap;

    static void commit(long address, long length, int segments, int region, String destination, String heap) {
        SegmentGiveBackEvent event = new SegmentGiveBackEvent();
        if (event.shouldCommit()) {
            event.address = address;
            event.length = length;
            event.segments = segments;
            event.region = region;
            event.destination = destination;
            event.heap = heap;
            event.commit();
        }
    }
}
