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

import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.MemoryAddress;

/**
 * A system call of the adaptive allocator's {@link PageStore}: the event's duration is the call's. Begun before the
 * call, committed after; see {@link PageStoreEvents}.
 */
@Enabled(false)
@Category("Netty")
@SuppressWarnings("Since15")
abstract class AbstractPageStoreEvent extends Event {
    @Description("Start of the memory range")
    @MemoryAddress
    public long address;
    @DataAmount
    @Description("Length of the memory range")
    public long length;
    @Description("Index of the page store region the range is in, or -1")
    public int region;
    @Description("Whether the call failed")
    public boolean failed;
}
