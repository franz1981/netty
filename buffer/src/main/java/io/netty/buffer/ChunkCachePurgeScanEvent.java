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
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@SuppressWarnings("Since15")
@Name(ChunkCachePurgeScanEvent.NAME)
@Label("Chunk Cache Purge Scan")
@Description("Triggered when a chunk cache purge scan completes")
@Enabled(false)
@Category("Netty")
final class ChunkCachePurgeScanEvent extends Event {
    static final String NAME = "io.netty.ChunkCachePurgeScan";
    private static final ChunkCachePurgeScanEvent INSTANCE = new ChunkCachePurgeScanEvent();

    public static boolean isEventEnabled() {
        return INSTANCE.isEnabled();
    }

    @Label("Thread local")
    @Description("Whether this is a thread-local or shared cache")
    public boolean threadLocal;
    @Label("Scanned")
    @Description("Number of chunks scanned")
    public int scanned;
    @Label("Evicted")
    @Description("Number of chunks evicted (markToDeallocate)")
    public int evicted;
    @Label("Kept")
    @Description("Number of chunks retained after purge")
    public int kept;
    @Label("Not empty count")
    @Description("Number of chunks with remaining capacity after partition")
    public int notEmptyCount;
    @Label("Partition swaps")
    @Description("Number of swaps performed during partition")
    public int partitionSwaps;
}
