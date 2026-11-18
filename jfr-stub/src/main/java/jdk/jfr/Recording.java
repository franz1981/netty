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
package jdk.jfr;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public final class Recording implements Closeable {
    public Recording() {
        throw new UnsupportedOperationException("Stub should only be used at compile time");
    }

    public EventSettings enable(String name) {
        throw new UnsupportedOperationException("Stub should only be used at compile time");
    }

    public void start() {
        throw new UnsupportedOperationException("Stub should only be used at compile time");
    }

    public boolean stop() {
        throw new UnsupportedOperationException("Stub should only be used at compile time");
    }

    public void dump(Path destination) throws IOException {
        throw new UnsupportedOperationException("Stub should only be used at compile time");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Stub should only be used at compile time");
    }
}
