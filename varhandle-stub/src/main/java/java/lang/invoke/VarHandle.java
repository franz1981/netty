/*
 * Copyright 2025 The Netty Project
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
package java.lang.invoke;

public class VarHandle {

    @MethodHandle.PolymorphicSignature
    public native Object get(Object... args);

    @MethodHandle.PolymorphicSignature
    public native Object getAcquire(Object... args);

    @MethodHandle.PolymorphicSignature
    public native void set(Object... args);

    @MethodHandle.PolymorphicSignature
    public native void setRelease(Object... args);

    @MethodHandle.PolymorphicSignature
    public native Object getAndAdd(Object... args);

    @MethodHandle.PolymorphicSignature
    public native boolean compareAndSet(Object... args);

    public static void storeStoreFence() {
        throw new UnsupportedOperationException("Not implemented in varhandle-stub");
    }
}
