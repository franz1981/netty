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
package io.netty.incubator.allocator;

import jdk.jfr.Event;
import jdk.jfr.Registered;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * That no event class of the module, nor a superclass of one, declares a field typed as another event class:
 * registering the class would load that type while holding JFR's metadata lock, which deadlocks with a thread
 * defining it before the fix of JDK-8368670. The classes are looked at by reflection, not run: this is a rule about
 * their shape, which no observable of the allocator shows.
 */
@EnabledForJreRange(min = JRE.JAVA_11)
class PageStoreJfrEventShapeTest {
    private static final Class<?>[] EVENTS = {
            PageStoreMapEvent.class, PageStoreUnmapEvent.class, PageStorePurgeEvent.class,
            AllocateChunkEvent.class, FreeChunkEvent.class, AllocateBufferEvent.class,
            ReallocateBufferEvent.class, FreeBufferEvent.class, PageStoreStateEvent.class};

    /** The control: the shape that deadlocks, an event with a static field of another event's type. */
    @Registered(false)
    private static final class A extends Event {
        static B other;
    }

    @Registered(false)
    private static final class B extends Event {
    }

    /** The same shape through an array: loading the array type loads its component type. */
    @Registered(false)
    private static final class D extends Event {
        B[] others;
    }

    /** The fields of {@code type} and of its superclasses below {@link Event} typed as an event of another class. */
    private static List<String> fieldsOfOtherEventTypes(Class<?> type) {
        List<String> found = new ArrayList<String>();
        for (Class<?> c = type; c != Event.class && c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                Class<?> fieldType = field.getType();
                while (fieldType.isArray()) {
                    fieldType = fieldType.getComponentType();
                }
                if (Event.class.isAssignableFrom(fieldType) && fieldType != c) {
                    found.add(c.getName() + "." + field.getName() + " : " + field.getType().getName());
                }
            }
        }
        return found;
    }

    @Test
    void theCheckFindsAnEventFieldOfAnotherEventType() {
        assertEquals(Collections.singletonList(A.class.getName() + ".other : " + B.class.getName()),
                fieldsOfOtherEventTypes(A.class));
    }

    @Test
    void theCheckFindsAnArrayOfAnotherEventType() {
        assertEquals(Collections.singletonList(D.class.getName() + ".others : " + B[].class.getName()),
                fieldsOfOtherEventTypes(D.class));
    }

    @Test
    void noEventClassOfTheModuleHasAFieldOfAnotherEventType() {
        for (Class<?> event : EVENTS) {
            assertEquals(Collections.emptyList(), fieldsOfOtherEventTypes(event), event.getName());
        }
    }
}
