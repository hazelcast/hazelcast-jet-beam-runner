/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.processor.TransformP;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.joda.time.Instant;

import java.util.Collection;

public class AssignWindowP extends TransformP {

    private final String ownerId;

    public AssignWindowP(String ownerId, WindowingStrategy windowingStrategy) {
        super(
                (FunctionEx<Object, Traverser<?>>) o -> {
                    WindowedValue input = (WindowedValue) o;
                    Collection<? extends BoundedWindow> windows; //todo: tons of garbage!
                    WindowFn windowFn = windowingStrategy.getWindowFn();
                    try {
                        windows = windowFn.assignWindows(new WindowAssignContext<>(windowFn, input));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    }

                    return Traversers.traverseStream(
                            windows.stream().map(window -> WindowedValue.of(input.getValue(), input.getTimestamp(), window, input.getPane()))
                    );
                }
        );
        this.ownerId = ownerId;
    }

    public static SupplierEx<Processor> supplier(WindowingStrategy windowingStrategy, String ownerId) {
        return () -> new AssignWindowP(ownerId, windowingStrategy);
    }

    private static class WindowAssignContext<InputT, W extends BoundedWindow> extends WindowFn<InputT, W>.AssignContext {
        private final WindowedValue<InputT> value;

        WindowAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
            fn.super();
            if (Iterables.size(value.getWindows()) != 1) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s passed to window assignment must be in a single window, but it was in %s: %s",
                                WindowedValue.class.getSimpleName(),
                                Iterables.size(value.getWindows()),
                                value.getWindows()));
            }
            this.value = value;
        }

        @Override
        public InputT element() {
            return value.getValue();
        }

        @Override
        public Instant timestamp() {
            return value.getTimestamp();
        }

        @Override
        public BoundedWindow window() {
            return Iterables.getOnlyElement(value.getWindows());
        }
    }
}
