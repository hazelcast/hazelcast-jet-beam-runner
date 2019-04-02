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
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

import static com.hazelcast.jet.Traversers.traverseStream;

public class WindowGroupP<T, K> extends AbstractProcessor {
    private final WindowingStrategy<T, BoundedWindow> windowingStrategy;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove, useful for debugging

    private final Map<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>> windowToKeyToList = new HashMap<>();
    private Traverser<WindowedValue<KV<K, List<T>>>> completionTraverser;

    private WindowGroupP(WindowingStrategy<T, BoundedWindow> windowingStrategy, String ownerId) {
        this.windowingStrategy = windowingStrategy;
        this.ownerId = ownerId;
        //System.out.println(WindowGroupP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        //System.out.println(WindowGroupP.class.getSimpleName() + " UPDATE ownerId = " + ownerId + ", item = " + item); //useful for debugging
        assert ordinal == 0;
        WindowedValue<KV<K, T>> windowedValue = (WindowedValue) item;
        K key = windowedValue.getValue().getKey();

        for (BoundedWindow window : windowedValue.getWindows()) {
            windowToKeyToList
                    .computeIfAbsent(window, w -> new HashMap<>())
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(windowedValue);
        }
        return true;
    }

    @Override
    public boolean complete() {
        if (completionTraverser == null) {
            mergeWindows();
            completionTraverser = traverseStream(windowToKeyToList.entrySet().stream())
                    .flatMap(mainEntry -> traverseStream(mainEntry.getValue().entrySet().stream())
                            .map(e -> createOutput(e.getKey(), mainEntry.getKey(), e.getValue())));
        }
        return emitFromTraverser(completionTraverser);
    }

    private void mergeWindows() {
        if (windowingStrategy.getWindowFn().isNonMerging()) {
            return;
        }
        try {
            windowingStrategy.getWindowFn().mergeWindows(windowingStrategy.getWindowFn().new MergeContext() {
                @Override
                public Collection windows() {
                    return windowToKeyToList.keySet();
                }

                @Override
                public void merge(Collection<BoundedWindow> windowsFrom, BoundedWindow windowTo) {
                    BiFunction<List<WindowedValue<KV<K, T>>>, List<WindowedValue<KV<K, T>>>, List<WindowedValue<KV<K, T>>>> mappingFn = mergeToFirstList();
                    for (BoundedWindow windowFrom : windowsFrom) {
                        // shortcut - nothing to merge or change
                        if (windowFrom.equals(windowTo)) {
                            continue;
                        }
                        Map<K, List<WindowedValue<KV<K, T>>>> windowToData =
                                windowToKeyToList.computeIfAbsent(windowTo, x -> new HashMap<>());
                        // shortcut - just move from one window to another
                        Map<K, List<WindowedValue<KV<K, T>>>> windowFromData = windowToKeyToList.remove(windowFrom);
                        assert windowFromData != null : "windowFromData == null";
                        if (windowToData.isEmpty()) {
                            windowToKeyToList.put(windowTo, windowFromData);
                            continue;
                        }
                        for (Entry<K, List<WindowedValue<KV<K, T>>>> entry : windowFromData.entrySet()) {
                            windowToData.merge(entry.getKey(), entry.getValue(), mappingFn);
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private WindowedValue<KV<K, List<T>>> createOutput(K key, BoundedWindow window, List<WindowedValue<KV<K, T>>> windowedValues) {
        assert !windowedValues.isEmpty() : "empty windowedValues";
        Instant timestamp = null;
        List<T> values = new ArrayList<>(windowedValues.size());
        for (WindowedValue<KV<K, T>> windowedValue : windowedValues) {
            if (!PaneInfo.NO_FIRING.equals(windowedValue.getPane())) throw new RuntimeException("Oops!");
            timestamp = timestamp == null ? windowedValue.getTimestamp()
                    : windowingStrategy.getTimestampCombiner().combine(timestamp, windowedValue.getTimestamp());
            values.add(windowedValue.getValue().getValue());
        }
        return WindowedValue.of(KV.of(key, values), timestamp, window, PaneInfo.NO_FIRING);
    }

    @SuppressWarnings("unchecked")
    public static SupplierEx<Processor> supplier(WindowingStrategy windowingStrategy, String ownerId) {
        return () -> new WindowGroupP<>(windowingStrategy, ownerId);
    }

    private static <T> BiFunction<List<T>, List<T>, List<T>> mergeToFirstList() {
        return (first, second) -> {
            first.addAll(second);
            return first;
        };
    }
}
