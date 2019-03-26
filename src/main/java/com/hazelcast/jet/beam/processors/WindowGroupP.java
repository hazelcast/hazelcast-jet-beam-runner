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
import org.apache.beam.sdk.transforms.windowing.WindowFn;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;

public class WindowGroupP<T, K> extends AbstractProcessor {
    private final WindowingStrategy<T, BoundedWindow> windowingStrategy;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove, useful for debugging

    private final Map<K, Map<BoundedWindow, List<WindowedValue<KV<K, T>>>>> keyToWindowToList = new HashMap<>();
    private Traverser<WindowedValue<KV<K, List<T>>>> resultTraverser;

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
            keyToWindowToList
                    .computeIfAbsent(key, k -> new HashMap<>())
                    .computeIfAbsent(window, w -> new ArrayList<>())
                    .add(windowedValue);
        }
        return true;
    }

    @Override
    public boolean complete() {
        //System.out.println(WindowGroupP.class.getSimpleName() + " COMPLETE ownerId = " + ownerId); //useful for debugging
        if (resultTraverser == null) {
            ResettableMergeContext mergeContext = new ResettableMergeContext<>(windowingStrategy.getWindowFn());
            resultTraverser = traverseStream(keyToWindowToList.entrySet().stream())
                    .flatMap(mainEntry -> {
                        K key = mainEntry.getKey();
                        @SuppressWarnings("unchecked")
                        Map<BoundedWindow, List<WindowedValue<KV<K, T>>>> subMap =
                                mergeWindows(mergeContext, mainEntry.getValue());
                        return traverseStream(subMap.entrySet().stream())
                                .map(e -> createOutput(key, e.getKey(), e.getValue()));
                    });
        }
        return emitFromTraverser(resultTraverser);
    }

    private Map<BoundedWindow, List<WindowedValue<KV<K, T>>>> mergeWindows(
            ResettableMergeContext<T> mergeContext,
            Map<BoundedWindow, List<WindowedValue<KV<K, T>>>> windowToListMap
    ) {
        if (windowingStrategy.getWindowFn().isNonMerging()) {
            return windowToListMap;
        }
        mergeContext.reset(windowToListMap.keySet());
        try {
            windowingStrategy.getWindowFn().mergeWindows(mergeContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // shortcut - no windows merged
        if (mergeContext.windowToMergeResult.entrySet().stream().allMatch(en -> en.getKey().equals(en.getValue()))) {
            return windowToListMap;
        }

        Map<BoundedWindow, List<WindowedValue<KV<K, T>>>> mergedWindowToListMap = new HashMap<>();
        for (Entry<BoundedWindow, List<WindowedValue<KV<K, T>>>> windowEntry : windowToListMap.entrySet()) {
            BoundedWindow initialWindow = windowEntry.getKey();
            BoundedWindow finalWindow = mergeContext.windowToMergeResult.getOrDefault(initialWindow, initialWindow);
            mergedWindowToListMap
                    .merge(finalWindow,
                            windowEntry.getValue(),
                            (l1, l2) -> Stream.of(l1, l2).flatMap(Collection::stream).collect(Collectors.toList()));
        }
        return mergedWindowToListMap;
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

    private static class ResettableMergeContext<T> extends WindowFn<T, BoundedWindow>.MergeContext {
        private Set<BoundedWindow> windows;
        private Map<BoundedWindow, BoundedWindow> windowToMergeResult = new HashMap<>();

        ResettableMergeContext(WindowFn<T, BoundedWindow> windowFn) {
            windowFn.super();
        }

        @Override
        public Collection<BoundedWindow> windows() {
            return windows;
        }

        @Override
        public void merge(Collection<BoundedWindow> toBeMerged, BoundedWindow mergeResult) {
            for (BoundedWindow w : toBeMerged) {
                windowToMergeResult.put(w, mergeResult);
            }
        }

        void reset(Set<BoundedWindow> windows) {
            this.windows = windows;
            windowToMergeResult.clear();
        }
    }
}
