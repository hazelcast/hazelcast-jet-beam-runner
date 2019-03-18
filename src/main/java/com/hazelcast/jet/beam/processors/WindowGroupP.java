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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.util.Preconditions.checkTrue;

public class WindowGroupP<W, K, A, R, OUT> extends AbstractProcessor {
    private final List<FunctionEx<?, ? extends K>> groupKeyFns;
    private final List<FunctionEx<?, Collection<? extends W>>> groupWindowFns;
    private final FunctionEx<Map<K, Map<W, A>>, Map<K, Map<W, A>>> mergeWindowsFn;
    private final AggregateOperation<A, R> aggrOp;
    private final TriFunction<? super K, ? super W, ? super R, OUT> mapToOutputFn;

    private final Map<K, Map<W, A>> keyToWindowToAcc = new HashMap<>();
    private Traverser<OUT> resultTraverser;

    private WindowGroupP(
            FunctionEx<?, ? extends K> groupKeyFn,
            FunctionEx<?, Collection<? extends W>> groupWindowFn,
            FunctionEx<Map<K, Map<W, A>>, Map<K, Map<W, A>>> mergeWindowsFn,
            AggregateOperation<A, R> aggrOp,
            TriFunction<? super K, ? super W, ? super R, OUT> mapToOutputFn
    ) {
        // todo handle more inputs
        this.groupKeyFns = Collections.singletonList(groupKeyFn);
        checkTrue(groupKeyFns.size() == aggrOp.arity(), groupKeyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");

        this.groupWindowFns = Collections.singletonList(groupWindowFn);
        checkTrue(groupWindowFns.size() == aggrOp.arity(), groupWindowFns.size() + " window functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");

        this.mergeWindowsFn = mergeWindowsFn;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Function<Object, Collection<? extends W>> windowFn = (Function<Object, Collection<? extends W>>) groupWindowFns.get(ordinal);
        Collection<? extends W> windows = windowFn.apply(item);

        Function<Object, ? extends K> keyFn = (Function<Object, ? extends K>) groupKeyFns.get(ordinal);
        K key = keyFn.apply(item);

        for (W window : windows) {
            A acc = keyToWindowToAcc
                    .computeIfAbsent(key, k -> new HashMap<>())
                    .computeIfAbsent(window, w -> aggrOp.createFn().get());

            aggrOp.accumulateFn(ordinal).accept(acc, item);
        }

        return true;
    }

    @Override
    public boolean complete() {
        if (resultTraverser == null) {
            resultTraverser = traverseStream(
                    mergeWindowsFn.apply(keyToWindowToAcc)
                                  .entrySet().stream()
                                  .flatMap(
                                          mainEntry -> {
                                              K key = mainEntry.getKey();
                                              Map<W, A> subEntry = mainEntry.getValue();
                                              return subEntry.entrySet().stream()
                                                             .map(e -> mapToOutputFn.apply(key, e.getKey(), aggrOp.finishFn().apply(e.getValue())));
                                          }
                                  )
            );
        }
        return emitFromTraverser(resultTraverser);
    }

    public static SupplierEx<Processor> supplier(WindowingStrategy windowingStrategy) {
        return new WindowGroupProcessorSupplier(windowingStrategy);
    }

    private static class WindowGroupProcessorSupplier implements SupplierEx<Processor> {

        private final SupplierEx<Processor> underlying;

        @SuppressWarnings("unchecked")
        private WindowGroupProcessorSupplier(WindowingStrategy windowingStrategy) {
            this.underlying = () -> new WindowGroupP<>(
                    new KeyExtractorFunction(),
                    new WindowExtractorFunction(),
                    new WindowMergingFunction(windowingStrategy.getWindowFn()),
                    AggregateOperations.toList(),
                    new WindowedValueMerger(windowingStrategy.getTimestampCombiner())
            );
        }

        @Override
        public Processor getEx() throws Exception {
            return underlying.getEx();
        }

    }

    private static class WindowExtractorFunction<K, InputT> implements FunctionEx<WindowedValue<KV<K, InputT>>, Collection<? extends BoundedWindow>> {
        @Override
        public Collection<? extends BoundedWindow> applyEx(WindowedValue<KV<K, InputT>> kvWindowedValue) {
            return kvWindowedValue.getWindows();
        }
    }

    private static class KeyExtractorFunction<K, InputT> implements FunctionEx<WindowedValue<KV<K, InputT>>, K> {
        @Override
        public K applyEx(WindowedValue<KV<K, InputT>> kvWindowedValue) {
            return kvWindowedValue.getValue().getKey();
        }
    }

    private static class WindowMergingFunction<K, InputT> implements FunctionEx<
            Map<K, Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>>>,
            Map<K, Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>>>> {

        private final WindowFn windowFn;

        private WindowMergingFunction(WindowFn windowFn) {
            this.windowFn = windowFn;
        }

        @Override
        public Map<K, Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>>> applyEx(
                Map<K, Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>>> keyToWindowToListMap
        ) {
            if (windowFn.isNonMerging()) return keyToWindowToListMap;

            Map<BoundedWindow, BoundedWindow> initialToFinalWindows = new HashMap<>();
            for (Map.Entry<K, Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>>> keyEntry : keyToWindowToListMap.entrySet()) {
                K key = keyEntry.getKey();
                Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>> windowToListMap = keyEntry.getValue();

                initialToFinalWindows.clear();
                try {
                    // todo reuse the context
                    windowFn.mergeWindows(new MergeContextImpl(windowFn, windowToListMap.keySet(), initialToFinalWindows));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                // optimization - no need to rebuild the map
                if (initialToFinalWindows.entrySet().stream().allMatch(en -> en.getKey().equals(en.getValue()))) {
                    continue;
                }

                Map<BoundedWindow, List<WindowedValue<KV<K, InputT>>>> mergedWindowToListMap = new HashMap<>();
                for (Entry<BoundedWindow, List<WindowedValue<KV<K, InputT>>>> windowEntry : windowToListMap.entrySet()) {
                    BoundedWindow initialWindow = windowEntry.getKey();
                    BoundedWindow finalWindow = initialToFinalWindows.getOrDefault(initialWindow, initialWindow);
                    mergedWindowToListMap
                            .merge(finalWindow,
                                    windowEntry.getValue(),
                                    (l1, l2) -> Stream.of(l1, l2).flatMap(Collection::stream).collect(Collectors.toList()));
                }
                keyToWindowToListMap.put(key, mergedWindowToListMap);
            }

            return keyToWindowToListMap;
        }
    }

    private static class MergeContextImpl extends WindowFn<Object, BoundedWindow>.MergeContext {

        private Set<BoundedWindow> windows;
        private Map<BoundedWindow, BoundedWindow> windowToMergeResult;

        MergeContextImpl(WindowFn<Object, BoundedWindow> windowFn, Set<BoundedWindow> windows, Map<BoundedWindow, BoundedWindow> windowToMergeResult) {
            windowFn.super();
            this.windows = windows;
            this.windowToMergeResult = windowToMergeResult;
        }

        @Override
        public Collection<BoundedWindow> windows() {
            return windows;
        }

        @Override
        public void merge(Collection<BoundedWindow> toBeMerged, BoundedWindow mergeResult) throws Exception {
            for (BoundedWindow w : toBeMerged) {
                windowToMergeResult.put(w, mergeResult);
            }
        }
    }

    private static class WindowedValueMerger<K, InputT> implements TriFunction<K, BoundedWindow,
            List<WindowedValue<KV<K, InputT>>>, WindowedValue<KV<K, Iterable<InputT>>>> {

        private final TimestampCombiner timestampCombiner;

        WindowedValueMerger(TimestampCombiner timestampCombiner) {
            this.timestampCombiner = timestampCombiner;
        }

        @Override
        public WindowedValue<KV<K, Iterable<InputT>>> applyEx(
                K k,
                BoundedWindow boundedWindow,
                List<WindowedValue<KV<K, InputT>>> windowedValues
        ) {
            assert !windowedValues.isEmpty() : "empty windowedValues";
            Instant instant = null;
            PaneInfo pane = PaneInfo.NO_FIRING; //todo: is this the right default?
            List<InputT> values = new ArrayList<>(windowedValues.size());
            for (WindowedValue<KV<K, InputT>> windowedValue : windowedValues) {
                if (pane == null) pane = windowedValue.getPane();
                instant = instant == null ? windowedValue.getTimestamp()
                        : timestampCombiner.combine(instant, windowedValue.getTimestamp());
                values.add(windowedValue.getValue().getValue());
            }
            return WindowedValue.of(KV.of(k, values), instant, boundedWindow, pane);
        }
    }
}
