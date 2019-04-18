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
import com.hazelcast.jet.beam.Utils;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseStream;

public class WindowGroupP<T, K> extends AbstractProcessor {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final Coder inputCoder;
    private final Coder outputCoder;
    private final WindowingStrategy<T, BoundedWindow> windowingStrategy;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove, useful for debugging

    private final Map<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>> windowToKeyToList = new HashMap<>();
    private Traverser<Object> flushTraverser;

    private WindowGroupP(
            Coder inputCoder,
            Coder outputCoder,
            WindowingStrategy<T, BoundedWindow> windowingStrategy,
            String ownerId
    ) {
        this.inputCoder = inputCoder;
        this.outputCoder = outputCoder;
        this.windowingStrategy = windowingStrategy;
        this.ownerId = ownerId;
        //System.out.println(WindowGroupP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        //System.out.println(WindowGroupP.class.getSimpleName() + " UPDATE ownerId = " + ownerId + ", item = " + item); //useful for debugging
        assert ordinal == 0;
        WindowedValue<KV<K, T>> windowedValue = Utils.decodeWindowedValue((byte[]) item, inputCoder);
        K key = windowedValue.getValue().getKey();

        for (BoundedWindow window : windowedValue.getWindows()) {
            windowToKeyToList
                    .computeIfAbsent(window, w -> new HashMap<>())
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(windowedValue);
        }
        mergeWindows(key);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return flush(watermark.timestamp(), true);
    }

    @Override
    public boolean complete() {
        return flush(Long.MAX_VALUE, false);
    }

    private boolean flush(long timestamp, boolean appendWatermark) {
        if (flushTraverser == null) {
            Iterator<Map.Entry<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>>> iterator =
                    windowToKeyToList.entrySet().iterator();

            // this traverses windowToKeyToList while filtering non-future items and
            Traverser<Map.Entry<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>>> windowTraverser = () -> {
                while (iterator.hasNext()) {
                    Entry<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>> next = iterator.next();
                    if (next.getKey().maxTimestamp().getMillis() < timestamp) {
                        iterator.remove();
                        return next;
                    }
                }
                return null;
            };
            flushTraverser = windowTraverser
                    .flatMap(mainEntry -> traverseStream(mainEntry.getValue().entrySet().stream())
                            .map(e -> (Object) createOutput(e.getKey(), mainEntry.getKey(), e.getValue())))
                    .onFirstNull(() -> flushTraverser = null);
            if (appendWatermark) {
                flushTraverser = flushTraverser.append(new Watermark(timestamp));
            }
        }
        return emitFromTraverser(flushTraverser);
    }

    private void mergeWindows(K key) {
        if (windowingStrategy.getWindowFn().isNonMerging()) {
            return;
        }
        try {
            windowingStrategy.getWindowFn().mergeWindows(windowingStrategy.getWindowFn().new MergeContext() {
                @Override
                public Collection<BoundedWindow> windows() {
                    return new WindowsForKeyCollection<>(windowToKeyToList, key);
                }

                @Override
                public void merge(Collection<BoundedWindow> windowsFrom, BoundedWindow windowTo) {
                    Map<K, List<WindowedValue<KV<K, T>>>> windowToData =
                            windowToKeyToList.computeIfAbsent(windowTo, x -> new HashMap<>());
                    List<WindowedValue<KV<K, T>>> toData = windowToData.get(key);

                    for (BoundedWindow windowFrom : windowsFrom) {
                        // shortcut - nothing to merge or change
                        if (windowFrom.equals(windowTo)) {
                            continue;
                        }
                        Map<K, List<WindowedValue<KV<K, T>>>> windowFromData = windowToKeyToList.get(windowFrom);
                        List<WindowedValue<KV<K, T>>> fromData = windowFromData.remove(key);
                        assert fromData != null : "fromData == null";
                        if (windowFromData.isEmpty()) {
                            // if the current key was the only key in the windowFrom, remove the window
                            windowToKeyToList.remove(windowFrom);
                        }
                        if (toData == null) {
                            toData = fromData;
                            windowToData.put(key, fromData);
                        } else {
                            toData.addAll(fromData);
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] createOutput(K key, BoundedWindow window, List<WindowedValue<KV<K, T>>> windowedValues) {
        assert !windowedValues.isEmpty() : "empty windowedValues";
        Instant timestamp = null;
        List<T> values = new ArrayList<>(windowedValues.size());
        TimestampCombiner timestampCombiner = windowingStrategy.getTimestampCombiner();
        for (WindowedValue<KV<K, T>> windowedValue : windowedValues) {
            if (!PaneInfo.NO_FIRING.equals(windowedValue.getPane())) throw new RuntimeException("Oops!");
            timestamp = timestamp == null ? windowedValue.getTimestamp()
                    : timestampCombiner.merge(window, timestamp, windowedValue.getTimestamp());
            values.add(windowedValue.getValue().getValue());
        }
        WindowedValue<KV<K, List<T>>> windowedValue = WindowedValue.of(KV.of(key, values), timestamp, window, PaneInfo.NO_FIRING);
        return Utils.encodeWindowedValue(windowedValue, outputCoder, baos);
    }

    @SuppressWarnings("unchecked")
    public static SupplierEx<Processor> supplier(
            Coder inputCoder,
            Coder outputCoder,
            WindowingStrategy windowingStrategy,
            String ownerId
    ) {
        return () -> new WindowGroupP<>(inputCoder, outputCoder, windowingStrategy, ownerId);
    }

    /**
     * A utility to iterate windows in windowToKeyToList but only those that
     * contain the given key with minimum garbage possible.
     * <p>
     * Only the {@link #iterator()} method is ever used on this class.
     */
    private static class WindowsForKeyCollection<K, T> extends AbstractCollection<BoundedWindow> {
        private final Map<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>> windowToKeyToList;
        private final K key;

        WindowsForKeyCollection(Map<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>> windowToKeyToList, K key) {
            this.windowToKeyToList = windowToKeyToList;
            this.key = key;
        }

        @Override @Nonnull
        public Iterator<BoundedWindow> iterator() {
            return new Iterator<BoundedWindow>() {
                private Iterator<Entry<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>>> iterator =
                        windowToKeyToList.entrySet().iterator();
                private BoundedWindow next;

                // constructor
                {
                    advance();
                }

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public BoundedWindow next() {
                    try {
                        return next;
                    } finally {
                        advance();
                    }
                }

                private void advance() {
                    next = null;
                    while (iterator.hasNext() && next == null) {
                        Entry<BoundedWindow, Map<K, List<WindowedValue<KV<K, T>>>>> next1 = iterator.next();
                        if (next1.getValue().containsKey(key)) {
                            next = next1.getKey();
                        }
                    }
                }
            };
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }
    }
}
