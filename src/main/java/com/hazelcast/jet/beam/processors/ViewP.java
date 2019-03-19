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
import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ViewP extends AbstractProcessor {

    private final TimestampCombiner timestampCombiner;
    private final PCollectionView view;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove, useful for debugging

    private Map<BoundedWindow, List<Object>> values = new HashMap<>();
    private Map<BoundedWindow, Instant> timestamps = new HashMap<>();
    private PaneInfo paneInfo = PaneInfo.NO_FIRING;
    private Traverser<SideInputValue> resultTraverser;

    private ViewP(PCollectionView view, WindowingStrategy windowingStrategy, String ownerId) {
        this.timestampCombiner = windowingStrategy.getTimestampCombiner();
        this.view = view;
        this.ownerId = ownerId;
    }

    @Override
    public boolean complete() {
        if (values.isEmpty()) return true;

        if (resultTraverser == null) {
            resultTraverser = Traversers.traverseStream(
                    values.entrySet().stream()
                    .map(
                            e -> {
                                BoundedWindow window = e.getKey();
                                List<Object> values = e.getValue();
                                Instant timestamp = timestamps.get(window);
                                return new SideInputValue(view, WindowedValue.of(values, timestamp, Collections.singleton(window), paneInfo));
                            }
                    )
            );
        }
        return emitFromTraverser(resultTraverser);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        WindowedValue windowedValue = (WindowedValue) item;

        for (BoundedWindow window : (Iterable<? extends BoundedWindow>) windowedValue.getWindows()) {
            values
                    .computeIfAbsent(window, w -> new ArrayList<>())
                    .add(windowedValue.getValue());

            timestamps.merge(
                    window,
                    windowedValue.getTimestamp(),
                    (t1, t2) -> timestampCombiner.combine(t1, t2)
            );
        }

        if (!paneInfo.equals(windowedValue.getPane())) throw new RuntimeException("Oops!");

        return true;
    }

    public static SupplierEx<Processor> supplier(PCollectionView<?> view, WindowingStrategy<?, ?> windowingStrategy, String ownerId) {
        return new ViewProcessorSupplier(view, windowingStrategy, ownerId);
    }

    private static class ViewProcessorSupplier implements SupplierEx<Processor> {

        private final SupplierEx<Processor> underlying;

        private ViewProcessorSupplier(PCollectionView<?> view, WindowingStrategy<?, ?> windowingStrategy, String ownerId) {
            this.underlying = () -> new ViewP(view, windowingStrategy, ownerId);
        }

        @Override
        public Processor getEx() throws Exception {
            return underlying.getEx();
        }

    }
}
