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
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.Traversers.traverseStream;

/**
 * Collects all input {@link WindowedValue}s, groups them by windows and when
 * input is complete emits them.
 */
public class ViewP extends AbstractProcessor {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final TimestampCombiner timestampCombiner;
    private final Coder inputCoder;
    private final Coder outputCoder;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove, useful for debugging

    private Map<BoundedWindow, TimestampAndValues> values = new HashMap<>();
    private Traverser<byte[]> resultTraverser;

    private ViewP(
            Coder inputCoder,
            Coder outputCoder,
            WindowingStrategy windowingStrategy,
            String ownerId
    ) {
        this.timestampCombiner = windowingStrategy.getTimestampCombiner();
        this.inputCoder = inputCoder;
        this.outputCoder = Utils.deriveIterableValueCoder((WindowedValue.FullWindowedValueCoder) outputCoder);
        this.ownerId = ownerId;
        //System.out.println(ViewP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        //System.out.println(ViewP.class.getSimpleName() + " UPDATE ownerId = " + ownerId + ", item = " + item); //useful for debugging
        WindowedValue<?> windowedValue = Utils.decodeWindowedValue((byte[]) item, inputCoder);
        for (BoundedWindow window : windowedValue.getWindows()) {
            values
                    .merge(window,
                            new TimestampAndValues(windowedValue.getPane(), windowedValue.getTimestamp(), windowedValue.getValue()),
                            (o, n) -> o.merge(timestampCombiner, n));
        }

        return true;
    }

    @Override
    public boolean complete() {
        //System.out.println(ViewP.class.getSimpleName() + " COMPLETE ownerId = " + ownerId); //useful for debugging
        if (resultTraverser == null) {
            resultTraverser = traverseStream(
                    values.entrySet().stream().map(
                            e -> {
                                WindowedValue<?> outputValue = WindowedValue.of(
                                        e.getValue().values,
                                        e.getValue().timestamp,
                                        Collections.singleton(e.getKey()),
                                        e.getValue().pane
                                );
                                return Utils.encodeWindowedValue(outputValue, outputCoder, baos);
                            }
                    )
            );
        }
        return emitFromTraverser(resultTraverser);
    }

    public static SupplierEx<Processor> supplier(
            Coder inputCoder,
            Coder outputCoder,
            WindowingStrategy<?, ?> windowingStrategy,
            String ownerId
    ) {
        return () -> new ViewP(inputCoder, outputCoder, windowingStrategy, ownerId);
    }

    public static class TimestampAndValues {
        private final List<Object> values = new ArrayList<>();
        private Instant timestamp;
        private PaneInfo pane;

        TimestampAndValues(PaneInfo pane, Instant timestamp, Object value) {
            this.pane = pane;
            this.timestamp = timestamp;
            this.values.add(value);
        }

        public Iterable<Object> getValues() {
            return values;
        }

        TimestampAndValues merge(TimestampCombiner timestampCombiner, TimestampAndValues other) {
            pane = other.pane;
            timestamp = timestampCombiner.combine(timestamp, other.timestamp);
            values.addAll(other.values);
            return this;
        }
    }
}
