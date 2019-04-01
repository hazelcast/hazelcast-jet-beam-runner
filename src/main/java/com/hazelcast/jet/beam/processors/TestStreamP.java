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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

import static com.hazelcast.jet.Traversers.traverseIterable;

public class TestStreamP extends AbstractProcessor {
    private final Traverser traverser;

    @SuppressWarnings("unchecked")
    private TestStreamP(List events) {
        traverser = traverseIterable(events)
                .map(event -> {
                    if (event instanceof SerializableWatermarkEvent) {
                        long ts = ((SerializableWatermarkEvent) event).getTimestamp();
                        if (ts == Long.MAX_VALUE) {
                            // this is an element added by advanceWatermarkToInfinity(), we ignore it, it's always at the end
                            return null;
                        }
                        return new Watermark(ts);
                    } else {
                        assert event instanceof SerializableTimestampedValue;
                        return ((SerializableTimestampedValue) event).asWindowedValue();
                    }
                });
    }

    @Override
    public boolean complete() {
        // todo: TestStream says it should cease emitting, but not stop after the items.
        //   But I don't know how they end the job otherwise...
        return emitFromTraverser(traverser);
    }

    public static ProcessorMetaSupplier supplier(List<Object> events) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(() -> new TestStreamP(events)));
    }

    public static class SerializableWatermarkEvent implements Serializable {
        private final long timestamp;

        public SerializableWatermarkEvent(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    public static class SerializableTimestampedValue<T> implements Serializable {
        private final T value;
        private final Instant timestamp;

        public SerializableTimestampedValue(@Nullable T value, Instant timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        WindowedValue<T> asWindowedValue() {
            return WindowedValue.timestampedValueInGlobalWindow(value, timestamp);
        }
    }
}
