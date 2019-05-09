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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.nio.Address;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class UnboundedSourceP<T, CMT extends UnboundedSource.CheckpointMark> extends AbstractProcessor implements Traverser<Object> {

    private UnboundedSource.UnboundedReader<T>[] readers;
    private Instant[] watermarks;
    private final List<? extends UnboundedSource<T, CMT>> allShards;
    private final PipelineOptions options;
    private final Coder outputCoder;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove it, very useful for debugging

    private int currentReaderIndex;
    private long lastSentWatermark;

    private UnboundedSourceP(List<? extends UnboundedSource<T, CMT>> allShards, PipelineOptions options, Coder outputCoder, String ownerId) {
        this.allShards = allShards;
        this.options = options;
        this.outputCoder = outputCoder;
        this.ownerId = ownerId;
    }

    @Override
    protected void init(@Nonnull Processor.Context context) {
        List<? extends UnboundedSource<T, CMT>> myShards =
                Utils.roundRobinSubList(allShards, context.globalProcessorIndex(), context.totalParallelism());
        this.readers = createReaders(myShards, options);
        this.watermarks = initWatermarks(myShards.size());
        Arrays.stream(readers).forEach(UnboundedSourceP::startReader);
        currentReaderIndex = 0;
        lastSentWatermark = 0;
    }

    @Override
    public Object next() {
        Instant minWatermark = getMin(watermarks);
        if (minWatermark.isAfter(lastSentWatermark)) {
            lastSentWatermark = minWatermark.getMillis();
            return new Watermark(lastSentWatermark);
        }

        try {
            //trying to fetch a value from the next reader
            for (int i = 0; i < readers.length; i++) {
                currentReaderIndex++;
                if (currentReaderIndex >= readers.length) {
                    currentReaderIndex = 0;
                }
                UnboundedSource.UnboundedReader<T> currentReader = readers[currentReaderIndex];
                if (currentReader.advance()) {
                    Instant currentWatermark = currentReader.getWatermark();
                    watermarks[currentReaderIndex] = currentWatermark; //todo: we should probably do this only on a timer...

                    Object item = currentReader.getCurrent();
                    WindowedValue<Object> res = WindowedValue.timestampedValueInGlobalWindow(item, currentReader.getCurrentTimestamp());
                    return Utils.encodeWindowedValue(res, outputCoder);
                }
            }

            //all advances have failed
            return null;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean complete() {
        if (readers.length == 0) {
            return true;
        }
        emitFromTraverser(this);
        return false;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void close() {
        Arrays.stream(readers).forEach(UnboundedSourceP::stopReader);
        Arrays.fill(readers, null);
    }

    @SuppressWarnings("unchecked")
    private static <T, CMT extends UnboundedSource.CheckpointMark> UnboundedSource.UnboundedReader<T>[] createReaders(
            List<? extends UnboundedSource<T, CMT>> shards, PipelineOptions options) {
        return shards.stream()
                .map(shard -> createReader(options, shard))
                .toArray(UnboundedSource.UnboundedReader[]::new);
    }

    private static Instant[] initWatermarks(int size) {
        Instant[] watermarks = new Instant[size];
        Arrays.fill(watermarks, new Instant(Long.MIN_VALUE));
        return watermarks;
    }

    private static <T> UnboundedSource.UnboundedReader<T> createReader(PipelineOptions options, UnboundedSource<T, ?> shard) {
        try {
            return shard.createReader(options, null);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static void startReader(UnboundedSource.UnboundedReader<?> reader) {
        try {
            reader.start();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static void stopReader(UnboundedSource.UnboundedReader<?> reader) {
        try {
            reader.close();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static Instant getMin(Instant[] instants) {
        Instant min = instants[0];
        for (int i = 1; i < instants.length; i++) {
            if (instants[i].isBefore(min)) {
                min = instants[i];
            }
        }
        return min;
    }

    public static <T, CMT extends UnboundedSource.CheckpointMark> ProcessorMetaSupplier supplier(
            UnboundedSource<T, CMT> unboundedSource,
            SerializablePipelineOptions options,
            Coder outputCoder,
            String ownerId
    ) {
        return new UnboundedSourceMetaProcessorSupplier<>(unboundedSource, options, outputCoder, ownerId);
    }

    private static class UnboundedSourceMetaProcessorSupplier<T, CMT extends UnboundedSource.CheckpointMark> implements ProcessorMetaSupplier {

        private final UnboundedSource<T, CMT> unboundedSource;
        private final SerializablePipelineOptions options;
        private final Coder outputCoder;
        private final String ownerId;

        private List<? extends UnboundedSource<T, CMT>> shards;

        private UnboundedSourceMetaProcessorSupplier(
                UnboundedSource<T, CMT> unboundedSource,
                SerializablePipelineOptions options,
                Coder outputCoder,
                String ownerId
        ) {
            this.unboundedSource = unboundedSource;
            this.options = options;
            this.outputCoder = outputCoder;
            this.ownerId = ownerId;
        }

        @Override
        public void init(@Nonnull ProcessorMetaSupplier.Context context) throws Exception {
            shards = unboundedSource.split(context.totalParallelism(), options.get());
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> ProcessorSupplier.of(() -> new UnboundedSourceP<>(shards, options.get(), outputCoder, ownerId));
        }
    }
}
