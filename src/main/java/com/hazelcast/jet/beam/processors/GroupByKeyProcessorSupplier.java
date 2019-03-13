package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupByKeyProcessorSupplier<K, InputT> implements DistributedSupplier<Processor> {

    private final DistributedSupplier<Processor> underlying;

    public GroupByKeyProcessorSupplier(TimestampCombiner timestampCombiner) {
        this.underlying = Processors.aggregateByKeyP(
                Collections.singletonList(new KeyExtractorFunction<>()),
                AggregateOperations.toList(),
                new WindowedValueMerger<>(timestampCombiner)
        );
    }

    @Override
    public Processor getEx() throws Exception {
        return underlying.getEx();
    }

    private static class KeyExtractorFunction<K, InputT> implements DistributedFunction<WindowedValue<KV<K, InputT>>, K> {
        @Override
        public K applyEx(WindowedValue<KV<K, InputT>> kvWindowedValue) throws Exception {
            return kvWindowedValue.getValue().getKey();
        }
    }

    private static class WindowedValueMerger<K, InputT> implements DistributedBiFunction<K, List<WindowedValue<KV<K, InputT>>>, WindowedValue<KV<K, Iterable<InputT>>>> {

        private final TimestampCombiner timestampCombiner;

        WindowedValueMerger(TimestampCombiner timestampCombiner) {
            this.timestampCombiner = timestampCombiner;
        }

        @Override
        public WindowedValue<KV<K, Iterable<InputT>>> applyEx(K k, List<WindowedValue<KV<K, InputT>>> windowedValues) throws Exception {
            //todo: can windowedValues be empty?
            List<Instant> instants = new ArrayList<>(); //todo: garbage!
            BoundedWindow window = null;
            PaneInfo pane = PaneInfo.NO_FIRING; //todo: is this the right default?
            List<InputT> values = new ArrayList<>();
            for (WindowedValue<KV<K, InputT>> windowedValue : windowedValues) {
                if (window == null) window = windowedValue.getWindows().iterator().next();
                if (pane == null) pane = windowedValue.getPane();
                instants.add(windowedValue.getTimestamp());
                values.add(windowedValue.getValue().getValue());
            }
            //todo: not sure if it's ok to just pick a random value for window/pane/instant...
            return WindowedValue.of(KV.of(k, values), timestampCombiner.combine(instants), window, pane);
        }

        private static Instant getLatest(Instant oldOne, Instant newOne) {
            if (oldOne == null) return newOne;
            return oldOne.compareTo(newOne) > 0 ? oldOne : newOne;
        }

    }
}
