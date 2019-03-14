package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.beam.Utils;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.nio.Address;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class BoundedSourceProcessorSupplier implements ProcessorMetaSupplier {

    private final BoundedSource boundedSource;
    private final SerializablePipelineOptions options;

    private transient int totalParallelism;
    private transient int localParallelism;

    public BoundedSourceProcessorSupplier(BoundedSource boundedSource, SerializablePipelineOptions options) {
        this.boundedSource = boundedSource;
        this.options = options;
    }

    @Override
    public void init(Context context) {
        totalParallelism = context.totalParallelism();
        localParallelism = context.localParallelism();
    }

    @Override
    public void close(Throwable error) throws Exception {
        //todo: close all opened readers
    }

    @SuppressWarnings("unchecked")
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        try {
            long desiredSizeBytes = boundedSource.getEstimatedSizeBytes(options.get()) / (totalParallelism - 1);
            List<? extends BoundedSource> shards = boundedSource.split(desiredSizeBytes, options.get());
            int numShards = shards.size();
            if (numShards != totalParallelism) {
                //todo: remove//todo: handle this, distribute the leftover shards among processors
            }

            Map<Address, ProcessorSupplier> map = new HashMap<>();
            for (int i = 0; i < addresses.size(); i++) {
                int globalIndexBase = localParallelism * i;
                ProcessorSupplier supplier = processorCount ->
                        range(globalIndexBase, globalIndexBase + processorCount)
                                .mapToObj(
                                        globalIndex -> {
                                            if (globalIndex >= shards.size()) return new NoopP();

                                            BoundedSource shard = shards.get(globalIndex);
                                            return new ShardProcessor(shard, options.get());
                                        }
                                )
                                .collect(toList());
                map.put(addresses.get(i), supplier);
            }
            return map::get;
        } catch (Exception e) {
            //todo: have to close all opened readers
            throw new RuntimeException(e); //todo: what to do with the error?
        }
    }

    private static class ShardProcessor extends AbstractProcessor implements Traverser {

        private BoundedSource.BoundedReader reader;
        private boolean available;

        ShardProcessor(BoundedSource source, PipelineOptions options) {
            try {
                this.reader = source.createReader(options);
                this.available = reader.start();
            } catch (IOException e) {
                e.printStackTrace();
                this.available = false; //todo: is this ok/enough?
            }
        }

        @Override
        public Object next() {
            try {
                if (available) {
                    Object item = reader.getCurrent();
                    if (item == null) {
                        item = Utils.getNull();
                    }
                    available = reader.advance();
                    return WindowedValue.timestampedValueInGlobalWindow(item, reader.getCurrentTimestamp()); //todo: this might need to get more flexible
                } else { //todo: is it ok to never check availability again?
                    return null;
                }
            } catch (IOException e) {
                e.printStackTrace();
                //todo: close the reader and make sure the traverser will know to emit null
                return null;
            }
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(this);
        }
    }

    private static class NoopP implements Processor { //todo: is in Processors, but it's private
        @Override
        public void process(int ordinal, Inbox inbox) {
            inbox.drain(ConsumerEx.noop());
        }

        @Override
        public void restoreFromSnapshot(Inbox inbox) {
            inbox.drain(ConsumerEx.noop());
        }
    }

}
