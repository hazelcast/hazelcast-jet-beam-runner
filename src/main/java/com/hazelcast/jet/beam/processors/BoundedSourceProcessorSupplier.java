package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.beam.Utils;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.Address;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;

public class BoundedSourceProcessorSupplier<T> implements ProcessorMetaSupplier {

    private final BoundedSource<T> boundedSource;
    private final SerializablePipelineOptions options;

    private transient Context context;
    private transient List<? extends BoundedSource<T>> shards;

    public BoundedSourceProcessorSupplier(BoundedSource<T> boundedSource, SerializablePipelineOptions options) {
        this.boundedSource = boundedSource;
        this.options = options;
    }

    @Override
    public void init(Context context) throws Exception {
        this.context = context;
        long desiredSizeBytes = Math.max(1, boundedSource.getEstimatedSizeBytes(options.get()) / context.totalParallelism());
        shards = boundedSource.split(desiredSizeBytes, options.get());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        return address -> new SourceProcessorSupplier(
                roundRobinSubList(shards, addresses.indexOf(address), addresses.size()), options);
    }

    private static class SourceProcessorSupplier<T> implements ProcessorSupplier {
        private final List<BoundedSource<T>> shards;
        private final SerializablePipelineOptions options;
        private transient Context context;

        SourceProcessorSupplier(List<BoundedSource<T>> shards, SerializablePipelineOptions options) {
            this.shards = shards;
            this.options = options;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            int indexBase = context.memberIndex() * context.localParallelism();
            List<Processor> res = new ArrayList<>(count);
            for (int i = 0; i < count; i++, indexBase++) {
                res.add(new ShardProcessor<>(roundRobinSubList(shards, i, count), options.get()));
            }
            return res;
        }
    }

    private static class ShardProcessor<T> extends AbstractProcessor implements Traverser {

        private final Traverser<BoundedSource<T>> shardsTraverser;
        private final PipelineOptions options;

        private BoundedSource.BoundedReader currentReader;

        ShardProcessor(List<BoundedSource<T>> shards, PipelineOptions options) {
            this.shardsTraverser = traverseIterable(shards);
            this.options = options;
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            nextShard();
        }

        @Override
        public Object next() {
            if (currentReader == null) {
                return null;
            }
            try {
                Object item = currentReader.getCurrent();
                if (item == null) {
                    item = Utils.getNull();
                }
                //todo: this might need to get more flexible
                WindowedValue<Object> res = WindowedValue.timestampedValueInGlobalWindow(item, currentReader.getCurrentTimestamp());
                if (!currentReader.advance()) {
                    nextShard();
                }
                return res;
            } catch (IOException e) {
                throw rethrow(e);
            }
        }

        /**
         * Called when currentReader is null or drained. At the end it will
         * contain a started reader of the next shard or null.
         */
        private void nextShard() throws IOException {
            for (;;) {
                if (currentReader != null) {
                    currentReader.close();
                    currentReader = null;
                }
                BoundedSource<T> shard = shardsTraverser.next();
                if (shard == null) {
                    break; // all shards done
                }
                currentReader = shard.createReader(options);
                if (currentReader.start()) {
                    break;
                }
            }
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(this);
        }

        @Override
        public void close() throws Exception {
            if (currentReader != null) {
                currentReader.close();
            }
        }
    }

    /**
     * Assigns the {@code list} to {@code count} sublists in a round-robin
     * fashion. One call returns the {@code index}-th sublist.
     *
     * <p>For example, for a 7-element list where {@code count == 3}, it would
     * respectively return for indices 0..2:
     * <pre>
     *   0, 3, 6
     *   1, 4
     *   2, 5
     * </pre>
     */
    private static <E> List<E> roundRobinSubList(List<E> list, int index, int count) {
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException("index=" + index + ", count=" + count);
        }
        return IntStream.range(0, list.size())
                        .filter(i -> i % count == index)
                        .mapToObj(list::get)
                        .collect(toList());
    }
}
