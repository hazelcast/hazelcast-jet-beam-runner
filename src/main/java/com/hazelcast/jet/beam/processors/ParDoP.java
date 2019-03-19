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

import com.hazelcast.jet.beam.DAGBuilder;
import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParDoP<InputT, OutputT> implements Processor {

    private final SerializablePipelineOptions pipelineOptions;
    private final DoFn<InputT, OutputT> doFn;
    private final WindowingStrategy<?, ?> windowingStrategy;
    private final Map<TupleTag<?>, int[]> outputCollToOrdinals;
    private final TupleTag<OutputT> mainOutputTag;
    private final Coder<InputT> inputCoder;
    private final Map<TupleTag<?>, Coder<?>> outputCoderMap;
    private final List<PCollectionView<?>> sideInputs;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String ownerId; //do not remove, useful for debugging

    private DoFnInvoker<InputT, OutputT> doFnInvoker;
    private SideInputHandler sideInputHandler;
    private DoFnRunner<InputT, OutputT> doFnRunner;
    private JetOutputManager outputManager;

    private ParDoP(
            DoFn<InputT, OutputT> doFn,
            WindowingStrategy<?, ?> windowingStrategy,
            Map<TupleTag<?>, int[]> outputCollToOrdinals,
            SerializablePipelineOptions pipelineOptions,
            TupleTag<OutputT> mainOutputTag,
            Coder<InputT> inputCoder,
            Map<TupleTag<?>, Coder<?>> outputCoderMap,
            List<PCollectionView<?>> sideInputs,
            String ownerId
    ) {
        this.pipelineOptions = pipelineOptions;
        this.doFn = doFn;
        this.windowingStrategy = windowingStrategy;
        this.outputCollToOrdinals = outputCollToOrdinals;
        this.mainOutputTag = mainOutputTag;
        this.inputCoder = inputCoder;
        this.outputCoderMap = outputCoderMap;
        this.sideInputs = sideInputs;
        this.ownerId = ownerId;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        doFnInvoker = DoFnInvokers.invokerFor(doFn);
        doFnInvoker.invokeSetup();

        SideInputReader sideInputReader;
        if (!sideInputs.isEmpty()) {
            sideInputHandler = new SideInputHandler(sideInputs, InMemoryStateInternals.forKey(null));
            sideInputReader = sideInputHandler;
        } else {
            sideInputReader = NullSideInputReader.of(sideInputs);
        }

        outputManager = new JetOutputManager(outbox, outputCollToOrdinals);
        doFnRunner = DoFnRunners.simpleRunner(
                pipelineOptions.get(),
                doFn,
                sideInputReader,
                outputManager,
                mainOutputTag,
                Lists.newArrayList(outputCollToOrdinals.keySet()),
                new NotImplementedStepContext(),
                inputCoder,
                outputCoderMap,
                windowingStrategy
        );
    }

    @Override
    public void close() {
        doFnInvoker.invokeTeardown();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!outputManager.tryFlush()) {
            // don't process more items until outputManager is empty
            return;
        }
        doFnRunner.startBundle();
        for (Object item; (item = inbox.peek()) != null; ) {
            if (item instanceof SideInputValue) {
                SideInputValue sideInput = (SideInputValue) item;
                PCollectionView<?> sideInputView = sideInput.getView();
                WindowedValue<Iterable<?>> sideInputValue = sideInput.getWindowedValue();
                sideInputHandler.addSideInputValue(sideInputView, sideInputValue);
            } else {
                //noinspection unchecked
                WindowedValue<InputT> windowedValue = (WindowedValue<InputT>) item;
                doFnRunner.processElement(windowedValue);
            }
            inbox.remove();
            if (!outputManager.tryFlush()) {
                break;
            }
        }
        doFnRunner.finishBundle();
        // finishBundle can also add items to outputManager, they will be flushed in tryProcess() or complete()
    }

    @Override
    public boolean tryProcess() {
        return outputManager.tryFlush();
    }

    @Override
    public boolean complete() {
        return outputManager.tryFlush();
    }

    /**
     * An output manager that stores the output in an ArrayList, one for each
     * output ordinal, and a way to drain to outbox ({@link #tryFlush()}).
     */
    private static class JetOutputManager implements DoFnRunners.OutputManager {
        private final Outbox outbox;
        private final Map<TupleTag<?>, int[]> outputCollToOrdinals;
        private final List<Object>[] outputBuckets;

        // the flush position to continue flushing to outbox
        private int currentBucket, currentItem;

        @SuppressWarnings("unchecked")
        JetOutputManager(Outbox outbox, Map<TupleTag<?>, int[]> outputCollToOrdinals) {
            this.outbox = outbox;
            this.outputCollToOrdinals = outputCollToOrdinals;
            assert !outputCollToOrdinals.isEmpty();
            int maxOrdinal = outputCollToOrdinals.values().stream().flatMapToInt(IntStream::of).max().orElse(-1);
            outputBuckets = new List[maxOrdinal + 1];
            Arrays.setAll(outputBuckets, i -> new ArrayList<>());
        }

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            assert currentBucket == 0 && currentItem == 0 : "adding output while flushing";
            for (int ordinal : outputCollToOrdinals.get(tag)) {
                outputBuckets[ordinal].add(output);
            }
        }

        @CheckReturnValue
        boolean tryFlush() {
            for (; currentBucket < outputBuckets.length; currentBucket++) {
                List<Object> bucket = outputBuckets[currentBucket];
                for (; currentItem < bucket.size(); currentItem++) {
                    if (!outbox.offer(currentBucket, bucket.get(currentItem))) {
                        return false;
                    }
                }
                bucket.clear();
                currentItem = 0;
            }
            currentBucket = 0;
            return true;
        }
    }

    public class NotImplementedStepContext implements StepContext {

        //not really needed until we start implementing state & timers

        @Override
        public StateInternals stateInternals() {
            throw new UnsupportedOperationException("stateInternals is not supported");
        }

        @Override
        public TimerInternals timerInternals() {
            throw new UnsupportedOperationException("timerInternals is not supported");
        }
    }

    public static class Supplier<InputT, OutputT>  implements SupplierEx<Processor>, DAGBuilder.WiringListener {

        private final String ownerId;

        private final SerializablePipelineOptions pipelineOptions;
        private final DoFn<InputT, OutputT> doFn;
        private final WindowingStrategy<?, ?> windowingStrategy;
        private final TupleTag<OutputT> mainOutputTag;
        private final Map<TupleTag<?>, List<Integer>> outputCollToOrdinals;
        private final Coder<InputT> inputCoder;
        private final Map<TupleTag<?>, Coder<?>> outputCoderMap;
        private final List<PCollectionView<?>> sideInputs;

        public Supplier(
                String ownerId,
                DoFn<InputT, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Set<TupleTag<?>> allOutputTags,
                Coder<InputT> inputCoder,
                Map<TupleTag<?>, Coder<?>> outputCoderMap,
                List<PCollectionView<?>> sideInputs
        ) {
            this.ownerId = ownerId;
            this.pipelineOptions = pipelineOptions;
            this.doFn = doFn;
            this.windowingStrategy = windowingStrategy;
            this.outputCollToOrdinals = allOutputTags.stream().collect(Collectors.toMap(Function.identity(), t -> new ArrayList<>()));
            this.mainOutputTag = mainOutputTag;
            this.inputCoder = inputCoder;
            this.outputCoderMap = outputCoderMap;
            this.sideInputs = sideInputs;
        }

        @Override
        public Processor getEx() {
            return new ParDoP<>(
                    doFn,
                    windowingStrategy,
                    outputCollToOrdinals.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().mapToInt(i -> i).toArray())),
                    pipelineOptions,
                    mainOutputTag,
                    inputCoder,
                    outputCoderMap,
                    sideInputs,
                    ownerId
            );
        }

        @Override
        public void isOutboundEdgeOfVertex(Edge edge, TupleTag pCollId, String vertexId) {
            if (ownerId.equals(vertexId)) {
                List<Integer> ordinals = outputCollToOrdinals.get(pCollId);
                if (ordinals == null) throw new RuntimeException("Oops"); //todo

                ordinals.add(edge.getSourceOrdinal());
            }
        }

        @Override
        public void isInboundEdgeOfVertex(Edge edge, TupleTag pCollId, String vertexId) {
            //do nothing
        }
    }
}
