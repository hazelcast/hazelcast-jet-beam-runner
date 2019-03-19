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
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Edge;
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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParDoProcessor<InputT, OutputT> extends AbstractProcessor {

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
    private boolean emissionAttemptedAndFailed;
    private SideInputHandler sideInputHandler;
    private DoFnRunner<InputT, OutputT> doFnRunner;

    private ParDoProcessor(
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
    protected void init(@Nonnull Context context) {
        doFnInvoker = DoFnInvokers.invokerFor(doFn);
        doFnInvoker.invokeSetup();

        SideInputReader sideInputReader = NullSideInputReader.of(sideInputs);
        if (!sideInputs.isEmpty()) {
            sideInputHandler = new SideInputHandler(sideInputs, InMemoryStateInternals.forKey(null));
            sideInputReader = sideInputHandler;
        }

        doFnRunner = DoFnRunners.simpleRunner(
                pipelineOptions.get(),
                doFn,
                sideInputReader,
                new JetOutputManager(),
                mainOutputTag,
                Lists.newArrayList(outputCollToOrdinals.keySet()),
                new JetNoOpStepContext(),
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
    protected boolean tryProcess(int ordinal, @Nonnull Object item) { //todo: this is reprocessing stuff as many times as emission fails...
        if (item instanceof SideInputValue) {
            SideInputValue sideInput = (SideInputValue) item;
            PCollectionView<?> sideInputView = sideInput.getView();
            WindowedValue<Iterable<?>> sideInputValue = sideInput.getWindowedValue();
            sideInputHandler.addSideInputValue(sideInputView, sideInputValue);
            return true;
        } else {
            //noinspection unchecked
            WindowedValue<InputT> windowedValue = (WindowedValue<InputT>) item;

            emissionAttemptedAndFailed = false;
            doFnRunner.startBundle();
            doFnRunner.processElement(windowedValue); //todo: would be good if a bundle would contain more than one element... (see Inbox.drainTo)
            doFnRunner.finishBundle();

            return !emissionAttemptedAndFailed;
        }
    }

    private class JetOutputManager implements DoFnRunners.OutputManager {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            //todo: is being called once for each output PCollections... how do we handle having managed to emit some of them, but not all?
            //todo: test for such problems by setting queue size = 1 on all edges
            int[] ordinals = outputCollToOrdinals.get(tag);
            if (ordinals == null) throw new RuntimeException("Oops!");
            if (ordinals.length > 0) {
                emissionAttemptedAndFailed = !tryEmit(ordinals, output);
            }
        }
    }

    public class JetNoOpStepContext implements StepContext {

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
            return new ParDoProcessor<>(
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
