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

import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StatefulParDoP<OutputT> extends AbstractParDoP<KV<?, ?>, OutputT> {

    private final Map<Object, DoFnRunner<KV<?, ?>, OutputT>> runners = new HashMap<>();

    private Object lastKey;
    private DoFnRunner<KV<?, ?>, OutputT> lastRunner;

    private StatefulParDoP(
            DoFn<KV<?, ?>, OutputT> doFn,
            WindowingStrategy<?, ?> windowingStrategy,
            Map<TupleTag<?>, int[]> outputCollToOrdinals,
            SerializablePipelineOptions pipelineOptions,
            TupleTag<OutputT> mainOutputTag,
            Coder<KV<?, ?>> inputCoder,
            Map<TupleTag<?>, Coder<?>> outputCoderMap,
            Map<Integer, PCollectionView<?>> ordinalToSideInput,
            String ownerId
    ) {
        super(
                doFn,
                windowingStrategy,
                outputCollToOrdinals,
                pipelineOptions,
                mainOutputTag,
                inputCoder,
                outputCoderMap,
                ordinalToSideInput,
                ownerId
        );
    }

    @Override
    protected void processNonBufferedRegularItems(Inbox inbox) {
        //System.out.println(StatefulParDoP.class.getSimpleName() + " UPDATE ownerId = " + ownerId); //useful for debugging

        for (WindowedValue<KV<?, ?>> windowedValue; (windowedValue = (WindowedValue<KV<?, ?>>) inbox.poll()) != null; ) {
            //System.out.println(StatefulParDoP.class.getSimpleName() + " UPDATE ownerId = " + ownerId + ", windowedValue = " + windowedValue); //useful for debugging
            KV<?, ?> kv = windowedValue.getValue();

            Object currentKey = kv.getKey();
            if (lastKey == null) { //first item from the inbox in this round of processing
                lastKey = currentKey;
                lastRunner = getRunner(currentKey);
                lastRunner.startBundle();
            } else if (!lastKey.equals(currentKey)) { //not first item from inbox in this round of processing, key changes
                lastRunner.finishBundle();
                lastKey = currentKey;
                lastRunner = getRunner(currentKey);
                lastRunner.startBundle();
            }

            lastRunner.processElement(windowedValue);
            if (!outputManager.tryFlush()) {
                break;
            }
        }
        if (lastRunner != null) lastRunner.finishBundle();

        lastKey = null;
        lastRunner = null;

        //todo: this approach to batching might not be the most efficient one... can we extract all items with same key from Inbox?
    }

    private DoFnRunner<KV<?, ?>, OutputT> getRunner(Object key) {
        return runners.computeIfAbsent(
                key,
                k -> DoFnRunners.simpleRunner(
                        pipelineOptions.get(),
                        doFn,
                        sideInputReader,
                        outputManager,
                        mainOutputTag,
                        Lists.newArrayList(outputCollToOrdinals.keySet()),
                        new KeyedStepContext(key),
                        inputCoder,
                        outputCoderMap,
                        windowingStrategy
                )
        );
    }

    public static class Supplier<OutputT> extends AbstractSupplier<KV<?, ?>, OutputT> {

        public Supplier(
                String ownerId,
                DoFn<KV<?, ?>, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Set<TupleTag<OutputT>> allOutputTags,
                Coder<KV<?, ?>> inputCoder,
                Map<TupleTag<?>, Coder<?>> outputCoderMap,
                List<PCollectionView<?>> sideInputs
        ) {
            super(
                    ownerId,
                    doFn,
                    windowingStrategy,
                    pipelineOptions,
                    mainOutputTag,
                    allOutputTags,
                    inputCoder,
                    outputCoderMap,
                    sideInputs
            );
        }

        @Override
        Processor getEx(
                DoFn<KV<?, ?>, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                Map<TupleTag<?>, int[]> outputCollToOrdinals,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Coder<KV<?, ?>> inputCoder,
                Map<TupleTag<?>, Coder<?>> outputCoderMap,
                Map<Integer, PCollectionView<?>> ordinalToSideInput,
                String ownerId
        ) {
            return new StatefulParDoP<>(
                    doFn,
                    windowingStrategy,
                    outputCollToOrdinals,
                    pipelineOptions,
                    mainOutputTag,
                    inputCoder,
                    outputCoderMap,
                    ordinalToSideInput,
                    ownerId
            );
        }
    }

    private static class KeyedStepContext implements StepContext {

        private final Object key;
        private final StateInternals stateInternals;

        KeyedStepContext(Object key) {
            this.key = key;
            stateInternals = InMemoryStateInternals.forKey(key);
        }

        @Override
        public StateInternals stateInternals() {
            return stateInternals;
        }

        @Override
        public TimerInternals timerInternals() {
            throw new UnsupportedOperationException("timerInternals is not supported");
        }
    }
}
