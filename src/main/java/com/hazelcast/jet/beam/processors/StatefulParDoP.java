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

import com.hazelcast.jet.core.Processor;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StatefulParDoP<OutputT> extends AbstractParDoP<KV<?, ?>, OutputT> {

    private KeyedStepContext keyedStepContext;
    private InMemoryTimerInternals timerInternals;

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
    protected DoFnRunner<KV<?, ?>, OutputT> getDoFnRunner(PipelineOptions pipelineOptions, DoFn<KV<?, ?>, OutputT> doFn, SideInputReader sideInputReader, JetOutputManager outputManager, TupleTag<OutputT> mainOutputTag, List<TupleTag<?>> additionalOutputTags, Coder<KV<?, ?>> inputCoder, Map<TupleTag<?>, Coder<?>> outputCoderMap, WindowingStrategy<?, ?> windowingStrategy) {
        timerInternals = new InMemoryTimerInternals();
        keyedStepContext = new KeyedStepContext(timerInternals);
        return DoFnRunners.simpleRunner(
                pipelineOptions,
                doFn,
                sideInputReader,
                outputManager,
                mainOutputTag,
                additionalOutputTags,
                keyedStepContext,
                inputCoder,
                outputCoderMap,
                windowingStrategy
        );
        //System.out.println(ParDoP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
        //if (ownerId.startsWith("8 ")) System.out.println(ParDoP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
    }

    @Override
    protected void startRunnerBundle(DoFnRunner<KV<?, ?>, OutputT> runner) {
        try {
            Instant now = Instant.now();
            timerInternals.advanceProcessingTime(now);
            timerInternals.advanceSynchronizedProcessingTime(now);
        } catch (Exception e) {
            throw new RuntimeException("Failed advancing time!");
        }

        super.startRunnerBundle(runner);
    }

    @Override
    protected void processElementWithRunner(DoFnRunner<KV<?, ?>, OutputT> runner, WindowedValue<KV<?, ?>> windowedValue) {
        KV<?, ?> kv = windowedValue.getValue();
        Object key = kv.getKey();
        keyedStepContext.setKey(key);

        super.processElementWithRunner(runner, windowedValue);
    }

    @Override
    public boolean complete() {
        try {
            timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

            timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
            timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

            fireEligibleTimers(timerInternals);
        } catch (Exception e) {
            throw new RuntimeException("Failed advancing processing time!");
        }

        return super.complete();
    }

    private void fireEligibleTimers(InMemoryTimerInternals timerInternals) {
        while (true) {
            TimerInternals.TimerData timer;
            boolean hasFired = false;

            while ((timer = timerInternals.removeNextEventTimer()) != null) {
                hasFired = true;
                fireTimer(timer, doFnRunner);
            }

            while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
                hasFired = true;
                fireTimer(timer, doFnRunner);
            }

            while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
                hasFired = true;
                fireTimer(timer, doFnRunner);
            }

            if (!hasFired) break;
        }
    }

    private static void fireTimer(TimerInternals.TimerData timer, DoFnRunner<KV<?, ?>, ?> doFnRunner) {
        StateNamespace namespace = timer.getNamespace();
        BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
        doFnRunner.onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
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

        private final Map<Object, InMemoryStateInternals> stateInternalsOfKeys;
        private final InMemoryTimerInternals timerInternals;

        private InMemoryStateInternals currentStateInternals;

        KeyedStepContext(InMemoryTimerInternals timerInternals) {
            this.stateInternalsOfKeys = new HashMap<>();
            this.timerInternals = timerInternals;
        }

        void setKey(Object key) {
            currentStateInternals = stateInternalsOfKeys.computeIfAbsent(key, InMemoryStateInternals::forKey);
        }

        @Override
        public StateInternals stateInternals() {
            return currentStateInternals;
        }

        @Override
        public TimerInternals timerInternals() {
            return timerInternals;
        }
    }
}
