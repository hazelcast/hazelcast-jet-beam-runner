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
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParDoP<InputT, OutputT> extends AbstractParDoP<InputT, OutputT> { //todo: unify with StatefulParDoP?

    private ParDoP(
            DoFn<InputT, OutputT> doFn,
            WindowingStrategy<?, ?> windowingStrategy,
            Map<TupleTag<?>, int[]> outputCollToOrdinals,
            SerializablePipelineOptions pipelineOptions,
            TupleTag<OutputT> mainOutputTag,
            Coder<InputT> inputCoder,
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
    protected DoFnRunner<InputT, OutputT> getDoFnRunner(PipelineOptions pipelineOptions, DoFn<InputT, OutputT> doFn, SideInputReader sideInputReader, JetOutputManager outputManager, TupleTag<OutputT> mainOutputTag, List<TupleTag<?>> additionalOutputTags, Coder<InputT> inputCoder, Map<TupleTag<?>, Coder<?>> outputCoderMap, WindowingStrategy<?, ?> windowingStrategy) {
        return DoFnRunners.simpleRunner(
                pipelineOptions,
                doFn,
                sideInputReader,
                outputManager,
                mainOutputTag,
                additionalOutputTags,
                new NotImplementedStepContext(),
                inputCoder,
                outputCoderMap,
                windowingStrategy
        );
        //System.out.println(ParDoP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
        //if (ownerId.startsWith("8 ")) System.out.println(ParDoP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
    }

    public static class Supplier<InputT, OutputT> extends AbstractSupplier<InputT, OutputT> {

        public Supplier(
                String ownerId,
                DoFn<InputT, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Set<TupleTag<OutputT>> allOutputTags,
                Coder<InputT> inputCoder,
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
                DoFn<InputT, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                Map<TupleTag<?>, int[]> outputCollToOrdinals,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Coder<InputT> inputCoder,
                Map<TupleTag<?>, Coder<?>> outputCoderMap,
                Map<Integer, PCollectionView<?>> ordinalToSideInput,
                String ownerId
        ) {
            return new ParDoP<>(
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

    private static class NotImplementedStepContext implements StepContext {

        //not needed when not handling state & timers

        @Override
        public StateInternals stateInternals() {
            throw new UnsupportedOperationException("stateInternals is not supported");
        }

        @Override
        public TimerInternals timerInternals() {
            throw new UnsupportedOperationException("timerInternals is not supported");
        }
    }
}
