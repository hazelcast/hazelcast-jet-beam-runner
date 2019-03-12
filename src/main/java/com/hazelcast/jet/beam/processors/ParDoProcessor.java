package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.Processor;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParDoProcessor<InputT, OutputT> extends AbstractParDoProcessor<InputT, OutputT> {

    ParDoProcessor(
            DoFn<InputT, OutputT> doFn,
            WindowingStrategy<?, ?> windowingStrategy,
            Map<TupleTag<?>, int[]> outputCollToOrdinals,
            SerializablePipelineOptions pipelineOptions,
            TupleTag<OutputT> mainOutputTag,
            Coder<InputT> inputCoder,
            Map<TupleTag<?>, Coder<?>> outputCoderMap,
            List<PCollectionView<?>> sideInputs
    ) {
        super(
                doFn,
                windowingStrategy,
                outputCollToOrdinals,
                pipelineOptions,
                mainOutputTag,
                inputCoder,
                outputCoderMap,
                sideInputs
        );
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) { //todo: this is reprocessing stuff as many times as emission fails...
        if (item instanceof SideInputValue) {
            SideInputValue sideInput = (SideInputValue) item;
            sideInputHandler.addSideInputValue(sideInput.getView(), sideInput.getWindowedValue());
            return true;
        } else {
            //noinspection unchecked
            WindowedValue<InputT> windowedValue = (WindowedValue<InputT>) item;
            if (windowedValue.getValue() == null) return true;

            emissionAttemptedAndFailed = false;
            doFnRunner.startBundle();
            doFnRunner.processElement(windowedValue); //todo: would be good if a bundle would contain more than one element... (see Inbox.drainTo)
            doFnRunner.finishBundle();

            return !emissionAttemptedAndFailed;
        }
    }

    public static class Supplier<InputT, OutputT> extends AbstractSupplier<InputT, OutputT> {

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
            super(ownerId, doFn, windowingStrategy, pipelineOptions, mainOutputTag, allOutputTags, inputCoder, outputCoderMap, sideInputs);
        }

        @Override
        protected Processor newProcessorInstance(
                String ownerId,
                DoFn<InputT, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Map<TupleTag<?>, int[]> outputCollToOrdinals,
                Coder<InputT> inputCoder,
                Map<TupleTag<?>, Coder<?>> outputCoderMap,
                List<PCollectionView<?>> sideInputs
        ) {
            return new ParDoProcessor<>(
                    doFn,
                    windowingStrategy,
                    outputCollToOrdinals,
                    pipelineOptions,
                    mainOutputTag,
                    inputCoder,
                    outputCoderMap,
                    sideInputs
            );
        }
    }
}