package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.Processor;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
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

public class ParDoPushbackProcessor<InputT, OutputT> extends AbstractParDoProcessor<InputT, OutputT> {

    private transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackDoFnRunner;

    ParDoPushbackProcessor(
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
        super(
                doFn,
                windowingStrategy,
                outputCollToOrdinals,
                pipelineOptions,
                mainOutputTag,
                inputCoder,
                outputCoderMap,
                sideInputs,
                ownerId
        );
    }

    @Override
    protected void init(Context context) throws Exception {
        pushbackDoFnRunner = SimplePushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) throws Exception { //todo: this is reprocessing stuff as many times as emission fails...
        if (item instanceof SideInputValue) {
            SideInputValue sideInput = (SideInputValue) item;
            sideInputHandler.addSideInputValue(sideInput.getView(), sideInput.getWindowedValue());
            return true;
        } else {
            //noinspection unchecked
            WindowedValue<InputT> windowedValue = (WindowedValue<InputT>) item;
            if (windowedValue.getValue() == null) return true;

            pushbackDoFnRunner.startBundle();
            emissionAttemptedAndFailed = false;

            Iterable<WindowedValue<InputT>> pushedBack = pushbackDoFnRunner.processElementInReadyWindows(windowedValue); //todo: would be good if a bundle would contain more than one element... (see Inbox.drainTo)
            pushbackDoFnRunner.finishBundle();

            if (pushedBack.iterator().hasNext()) {
                return false; //todo: will not be this simple once we'll have bundles larges than one
            }

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
            return new ParDoPushbackProcessor<>(
                    doFn,
                    windowingStrategy,
                    outputCollToOrdinals,
                    pipelineOptions,
                    mainOutputTag,
                    inputCoder,
                    outputCoderMap,
                    sideInputs,
                    ownerId
            );
        }
    }
}
