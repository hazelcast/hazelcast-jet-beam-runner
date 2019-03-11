package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.beam.DAGBuilder;
import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedSupplier;
import org.apache.beam.runners.core.*;
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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ParDoPushbackProcessor<InputT, OutputT> extends AbstractProcessor implements Serializable {

    private final SerializablePipelineOptions pipelineOptions;
    private final DoFn<InputT, OutputT> doFn;
    private final WindowingStrategy<?, ?> windowingStrategy;
    private final Map<TupleTag<?>, Integer> outputMap;
    private final TupleTag<OutputT> mainOutputTag;
    private final Coder<InputT> inputCoder;
    private final Map<TupleTag<?>, Coder<?>> outputCoderMap;
    private final List<PCollectionView<?>> sideInputs;
    private final boolean needsToEmit;

    private transient DoFnInvoker<InputT, OutputT> doFnInvoker;
    private transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackDoFnRunner;
    private transient boolean emissionAttemptedAndFailed;
    private transient SideInputHandler sideInputHandler;

    ParDoPushbackProcessor(
            DoFn<InputT, OutputT> doFn,
            WindowingStrategy<?, ?> windowingStrategy,
            Map<TupleTag<?>, Integer> outputMap,
            SerializablePipelineOptions pipelineOptions,
            TupleTag<OutputT> mainOutputTag,
            Coder<InputT> inputCoder,
            Map<TupleTag<?>, Coder<?>> outputCoderMap,
            List<PCollectionView<?>> sideInputs,
            boolean needsToEmit
    ) {
        this.pipelineOptions = pipelineOptions;
        this.doFn = doFn;
        this.windowingStrategy = windowingStrategy;
        this.outputMap = outputMap;
        this.mainOutputTag = mainOutputTag;
        this.inputCoder = inputCoder;
        this.outputCoderMap = outputCoderMap;
        this.sideInputs = sideInputs;
        this.needsToEmit = needsToEmit;
    }

    @Override
    protected void init(Context context) throws Exception {
        super.init(context);

        doFnInvoker = DoFnInvokers.invokerFor(doFn);
        doFnInvoker.invokeSetup();

        SideInputReader sideInputReader = NullSideInputReader.of(sideInputs);
        if (!sideInputs.isEmpty()) {
            sideInputHandler = new SideInputHandler(sideInputs, InMemoryStateInternals.forKey(null));
            sideInputReader = sideInputHandler;
        }

        DoFnRunner<InputT, OutputT> doFnRunner = DoFnRunners.simpleRunner(
                pipelineOptions.get(),
                doFn,
                sideInputReader,
                new JetOutputManager(),
                mainOutputTag,
                Lists.newArrayList(outputMap.keySet()),
                new JetNoOpStepContext(),
                inputCoder,
                outputCoderMap,
                windowingStrategy
        );
        pushbackDoFnRunner = SimplePushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);
    }

    @Override
    public void close() throws Exception {
        doFnInvoker.invokeTeardown();
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) throws Exception { //todo: this is reprocessing stuff as many times as emission fails...
        //System.out.println(doFn + ": item = " + item); //todo: remove
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

    private class JetOutputManager implements DoFnRunners.OutputManager {
        @Override
        @SuppressWarnings("unchecked")
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            //todo: this can be called multiple times for one input? if yes, how do we handle having emited some of the output, but not all?
            if (needsToEmit) {
                emissionAttemptedAndFailed = !tryEmit(output);
            }
        }
    }

    private class JetNoOpStepContext implements StepContext { //todo: wtf is this about?

        @Override
        public StateInternals stateInternals() {
            throw new UnsupportedOperationException("stateInternals is not supported");
        }

        @Override
        public TimerInternals timerInternals() {
            throw new UnsupportedOperationException("timerInternals is not supported");
        }
    }

    public static class Supplier<InputT, OutputT> implements DistributedSupplier<Processor>, DAGBuilder.WiringListener {
        private final String ownerId;

        private final SerializablePipelineOptions pipelineOptions;
        private final DoFn<InputT, OutputT> doFn;
        private final WindowingStrategy<?, ?> windowingStrategy;
        private final Map<TupleTag<?>, Integer> outputMap;
        private final TupleTag<OutputT> mainOutputTag;
        private final Coder<InputT> inputCoder;
        private final Map<TupleTag<?>, Coder<?>> outputCoderMap;
        private final List<PCollectionView<?>> sideInputs;

        private boolean needsToEmit = false;

        public Supplier(
                String ownerId,
                DoFn<InputT, OutputT> doFn,
                WindowingStrategy<?, ?> windowingStrategy,
                Map<TupleTag<?>, Integer> outputMap,
                SerializablePipelineOptions pipelineOptions,
                TupleTag<OutputT> mainOutputTag,
                Coder<InputT> inputCoder,
                Map<TupleTag<?>, Coder<?>> outputCoderMap,
                List<PCollectionView<?>> sideInputs
        ) {
            this.ownerId = ownerId;
            this.pipelineOptions = pipelineOptions;
            this.doFn = doFn;
            this.windowingStrategy = windowingStrategy;
            this.outputMap = outputMap;
            this.mainOutputTag = mainOutputTag;
            this.inputCoder = inputCoder;
            this.outputCoderMap = outputCoderMap;
            this.sideInputs = sideInputs;
        }

        @Override
        public ParDoPushbackProcessor<InputT, OutputT> getEx() {
            return new ParDoPushbackProcessor<>(
                    doFn,
                    windowingStrategy,
                    outputMap,
                    pipelineOptions,
                    mainOutputTag,
                    inputCoder,
                    outputCoderMap,
                    sideInputs,
                    needsToEmit
            );
        }

        @Override
        public void isOutboundEdgeOfVerted(Edge edge, String vertexId) {
            if (ownerId.equals(vertexId)) {
                needsToEmit = true;
            }
        }

        @Override
        public void isInboundEdgeOfVertex(Edge edge, String vertexId) {
            //do nothing
        }
    }
}
