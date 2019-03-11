package com.hazelcast.jet.beam;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.beam.processors.BoundedSourceProcessorSupplier;
import com.hazelcast.jet.beam.processors.CreateViewProcessor;
import com.hazelcast.jet.beam.processors.ParDoProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import org.apache.beam.runners.core.construction.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

@SuppressWarnings("unchecked")
class JetTransformTranslators {

    /**
     * A map from a Transform URN to the translator.
     */
    private static final Map<String, JetTransformTranslator> TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator());
        TRANSLATORS.put(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, new CreateViewTranslator());
        TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslator());
        TRANSLATORS.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator());
        TRANSLATORS.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator());
        TRANSLATORS.put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowTranslator());
    }

    static JetTransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
        String urn = PTransformTranslation.urnForTransformOrNull(transform);
        return urn == null ? null : TRANSLATORS.get(urn);
    }

    private static class ReadSourceTranslator<T> implements JetTransformTranslator<PTransform<PBegin, PCollection<T>>> {

        @Override
        public void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context) {
            AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> appliedTransform =
                    (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>) node.toAppliedPTransform(pipeline);
            if (Utils.isBounded(appliedTransform)) {
                @SuppressWarnings("unchecked")
                BoundedSource<T> source;
                try {
                    source = ReadTranslation.boundedSourceFromTransform(appliedTransform);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                PCollection<T> output = Utils.getOutput(appliedTransform);

                String transformName = appliedTransform.getFullName();
                SerializablePipelineOptions pipelineOptions = context.getOptions();
                ProcessorMetaSupplier processorSupplier = new BoundedSourceProcessorSupplier(source, pipelineOptions);

                DAGBuilder dagBuilder = context.getDagBuilder();
                String vertexId = dagBuilder.newVertexId(transformName);
                Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

                dagBuilder.registerEdgeStartPoint(Utils.getTupleTag(output), vertex);
            } else {
                throw new UnsupportedOperationException(); //todo
            }
        }
    }

    private static class ParDoTranslator<InputT, OutputT> implements JetTransformTranslator<PTransform<PCollection<InputT>, PCollectionTuple>> {

        @Override
        public void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, PTransform<PCollection<InputT>, PCollection<OutputT>>> appliedTransform =
                    (AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, PTransform<PCollection<InputT>, PCollection<OutputT>>>) node.toAppliedPTransform(pipeline);

            DoFn<InputT, OutputT> doFn;
            try {
                doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(appliedTransform);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (DoFnSignatures.signatureForDoFn(doFn).processElement().isSplittable()) {
                throw new IllegalStateException("Not expected to directly translate splittable DoFn, should have been overridden: " + doFn); //todo
            }

            Map<TupleTag<?>, PValue> outputs = Utils.getOutputs(appliedTransform);

            TupleTag<OutputT> mainOutputTag;
            try {
                mainOutputTag = (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(appliedTransform);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Map<TupleTag<?>, Integer> outputMap = new HashMap<>();
            // put the main output at index 0, FlinkMultiOutputDoFnFunction  expects this
            outputMap.put(mainOutputTag, 0);
            int count = 1;
            for (TupleTag<?> tag : outputs.keySet()) {
                if (!outputMap.containsKey(tag)) {
                    outputMap.put(tag, count++);
                }
            }

            // Union coder elements must match the order of the output tags.
            Map<Integer, TupleTag<?>> indexMap = Maps.newTreeMap();
            for (Map.Entry<TupleTag<?>, Integer> entry : outputMap.entrySet()) {
                indexMap.put(entry.getValue(), entry.getKey());
            }

            // assume that the windowing strategy is the same for all outputs
            final WindowingStrategy<?, ?>[] windowingStrategy = new WindowingStrategy[1]; //todo: the array is an ugly hack

            // collect all output Coders and create a UnionCoder for our tagged outputs
            List<Coder<?>> outputCoders = Lists.newArrayList();
            for (TupleTag<?> tag : indexMap.values()) {
                PValue taggedValue = outputs.get(tag);
                checkState(
                        taggedValue instanceof PCollection,
                        "Within ParDo, got a non-PCollection output %s of type %s",
                        taggedValue,
                        taggedValue.getClass().getSimpleName());
                PCollection<?> coll = (PCollection<?>) taggedValue;
                outputCoders.add(coll.getCoder());
                windowingStrategy[0] = coll.getWindowingStrategy();
            }

            if (windowingStrategy[0] == null) {
                throw new IllegalStateException("No outputs defined.");
            }

            boolean usesStateOrTimers;
            try {
                usesStateOrTimers = ParDoTranslation.usesStateOrTimers(appliedTransform);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Map<TupleTag<?>, Coder<?>> outputCoderMap = Utils.getOutputCoders(appliedTransform);

            if (usesStateOrTimers) {
                throw new UnsupportedOperationException(); //todo
            }

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            SerializablePipelineOptions pipelineOptions = context.getOptions();
            Coder<InputT> coder = ((PCollection) Utils.getInput(appliedTransform)).getCoder();
            List<PCollectionView<?>> sideInputs = Utils.getSideInputs(appliedTransform);
            ParDoProcessor.Supplier<InputT, OutputT> processorSupplier = new ParDoProcessor.Supplier<>(
                    vertexId,
                    doFn,
                    windowingStrategy[0],
                    outputMap,
                    pipelineOptions,
                    mainOutputTag,
                    coder,
                    outputCoderMap,
                    sideInputs
            );

            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);
            dagBuilder.registerConstructionListeners(processorSupplier);

            Collection<PValue> mainInputs = Utils.getMainInputs(pipeline, node);
            if (mainInputs.size() != 1) throw new RuntimeException("Oops!");
            dagBuilder.registerEdgeEndPoint(Utils.getTupleTag(mainInputs.iterator().next()), vertex);

            Map<TupleTag<?>, PValue> additionalInputs = Utils.getAdditionalInputs(node);
            if (additionalInputs != null && !additionalInputs.isEmpty()) {
                for (TupleTag<?> tupleTag : additionalInputs.keySet()) {
                    dagBuilder.registerEdgeEndPoint(tupleTag, vertex);
                }
            }

            for (PValue value : outputs.values()) {
                dagBuilder.registerEdgeStartPoint(Utils.getTupleTag(value), vertex);
            }

        }

    }

    private static class GroupByKeyTranslator<K, InputT> implements JetTransformTranslator<PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

        @Override
        public void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<K>, PCollection<InputT>, PTransform<PCollection<K>, PCollection<InputT>>> appliedTransform =
                    (AppliedPTransform<PCollection<K>, PCollection<InputT>, PTransform<PCollection<K>, PCollection<InputT>>>) node.toAppliedPTransform(pipeline);
            if (Utils.isBounded(appliedTransform)) {
                String transformName = appliedTransform.getFullName();
                DistributedFunction<WindowedValue<KV<K, InputT>>, K> keyExtractorFunction =
                        (DistributedFunction<WindowedValue<KV<K, InputT>>, K>) windowedKeyValuePair -> windowedKeyValuePair.getValue().getKey();
                DistributedSupplier<Processor> processorSupplier = Processors.aggregateByKeyP(
                        Collections.singletonList(keyExtractorFunction),
                        AggregateOperations.toList(),
                        (DistributedBiFunction<K, List<WindowedValue<KV<K, InputT>>>, WindowedValue<KV<K, Iterable<InputT>>>>)
                                (k, windowedValues) ->
                                        WindowedValue.valueInGlobalWindow( //todo: this will definitely not work for unbounded streams...
                                                KV.of(
                                                        k,
                                                        windowedValues.stream()
                                                                .map(WindowedValue::getValue)
                                                                .map(KV::getValue)
                                                                .collect(Collectors.toList())
                                                )
                                        )
                );

                DAGBuilder dagBuilder = context.getDagBuilder();
                String vertexId = dagBuilder.newVertexId(transformName);
                Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

                PCollection<KV<K, InputT>> input = Utils.getInput(appliedTransform);
                dagBuilder.registerEdgeEndPoint(Utils.getTupleTag(input), vertex);

                PCollection<KV<K, Iterable<InputT>>> output = Utils.getOutput(appliedTransform);
                dagBuilder.registerEdgeStartPoint(Utils.getTupleTag(output), vertex);
            } else {
                throw new UnsupportedOperationException(); //todo
            }
        }

    }

    private static class CreateViewTranslator<T> implements JetTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {

        @Override
        public void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>> appliedTransform =
                    (AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>>) node.toAppliedPTransform(pipeline);
            PCollectionView<T> view;
            try {
                view = CreatePCollectionViewTranslation.getView(appliedTransform); //todo: there is no SDK-agnostic specification for this; using it means your runner is tied to Java
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String transformName = appliedTransform.getFullName();
            DistributedSupplier<Processor> processorSupplier = () -> new CreateViewProcessor(view);

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

            PCollection<T> input = Utils.getInput(appliedTransform);
            dagBuilder.registerEdgeEndPoint(Utils.getTupleTag(input), vertex);

            PCollection<T> output = Utils.getOutput(appliedTransform);
            dagBuilder.registerEdgeStartPoint(Utils.getTupleTag(output), vertex);

            TupleTag<?> viewTag = Utils.getTupleTag(view);
            dagBuilder.registerEdgeStartPoint(viewTag, vertex); //todo: view out edges should most likely be of broadcast type
        }
    }

    private static class FlattenTranslator<T> implements JetTransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {

        @Override
        public void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context) {
            AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(pipeline);
            DistributedSupplier<Processor> processorSupplier = Processors.mapP(DistributedFunction.identity());

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(appliedTransform.getFullName());
            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

            Collection<PValue> mainInputs = Utils.getMainInputs(pipeline, node);
            if (mainInputs.isEmpty()) {
                throw new RuntimeException("Oops!");
            } else {
                for (PValue value : mainInputs) {
                    PCollection<T> input = (PCollection<T>) value;
                    dagBuilder.registerEdgeEndPoint(Utils.getTupleTag(input), vertex);
                }
            }

            PCollection<T> output = Utils.getOutput(appliedTransform);
            dagBuilder.registerEdgeStartPoint(Utils.getTupleTag(output), vertex);
        }
    }

    private static class WindowTranslator<T> implements JetTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
        @Override
        public void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>> appliedTransform =
                    (AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>>) node.toAppliedPTransform(pipeline);
            WindowingStrategy<T, ? extends BoundedWindow> windowingStrategy =
                    (WindowingStrategy<T, ? extends BoundedWindow>) Utils.<PCollection<T>>getOutput(appliedTransform).getWindowingStrategy();
            WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

            DistributedSupplier<Processor> processorSupplier = Processors.flatMapP(
                    (DistributedFunction<Object, Traverser<?>>) o -> {
                        WindowedValue input = (WindowedValue) o;
                        Collection<? extends BoundedWindow> windows = windowFn.assignWindows(new WindowAssignContext<>(windowFn, input)); //todo: tons of garbage!

                        return Traversers.traverseStream(
                                windows.stream().map(window -> WindowedValue.of(input.getValue(), input.getTimestamp(), window, input.getPane()))
                        );
                    }
            );

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

            PCollection<WindowedValue> input = Utils.getInput(appliedTransform);
            dagBuilder.registerEdgeEndPoint(Utils.getTupleTag(input), vertex);

            PCollection<WindowedValue> output = Utils.getOutput(appliedTransform);
            dagBuilder.registerEdgeStartPoint(Utils.getTupleTag(output), vertex);
        }

        private static class WindowAssignContext<InputT, W extends BoundedWindow> extends WindowFn<InputT, W>.AssignContext {
            private final WindowedValue<InputT> value;

            WindowAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
                fn.super();
                if (Iterables.size(value.getWindows()) != 1) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "%s passed to window assignment must be in a single window, but it was in %s: %s",
                                    WindowedValue.class.getSimpleName(),
                                    Iterables.size(value.getWindows()),
                                    value.getWindows()));
                }
                this.value = value;
            }

            @Override
            public InputT element() {
                return value.getValue();
            }

            @Override
            public Instant timestamp() {
                return value.getTimestamp();
            }

            @Override
            public BoundedWindow window() {
                return Iterables.getOnlyElement(value.getWindows());
            }
        }
    }

}
