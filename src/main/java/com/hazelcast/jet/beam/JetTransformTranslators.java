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

package com.hazelcast.jet.beam;

import com.hazelcast.jet.beam.processors.AssignWindowP;
import com.hazelcast.jet.beam.processors.BoundedSourceP;
import com.hazelcast.jet.beam.processors.FlattenP;
import com.hazelcast.jet.beam.processors.ImpulseP;
import com.hazelcast.jet.beam.processors.ParDoP;
import com.hazelcast.jet.beam.processors.TestStreamP;
import com.hazelcast.jet.beam.processors.TestStreamP.SerializableTimestampedValue;
import com.hazelcast.jet.beam.processors.TestStreamP.SerializableWatermarkEvent;
import com.hazelcast.jet.beam.processors.ViewP;
import com.hazelcast.jet.beam.processors.WindowGroupP;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import org.apache.beam.runners.core.construction.CreatePCollectionViewTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
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
        TRANSLATORS.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator());
        TRANSLATORS.put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new TestStreamTranslator());
    }

    static JetTransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
        String urn = PTransformTranslation.urnForTransformOrNull(transform);
        return urn == null ? null : TRANSLATORS.get(urn);
    }

    private static class ReadSourceTranslator<T> implements JetTransformTranslator<PTransform<PBegin, PCollection<T>>> {

        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> appliedTransform =
                    (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>) node.toAppliedPTransform(pipeline);
            if (!Utils.isBounded(appliedTransform)) {
                throw new UnsupportedOperationException(); //todo
            }

            @SuppressWarnings("unchecked")
            BoundedSource<T> source;
            try {
                source = ReadTranslation.boundedSourceFromTransform(appliedTransform);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            SerializablePipelineOptions pipelineOptions = context.getOptions();
            ProcessorMetaSupplier processorSupplier = BoundedSourceP.supplier(source, pipelineOptions, vertexId);

            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

    private static class ParDoTranslator<InputT, OutputT> implements JetTransformTranslator<PTransform<PCollection<InputT>, PCollectionTuple>> {

        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, PTransform<PCollection<InputT>, PCollection<OutputT>>> appliedTransform =
                    (AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, PTransform<PCollection<InputT>, PCollection<OutputT>>>) node.toAppliedPTransform(pipeline);

            boolean usesStateOrTimers;
            try {
                usesStateOrTimers = ParDoTranslation.usesStateOrTimers(appliedTransform);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (usesStateOrTimers) throw new UnsupportedOperationException("State and/or timers not supported at the moment!");

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
            int count = 1;
            for (TupleTag<?> tag : outputs.keySet()) {
                if (!outputMap.containsKey(tag)) {
                    outputMap.put(tag, count++);
                }
            }

            // assume that the windowing strategy is the same for all outputs
            final WindowingStrategy<?, ?>[] windowingStrategy = new WindowingStrategy[1]; //todo: the array is an ugly hack

            // collect all output Coders and create a UnionCoder for our tagged outputs
            List<Coder<?>> outputCoders = Lists.newArrayList();
            for (TupleTag<?> tag : outputs.keySet()) {
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

            Map<TupleTag<?>, Coder<?>> outputCoderMap = Utils.getOutputCoders(appliedTransform);

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            SerializablePipelineOptions pipelineOptions = context.getOptions();
            Coder<InputT> coder = ((PCollection) Utils.getInput(appliedTransform)).getCoder();
            List<PCollectionView<?>> sideInputs = Utils.getSideInputs(appliedTransform);
            ParDoP.Supplier<InputT, OutputT> processorSupplier = new ParDoP.Supplier<>(
                    vertexId,
                    doFn,
                    windowingStrategy[0],
                    pipelineOptions, mainOutputTag, outputMap.keySet(),
                    coder,
                    outputCoderMap,
                    sideInputs
            );

            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);
            dagBuilder.registerConstructionListeners(processorSupplier);

            Collection<PValue> mainInputs = Utils.getMainInputs(pipeline, node);
            if (mainInputs.size() != 1) throw new RuntimeException("Oops!");
            dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(mainInputs.iterator().next()), vertex);

            Map<TupleTag<?>, PValue> additionalInputs = Utils.getAdditionalInputs(node);
            if (additionalInputs != null && !additionalInputs.isEmpty()) {
                for (TupleTag<?> tupleTag : additionalInputs.keySet()) {
                    dagBuilder.registerEdgeEndPoint(tupleTag.getId(), vertex);
                }
            }

            for (Map.Entry<TupleTag<?>, PValue> entry : outputs.entrySet()) {
                TupleTag<?> pCollId = entry.getKey();
                String edgeId = Utils.getTupleTagId(entry.getValue());
                dagBuilder.registerCollectionOfEdge(edgeId, pCollId.getId());
                dagBuilder.registerEdgeStartPoint(edgeId, vertex);
            }

            return vertex;
        }

    }

    private static class GroupByKeyTranslator<K, InputT> implements JetTransformTranslator<PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            // todo type arguments are wrong here, but we don't care
            AppliedPTransform<PCollection<K>, PCollection<InputT>, PTransform<PCollection<K>, PCollection<InputT>>> appliedTransform =
                    (AppliedPTransform<PCollection<K>, PCollection<InputT>, PTransform<PCollection<K>, PCollection<InputT>>>) node.toAppliedPTransform(pipeline);
            String transformName = appliedTransform.getFullName();

            PCollection<KV<K, InputT>> input = Utils.getInput(appliedTransform);
            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
            assert input != null : "null input";

            WindowingStrategy<InputT, ? extends BoundedWindow> outputWindowingStrategy =
                    (WindowingStrategy<InputT, ? extends BoundedWindow>) ((PCollection) Utils.getOutput(appliedTransform).getValue()).getWindowingStrategy();
            WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
            Trigger trigger = outputWindowingStrategy.getTrigger();
            if (!trigger.isCompatible(DefaultTrigger.of())
                    && !trigger.isCompatible(Never.ever())) {
                throw new UnsupportedOperationException("Only DefaultTrigger and Never.NeverTrigger supported, got "
                        + trigger);
            }
            if (outputWindowingStrategy.getAllowedLateness().getMillis() != 0) {
                throw new UnsupportedOperationException("Non-zero allowed lateness not supported");
            }

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            Vertex vertex = dagBuilder.addVertex(vertexId, WindowGroupP.supplier(windowingStrategy, vertexId));

            dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);

            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

    private static class CreateViewTranslator<T> implements JetTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {

        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>> appliedTransform =
                    (AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>>) node.toAppliedPTransform(pipeline);
            PCollectionView<T> view;
            try {
                view = CreatePCollectionViewTranslation.getView(appliedTransform); //todo: there is no SDK-agnostic specification for this; using it means your runner is tied to Java
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            PCollection<T> input = Utils.getInput(appliedTransform);

            Vertex vertex = dagBuilder.addVertex(vertexId, ViewP.supplier(view, input.getWindowingStrategy(), vertexId));

            dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);

            String viewTag = Utils.getTupleTagId(view);
            dagBuilder.registerCollectionOfEdge(viewTag, view.getTagInternal().getId());
            dagBuilder.registerEdgeStartPoint(viewTag, vertex);

            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

    private static class FlattenTranslator<T> implements JetTransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {

        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(pipeline);

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(appliedTransform.getFullName());
            Vertex vertex = dagBuilder.addVertex(vertexId, FlattenP.supplier(vertexId));

            Collection<PValue> mainInputs = Utils.getMainInputs(pipeline, node);
            for (PValue value : mainInputs) {
                PCollection<T> input = (PCollection<T>) value;
                dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);
            }

            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

    private static class WindowTranslator<T> implements JetTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>> appliedTransform =
                    (AppliedPTransform<PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>>) node.toAppliedPTransform(pipeline);
            WindowingStrategy<T, ? extends BoundedWindow> windowingStrategy =
                    (WindowingStrategy<T, ? extends BoundedWindow>) ((PCollection) Utils.getOutput(appliedTransform).getValue()).getWindowingStrategy();

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            Vertex vertex = dagBuilder.addVertex(vertexId, AssignWindowP.supplier(windowingStrategy, vertexId));

            PCollection<WindowedValue> input = Utils.getInput(appliedTransform);
            dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);

            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

    private static class ImpulseTranslator implements JetTransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {
        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(pipeline);

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            Vertex vertex = dagBuilder.addVertex(vertexId, ImpulseP.supplier(vertexId));

            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

    private static class TestStreamTranslator<T> implements JetTransformTranslator<PTransform<PBegin, PCollection<T>>> {
        @Override
        public Vertex translate(Pipeline pipeline, Node node, JetTranslationContext context) {
            AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(pipeline);

            String transformName = appliedTransform.getFullName();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            TestStream<T> transform = (TestStream<T>) appliedTransform.getTransform();

            // events in the transform are not serializable, we have to translate them. We'll also flatten the collection.
            List<Object> serializableEvents = transform.getEvents().stream()
                    .flatMap(e -> {
                        if (e instanceof TestStream.WatermarkEvent) {
                            return Stream.of(new SerializableWatermarkEvent(((WatermarkEvent<T>) e).getWatermark().getMillis()));
                        } else if (e instanceof TestStream.ElementEvent) {
                            return StreamSupport.stream(((ElementEvent<T>) e).getElements().spliterator(), false)
                                                .map(te -> new SerializableTimestampedValue<>(te.getValue(), te.getTimestamp()));
                        } else {
                            throw new UnsupportedOperationException("Event type not supported in TestStream: " + e.getClass() + ", event: " + e);
                        }
                    })
                    .collect(toList());
            Vertex vertex = dagBuilder.addVertex(vertexId, TestStreamP.supplier(serializableEvents));

            Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
            String outputEdgeId = Utils.getTupleTagId(output.getValue());
            dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
            return vertex;
        }
    }

}
