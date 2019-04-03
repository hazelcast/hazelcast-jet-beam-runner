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

package com.hazelcast.jet.beam.portability;

import com.hazelcast.jet.beam.DAGBuilder;
import com.hazelcast.jet.beam.JetTranslationContext;
import com.hazelcast.jet.beam.processors.AssignWindowP;
import com.hazelcast.jet.beam.processors.FlattenP;
import com.hazelcast.jet.beam.processors.ImpulseP;
import com.hazelcast.jet.beam.processors.WindowGroupP;
import com.hazelcast.jet.core.Vertex;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;

class PTransformTranslators {

    private static final Map<String, PTransformTranslator> TRANSLATORS;

    static {
        ImmutableMap.Builder<String, PTransformTranslator> builder = ImmutableMap.builder();
        builder.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator());
        builder.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator());
        builder.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator());
        builder.put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowTranslator());
        builder.put(ExecutableStage.URN, new ExecutableStageTranslator());
        builder.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslator());
        TRANSLATORS = builder.build();
    }

    static void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext translationContext) {
        TRANSLATORS
                .getOrDefault(
                        transform.getTransform().getSpec().getUrn(),
                        PTransformTranslators::urnNotFound)
                .translate(transform, pipeline, translationContext);
    }

    private static void urnNotFound(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
        throw new IllegalArgumentException(
                String.format(
                        "Unknown type of URN [%s] for PTransform with id [%s].",
                        transform.getTransform().getSpec().getUrn(), transform.getId()));
    }

    private static class ExecutableStageTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context) {
            RunnerApi.ExecutableStagePayload stagePayload = PortabilityUtils.getExecutionStagePayload(transform);

            String input = PortabilityUtils.getInput(transform);
            Collection<String> outputs = PortabilityUtils.getOutputs(transform);

            String transformName = transform.getId();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            ExecuteStageP.Supplier processorSupplier = new ExecuteStageP.Supplier(
                    stagePayload,
                    context.getJobInfo(),
                    outputs,
                    vertexId
            );
            Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);
            dagBuilder.registerConstructionListeners(processorSupplier);

            dagBuilder.registerEdgeEndPoint(input, vertex);

            for (String edgeId : outputs) {
                dagBuilder.registerCollectionOfEdge(edgeId, edgeId);
                dagBuilder.registerEdgeStartPoint(edgeId, vertex);
            }
        }
    }

    private static class ReshuffleTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context) {
            throw new UnsupportedOperationException("Reshuffle nodes aren't implemented yet!");
            //todo: we should cut out reshuffle nodes completely from the pipeline, explicitly reshuffling doesn't make sense in Jet (Jet does it implicitly all the time)
        }
    }

    private static class GroupByKeyTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context) {
            String transformName = transform.getId();

            String input = PortabilityUtils.getInput(transform);
            String output = PortabilityUtils.getOutput(transform);

            WindowingStrategy<Object, BoundedWindow> windowingStrategy = PortabilityUtils.getWindowingStrategy(pipeline, output);
            if (!windowingStrategy.getTrigger().isCompatible(DefaultTrigger.of()) && !windowingStrategy.getTrigger().isCompatible(Never.ever())) {
                throw new UnsupportedOperationException("Only DefaultTrigger and Never.NeverTrigger supported, got " + windowingStrategy.getTrigger());
            }
            if (windowingStrategy.getAllowedLateness().getMillis() != 0) {
                throw new UnsupportedOperationException("Non-zero allowed lateness not supported");
            }

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            Vertex vertex = dagBuilder.addVertex(vertexId, WindowGroupP.supplier(windowingStrategy, vertexId));

            dagBuilder.registerEdgeEndPoint(input, vertex);

            dagBuilder.registerCollectionOfEdge(output, output);
            dagBuilder.registerEdgeStartPoint(output, vertex);
        }

    }

    private static class FlattenTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context) {
            String transformName = transform.getId();

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);
            Vertex vertex = dagBuilder.addVertex(vertexId, FlattenP.supplier(vertexId));

            Collection<String> inputs = PortabilityUtils.getInputs(transform);
            for (String inputCollectionId : inputs) {
                dagBuilder.registerEdgeEndPoint(inputCollectionId, vertex);
            }

            String output = PortabilityUtils.getOutput(transform);
            dagBuilder.registerCollectionOfEdge(output, output);
            dagBuilder.registerEdgeStartPoint(output, vertex);
        }
    }

    private static class WindowTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context) {
            String transformName = transform.getId();

            String input = PortabilityUtils.getInput(transform);
            String output = PortabilityUtils.getOutput(transform);

            WindowingStrategy<Object, BoundedWindow> windowingStrategy = PortabilityUtils.getWindowingStrategy(pipeline, output);

            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            Vertex vertex = dagBuilder.addVertex(vertexId, AssignWindowP.supplier(windowingStrategy, vertexId));

            dagBuilder.registerEdgeEndPoint(input, vertex);

            dagBuilder.registerCollectionOfEdge(output, output);
            dagBuilder.registerEdgeStartPoint(output, vertex);
        }
    }

    private static class ImpulseTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context) {
            String transformName = transform.getId();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            Vertex vertex = dagBuilder.addVertex(vertexId, ImpulseP.supplier(vertexId));

            String output = PortabilityUtils.getOutput(transform);
            dagBuilder.registerCollectionOfEdge(output, output);
            dagBuilder.registerEdgeStartPoint(output, vertex);
        }
    }

}
