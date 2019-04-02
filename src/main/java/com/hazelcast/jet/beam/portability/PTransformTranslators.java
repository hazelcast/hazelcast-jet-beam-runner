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
import com.hazelcast.jet.beam.processors.ImpulseP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

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

    static void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext translationContext) {
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
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            RunnerApi.ExecutableStagePayload executionStagePayload = PortabilityUtils.getExecutionStagePayload(transform);
            Map.Entry<String, String> input = PortabilityUtils.getInput(transform);
            Map<String, String> outputs = PortabilityUtils.getOutputs(transform);

            String transformName = transform.getId();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            Vertex vertex = dagBuilder.addVertex(vertexId, Processors.noopP()); //todo: dummy processor!

            dagBuilder.registerEdgeEndPoint(input.getValue(), vertex);

            for (String edgeId : outputs.values()) {
                dagBuilder.registerCollectionOfEdge(edgeId, edgeId);
                dagBuilder.registerEdgeStartPoint(edgeId, vertex);
            }
        }
    }

    private static class ReshuffleTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            System.out.println(); //todo: remove
            //todo
        }
    }

    private static class GroupByKeyTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            System.out.println(); //todo: remove
            //todo
        }
    }

    private static class FlattenTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            System.out.println(); //todo: remove
            //todo
        }
    }

    private static class WindowTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            System.out.println(); //todo: remove
            //todo
        }
    }

    private static class ImpulseTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            String transformName = transform.getId();
            DAGBuilder dagBuilder = context.getDagBuilder();
            String vertexId = dagBuilder.newVertexId(transformName);

            Vertex vertex = dagBuilder.addVertex(vertexId, ImpulseP.supplier(vertexId));

            Map.Entry<String, String> output = PortabilityUtils.getOutput(transform);
            String outputEdgeId = output.getValue();
            dagBuilder.registerCollectionOfEdge(outputEdgeId, outputEdgeId);
            dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex);
        }
    }

}
