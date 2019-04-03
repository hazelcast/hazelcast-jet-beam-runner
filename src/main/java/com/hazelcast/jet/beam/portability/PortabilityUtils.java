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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.InvalidProtocolBufferException;

import java.util.Collection;

class PortabilityUtils {

    static String getOutput(PipelineNode.PTransformNode transform) {
        return asSingleString(getOutputs(transform));
    }

    static Collection<String> getOutputs(PipelineNode.PTransformNode transform) {
        return transform.getTransform().getOutputsMap().values();
    }

    static String getInput(PipelineNode.PTransformNode transform) {
        return asSingleString(getInputs(transform));
    }

    static Collection<String> getInputs(PipelineNode.PTransformNode transform) {
        return transform.getTransform().getInputsMap().values();
    }

    private static String asSingleString(Collection<String> map) {
        if (map.size() != 1) throw new RuntimeException("Oops!");
        return map.iterator().next();
    }

    static RunnerApi.ExecutableStagePayload getExecutionStagePayload(PipelineNode.PTransformNode transform) {
        try {
            ByteString payload = transform.getTransform().getSpec().getPayload();
            return RunnerApi.ExecutableStagePayload.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Oops!", e);
        }
    }

    static WindowingStrategy<Object, BoundedWindow> getWindowingStrategy(RunnerApi.Pipeline pipeline, String collectionId) {
        RunnerApi.PCollection pCollection = pipeline.getComponents().getPcollectionsOrThrow(collectionId);
        String windowingStrategyId = pCollection.getWindowingStrategyId();
        RunnerApi.WindowingStrategy windowingStrategyProto = pipeline.getComponents().getWindowingStrategiesOrThrow(windowingStrategyId);

        RehydratedComponents rehydratedComponents = RehydratedComponents.forComponents(pipeline.getComponents());

        try {
            return (WindowingStrategy<Object, BoundedWindow>) WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(String.format("Unable to hydrate GroupByKey windowing strategy %s.", windowingStrategyProto), e);
        }
    }
}
