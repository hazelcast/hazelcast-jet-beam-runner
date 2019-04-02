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
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.InvalidProtocolBufferException;

import java.util.Map;

class PortabilityUtils {

    static Map.Entry<String, String> getOutput(PipelineNode.PTransformNode transform) {
        return asSingleEntry(getOutputs(transform));
    }

    static Map<String, String> getOutputs(PipelineNode.PTransformNode transform) {
        return transform.getTransform().getOutputsMap();
    }

    static Map.Entry<String, String> getInput(PipelineNode.PTransformNode transform) {
        return asSingleEntry(getInputs(transform));
    }

    static Map<String, String> getInputs(PipelineNode.PTransformNode transform) {
        return transform.getTransform().getInputsMap();
    }

    private static Map.Entry<String, String> asSingleEntry(Map<String, String> map) {
        if (map.size() != 1) throw new RuntimeException("Oops!");
        return map.entrySet().iterator().next();
    }

    static RunnerApi.ExecutableStagePayload getExecutionStagePayload(PipelineNode.PTransformNode transform) {
        try {
            ByteString payload = transform.getTransform().getSpec().getPayload();
            return RunnerApi.ExecutableStagePayload.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Oops!", e);
        }
    }
}
