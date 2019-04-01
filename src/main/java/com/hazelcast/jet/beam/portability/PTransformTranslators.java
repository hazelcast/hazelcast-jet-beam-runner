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

import com.hazelcast.jet.beam.JetTranslationContext;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

import java.util.Map;

class PTransformTranslators {

    private static final Map<String, PTransformTranslator> TRANSLATORS;
    static {
        ImmutableMap.Builder<String, PTransformTranslator> builder = ImmutableMap.builder();
        builder.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator());
        builder.put(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, new CreateViewTranslator());
        builder.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslator());
        builder.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator());
        builder.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator());
        builder.put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowTranslator());
        builder.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator());
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
                        "Unknown type of URN %s for PTransform with id %s.",
                        transform.getTransform().getSpec().getUrn(), transform.getId()));
    }

    private static class ReadSourceTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

    private static class ParDoTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

    private static class CreateViewTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

    private static class GroupByKeyTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

    private static class FlattenTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

    private static class WindowTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

    private static class ImpulseTranslator implements PTransformTranslator {
        @Override
        public void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationContext context) {
            //todo
        }
    }

}
