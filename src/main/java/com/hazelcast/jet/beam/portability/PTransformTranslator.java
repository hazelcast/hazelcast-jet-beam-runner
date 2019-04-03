package com.hazelcast.jet.beam.portability;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode;

@FunctionalInterface
public interface PTransformTranslator {
    void translate(PipelineNode.PTransformNode transform, RunnerApi.Pipeline pipeline, JetTranslationPortabilityContext context);
}
