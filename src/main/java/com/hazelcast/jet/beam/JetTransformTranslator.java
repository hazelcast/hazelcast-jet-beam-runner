package com.hazelcast.jet.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;

interface JetTransformTranslator<T extends PTransform> {

    void translate(Pipeline pipeline, TransformHierarchy.Node node, JetTranslationContext context);

}
