package com.hazelcast.jet.beam;

import com.hazelcast.jet.core.DAG;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;

class JetGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private final JetTranslationContext translationContext;

    private boolean finalized = false;

    JetGraphVisitor(JetRunnerOptions options) {
        this.translationContext = new JetTranslationContext(options);
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
        if (finalized) throw new IllegalStateException("Attempting to traverse an already finalized pipeline!");

        PTransform<?, ?> transform = node.getTransform();
        if (transform != null) {
            JetTransformTranslator<?> translator = JetTransformTranslators.getTranslator(transform);
            if (translator != null) {
                translate(node, translator);
                return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            }
        }
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
        if (finalized) throw new IllegalStateException("Attempting to traverse an already finalized pipeline!");
        if (node.isRootNode()) finalized = true;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
        PTransform<?, ?> transform = node.getTransform();
        JetTransformTranslator<?> translator = JetTransformTranslators.getTranslator(transform);
        if (translator == null) {
            String transformUrn = PTransformTranslation.urnForTransform(transform);
            throw new UnsupportedOperationException("The transform " + transformUrn + " is currently not supported.");
        }
        translate(node, translator);
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
        // do nothing here
    }

    DAG getDAG() {
        return translationContext.getDagBuilder().getDag();
    }

    private <T extends PTransform<?, ?>> void translate(
            TransformHierarchy.Node node,
            JetTransformTranslator<?> translator
    ) {
        @SuppressWarnings("unchecked")
        JetTransformTranslator<T> typedTranslator = (JetTransformTranslator<T>) translator;
        typedTranslator.translate(getPipeline(), node, translationContext);
    }

}