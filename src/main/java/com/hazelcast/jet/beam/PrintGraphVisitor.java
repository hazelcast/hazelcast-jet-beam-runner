package com.hazelcast.jet.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

class PrintGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private final StringBuilder sb = new StringBuilder();

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
        sb.append("\n\n\tNode: ").append(node.getFullName()).append("@").append(System.identityHashCode(node));

        PTransform<?, ?> transform = node.getTransform();
        sb.append("\n\t\tTransform: ").append(transform);

        Map<TupleTag<?>, PValue> additionalInputs = Utils.getAdditionalInputs(node);
        if (additionalInputs != null && !additionalInputs.isEmpty()) {
            sb.append("\n\t\tSide inputs: ");
            printTags(additionalInputs.keySet(), "\t", sb);
        }

        sb.append("\n\t\tInputs: ");
        Collection<PValue> mainInputs = Utils.getMainInputs(getPipeline(), node);
        printValues(mainInputs, "\t", sb);

        sb.append("\n\t\tOutputs: ");
        printValues(node.getOutputs().values(), "\t", sb);

        if (transform instanceof View.CreatePCollectionView) {
            sb.append("\n\t\tSide outputs:");
            printTags(Collections.singleton(Utils.getTupleTag(((View.CreatePCollectionView) transform).getView())), "\t", sb);
        }
    }

    static void printTags(Collection<TupleTag<?>> tags, String indent, StringBuilder sb) {
        for (TupleTag tag : tags) {
            sb.append("\n\t\t").append(indent).append(tag);
        }
    }

    static void printValues(Collection<PValue> values, String indent, StringBuilder sb) {
        for (PValue value : values) {
            sb.append("\n\t\t").append(indent).append(value.getName()).append(" (").append(Utils.getTupleTag(value)).append(")");
        }
    }

    String print() {
        return sb.toString();
    }

}
