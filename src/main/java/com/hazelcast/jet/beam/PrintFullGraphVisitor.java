package com.hazelcast.jet.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

class PrintFullGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private final StringBuilder sb = new StringBuilder();

    private int depth;

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
        printNode(node, " ENTER COMPOSITE");
        depth++;
        return super.enterCompositeTransform(node);
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
        --depth;
        printNode(node, " EXIT COMPOSITE");
        super.leaveCompositeTransform(node);
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
        printNode(node, " PRIMITIVE");
    }

    private void printNode(TransformHierarchy.Node node, String prefix) {
        String indent = genTabs(2 * depth);
        sb.append("\n\n").append(indent).append(depth).append(prefix).append(" Node: ").append(node.getFullName()).append("@").append(System.identityHashCode(node));

        PTransform<?, ?> transform = node.getTransform();
        sb.append("\n\t").append(indent).append("Transform: ").append(transform);

        Map<TupleTag<?>, PValue> additionalInputs = Utils.getAdditionalInputs(node);
        if (additionalInputs != null && !additionalInputs.isEmpty()) {
            sb.append("\n\t\tSide inputs: ");
            PrintGraphVisitor.printTags(additionalInputs.keySet(), indent, sb);
        }

        sb.append("\n\t").append(indent).append("Inputs: ");
        Collection<PValue> mainInputs = Utils.getMainInputs(getPipeline(), node);
        PrintGraphVisitor.printValues(mainInputs, indent, sb);

        sb.append("\n\t").append(indent).append("Outputs: ");
        PrintGraphVisitor.printValues(node.getOutputs().values(), indent, sb);

        if (transform instanceof View.CreatePCollectionView) {
            sb.append("\n\t\tSide outputs:");
            PrintGraphVisitor.printTags(Collections.singleton(Utils.getTupleTag(((View.CreatePCollectionView) transform).getView())), "\t", sb);
        }
    }

    String print() {
        return sb.toString();
    }

    private static String genTabs(int n) {
        char[] charArray = new char[n];
        Arrays.fill(charArray, ' ');
        return new String(charArray);
    }
}
