package com.hazelcast.jet.beam;

import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {

    static TupleTag<?> getTupleTag(PValue value) {
        Map<TupleTag<?>, PValue> expansion = value.expand();
        if (expansion.size() != 1) throw new RuntimeException(); //todo: Houston, we have a problem!
        return expansion.keySet().iterator().next();
    }

    static Collection<PValue> getMainInputs(Pipeline pipeline, TransformHierarchy.Node node) {
        return TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(pipeline));
    }

    static Map<TupleTag<?>, PValue> getInputs(AppliedPTransform<?, ?, ?> appliedTransform) {
        return appliedTransform.getInputs();
    }

    static Map<TupleTag<?>, PValue> getAdditionalInputs(TransformHierarchy.Node node) {
        return node.getTransform().getAdditionalInputs();
    }

    static <T extends PValue> T getInput(AppliedPTransform<?, ?, ?> appliedTransform) {
        return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(appliedTransform));
    }

    static Map<TupleTag<?>, PValue> getOutputs(AppliedPTransform<?, ?, ?> appliedTransform) {
        return appliedTransform.getOutputs();
    }

    static <T extends PValue> T getOutput(AppliedPTransform<?, ?, ?> appliedTransform) {
        return (T) Iterables.getOnlyElement(getOutputs(appliedTransform).values());
    }

    static <T> boolean isBounded(AppliedPTransform<?, ?, ?> appliedTransform) {
        return ((PCollection) getOutput(appliedTransform)).isBounded().equals(PCollection.IsBounded.BOUNDED);
    }

    static Map<TupleTag<?>, Coder<?>> getOutputCoders(AppliedPTransform<?, ?, ?> appliedTransform) {
        return appliedTransform
                .getOutputs()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() instanceof PCollection)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
    }

    static List<PCollectionView<?>> getSideInputs(AppliedPTransform<?, ?, ?> appliedTransform) {
        PTransform<?, ?> transform = appliedTransform.getTransform();
        if (transform instanceof ParDo.MultiOutput) {
            ParDo.MultiOutput multiParDo = (ParDo.MultiOutput) transform;
            return multiParDo.getSideInputs();
        } else if (transform instanceof ParDo.SingleOutput) {
            ParDo.SingleOutput singleParDo = (ParDo.SingleOutput) transform;
            return singleParDo.getSideInputs();
        }
        return Collections.emptyList();
    }

    public static Object getNull() {
        return Null.INSTANCE;
    }

    public static boolean isNull(Object o) {
        return o instanceof Null;
    }

    private static final class Null { //todo: is this safe? any other options for doing it?

        private static final Null INSTANCE = new Null();

    }
}
