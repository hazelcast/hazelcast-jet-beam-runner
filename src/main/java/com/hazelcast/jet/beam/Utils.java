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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class Utils {

    public static String getTupleTagId(PValue value) {
        Map<TupleTag<?>, PValue> expansion = value.expand();
        if (expansion.size() != 1) throw new RuntimeException(); //todo: Houston, we have a problem!
        return expansion.keySet().iterator().next().getId();
    }

    static Collection<PValue> getMainInputs(Pipeline pipeline, TransformHierarchy.Node node) {
        if (node.getTransform() == null) {
            return null;
        }
        return TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(pipeline));
    }

    static Map<TupleTag<?>, PValue> getInputs(AppliedPTransform<?, ?, ?> appliedTransform) {
        return appliedTransform.getInputs();
    }

    static Map<TupleTag<?>, PValue> getAdditionalInputs(TransformHierarchy.Node node) {
        return node.getTransform() != null ? node.getTransform().getAdditionalInputs() : null;
    }

    static <T extends PValue> T getInput(AppliedPTransform<?, ?, ?> appliedTransform) {
        if (appliedTransform.getTransform() == null) {
            return null;
        }
        return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(appliedTransform));
    }

    static Map<TupleTag<?>, PValue> getOutputs(AppliedPTransform<?, ?, ?> appliedTransform) {
        if (appliedTransform.getTransform() == null) {
            return null;
        }
        return appliedTransform.getOutputs();
    }

    static Map.Entry<TupleTag<?>, PValue> getOutput(AppliedPTransform<?, ?, ?> appliedTransform) {
        return Iterables.getOnlyElement(getOutputs(appliedTransform).entrySet());
    }

    static <T> boolean isBounded(AppliedPTransform<?, ?, ?> appliedTransform) {
        return ((PCollection) getOutput(appliedTransform).getValue()).isBounded().equals(PCollection.IsBounded.BOUNDED);
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

    /**
     * Assigns the {@code list} to {@code count} sublists in a round-robin
     * fashion. One call returns the {@code index}-th sublist.
     *
     * <p>For example, for a 7-element list where {@code count == 3}, it would
     * respectively return for indices 0..2:
     * <pre>
     *   0, 3, 6
     *   1, 4
     *   2, 5
     * </pre>
     */
    public static <E> List<E> roundRobinSubList(List<E> list, int index, int count) {
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException("index=" + index + ", count=" + count);
        }
        return IntStream.range(0, list.size())
                        .filter(i -> i % count == index)
                        .mapToObj(list::get)
                        .collect(toList());
    }

    /**
     * Returns a deep clone of an object by serializing and deserializing it
     * (ser-de).
     */
    @SuppressWarnings("unchecked")
    public static <T> T serde(T object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
            byte[] byteData = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
            return (T) new ObjectInputStream(bais).readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
