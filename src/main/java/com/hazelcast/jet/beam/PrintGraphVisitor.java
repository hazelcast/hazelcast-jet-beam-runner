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
            printTags(Collections.singleton(Utils.getTupleTagId(((View.CreatePCollectionView) transform).getView())), "\t", sb);
        }
    }

    static void printTags(Collection<?> tags, String indent, StringBuilder sb) {
        for (Object tag : tags) {
            sb.append("\n\t\t").append(indent).append(tag);
        }
    }

    static void printValues(Collection<PValue> values, String indent, StringBuilder sb) {
        if (values == null) {
            sb.append("null");
            return;
        }
        for (PValue value : values) {
            sb.append("\n\t\t").append(indent).append(value.getName()).append(" (").append(Utils.getTupleTagId(value)).append(")");
        }
    }

    String print() {
        return sb.toString();
    }

}
