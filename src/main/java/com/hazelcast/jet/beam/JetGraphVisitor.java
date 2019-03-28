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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;

class JetGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private final JetTranslationContext translationContext;

    private boolean finalized = false;

    JetGraphVisitor(JetPipelineOptions options) {
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
        Vertex vertex = typedTranslator.translate(getPipeline(), node, translationContext);

        // attach peeking (for debugging)
//        if (vertex.getName().equals("2 (View.AsIterable/View.CreatePCollectionView)")) {
//            System.out.println("attaching peek");
//            vertex.updateMetaSupplier(supplier -> {
//                supplier = DiagnosticProcessors.peekInputP(supplier);
//                supplier = DiagnosticProcessors.peekOutputP(supplier);
//                return supplier;
//            });
//        }
    }

}
