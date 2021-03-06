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

package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.beam.DAGBuilder;
import com.hazelcast.jet.beam.Utils;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/** Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's Flatten primitive. */
public class FlattenP extends AbstractProcessor {

    private final Map<Integer, Coder> inputOrdinalCoders;
    private final Coder outputCoder;
    @SuppressWarnings("FieldCanBeLocal") //do not remove, useful for debugging
    private final String ownerId;

    private FlattenP(Map<Integer, Coder> inputOrdinalCoders, Coder outputCoder, String ownerId) {
        this.inputOrdinalCoders = inputOrdinalCoders;
        this.outputCoder = outputCoder;
        this.ownerId = ownerId;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Coder inputCoder = inputOrdinalCoders.get(ordinal);
        WindowedValue<Object> windowedValue = Utils.decodeWindowedValue((byte[]) item, inputCoder);
        return tryEmit(Utils.encode(windowedValue, outputCoder));
    }

    public static final class Supplier implements SupplierEx<Processor>, DAGBuilder.WiringListener {

        private final Map<String, Coder> inputCollectionCoders;
        private final Coder outputCoder;
        private final String ownerId;
        private final Map<Integer, Coder> inputOrdinalCoders;

        public Supplier(Map<String, Coder> inputCoders, Coder outputCoder, String ownerId) {
            this.inputCollectionCoders = inputCoders;
            this.outputCoder = outputCoder;
            this.ownerId = ownerId;
            this.inputOrdinalCoders = new HashMap<>();
        }

        @Override
        public Processor getEx() {
            return new FlattenP(inputOrdinalCoders, outputCoder, ownerId);
        }

        @Override
        public void isOutboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId) {
            //do nothing
        }

        @Override
        public void isInboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId) {
            if (ownerId.equals(vertexId)) {
                Coder coder = inputCollectionCoders.get(edgeId);
                inputOrdinalCoders.put(edge.getDestOrdinal(), coder);
            }
        }
    }
}
