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
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DAGBuilder {

    private static final FunctionEx<Object, Object> PARTITION_KEY_EXTRACTOR = t -> {
        Object key = null;
        if (t instanceof WindowedValue) {
            t = ((WindowedValue) t).getValue();
        }
        if (t instanceof KV) {
            key = ((KV) t).getKey();
        }
        return key == null ? "all" : key;
    };

    private final DAG dag = new DAG();
    private final int localParallelism;

    private final Map<TupleTag, Vertex> edgeStartPoints = new HashMap<>();
    private final Map<TupleTag, List<Vertex>> edgeEndPoints = new HashMap<>();
    private final Map<TupleTag, TupleTag> pCollsOfEdges = new HashMap<>();

    private final List<WiringListener> listeners = new ArrayList<>();

    private int vertexId = 0;

    DAGBuilder(JetPipelineOptions options) {
        this.localParallelism = options.getJetLocalParallelism();
    }

    DAG getDag() {
        wireUp();
        return dag;
    }

    void registerConstructionListeners(WiringListener listener) {
        listeners.add(listener);
    }

    String newVertexId(String transformName) {
        return vertexId++ + " (" + transformName + ")";
    }

    void registerCollectionOfEdge(TupleTag<?> edgeId, TupleTag pCollId) {
        TupleTag prevPCollId = pCollsOfEdges.put(edgeId, pCollId);
        if (prevPCollId != null) throw new RuntimeException("Oops!");
    }

    void registerEdgeStartPoint(TupleTag<?> edgeId, Vertex vertex) {
        Vertex prevVertex = edgeStartPoints.put(edgeId, vertex);
        if (prevVertex != null) throw new RuntimeException("Oops!");
    }

    void registerEdgeEndPoint(TupleTag<?> edgeId, Vertex vertex) {
        edgeEndPoints
                .computeIfAbsent(edgeId, x -> new ArrayList<>())
                .add(vertex);
    }

    Vertex addVertex(String id, ProcessorMetaSupplier processorMetaSupplier) {
        return dag.newVertex(id, processorMetaSupplier);
    }

    Vertex addVertex(String id, SupplierEx<Processor> processor, boolean canBeParallel) {
        return dag
                .newVertex(id, processor)
                .localParallelism(canBeParallel ? localParallelism : 1) //todo: quick and dirty hack for now, can't leave it like this
                ;
    }

    private void wireUp() {
        new WiringInstaller().wireUp();
    }

    private class WiringInstaller {

        private final Map<Vertex, Integer> inboundOrdinals = new HashMap<>();
        private final Map<Vertex, Integer> outboundOrdinals = new HashMap<>();

        void wireUp() {
            Collection<TupleTag> edgeIds = new HashSet<>();
            edgeIds.addAll(edgeStartPoints.keySet());
            edgeIds.addAll(edgeEndPoints.keySet());

            for (TupleTag edgeId : edgeIds) {
                TupleTag pCollId = pCollsOfEdges.get(edgeId);
                if (pCollId == null) throw new RuntimeException("Oops!");

                Vertex sourceVertex = edgeStartPoints.get(edgeId);
                if (sourceVertex == null) throw new RuntimeException("Oops!");

                List<Vertex> destinationVertices = edgeEndPoints.getOrDefault(edgeId, Collections.emptyList());
                boolean sideInputEdge = edgeId.toString().contains("PCollectionView"); //todo: this is a hack!
                for (Vertex destinationVertex : destinationVertices) {
                    addEdge(sourceVertex, destinationVertex, pCollId, sideInputEdge);
                }
            }
        }

        private void addEdge(Vertex sourceVertex, Vertex destinationVertex, TupleTag pCollId, boolean sideInputEdge) {
            //todo: set up the edges properly, including other aspects too, like parallelism

            try {
                Edge edge = Edge
                        .from(sourceVertex, getNextFreeOrdinal(sourceVertex, false))
                        .to(destinationVertex, getNextFreeOrdinal(destinationVertex, true))
                        .distributed();
                if (sideInputEdge) {
                    edge = edge.broadcast();
                } else {
                    edge = edge.partitioned(PARTITION_KEY_EXTRACTOR); // todo: we likely don't need to partition everything
                }
                dag.edge(edge);

                String sourceVertexName = sourceVertex.getName();
                String destinationVertexName = destinationVertex.getName();
                for (WiringListener listener : listeners) {
                    listener.isInboundEdgeOfVertex(edge, pCollId, destinationVertexName);
                    listener.isOutboundEdgeOfVertex(edge, pCollId, sourceVertexName);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private int getNextFreeOrdinal(Vertex vertex, boolean inbound) {
            Map<Vertex, Integer> ordinals = inbound ? inboundOrdinals : outboundOrdinals;
            int nextOrdinal = 1 + ordinals.getOrDefault(vertex, -1);
            ordinals.put(vertex, nextOrdinal);
            return nextOrdinal;
        }

    }

    public interface WiringListener {

        void isOutboundEdgeOfVertex(Edge edge, TupleTag pCollId, String vertexId);

        void isInboundEdgeOfVertex(Edge edge, TupleTag pCollId, String vertexId);
    }

}
