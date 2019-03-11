package com.hazelcast.jet.beam;

import com.hazelcast.jet.core.*;
import com.hazelcast.jet.function.DistributedSupplier;
import org.apache.beam.sdk.values.TupleTag;

import java.util.*;

public class DAGBuilder {

    private final DAG dag = new DAG();

    private final Map<TupleTag, Vertex> edgeStartPoints = new HashMap<>();
    private final Map<TupleTag, Set<Vertex>> edgeEndPoints = new HashMap<>();

    private final List<WiringListener> listeners = new ArrayList<>();

    private int vertexId = 0;

    DAG getDag() {
        vireUp();
        return dag;
    }

    void registerConstructionListeners(WiringListener listener) {
        listeners.add(listener);
    }

    String newVertexId(String transformName) {
        return Integer.toString(vertexId++) + " (" + transformName + ")";
    }

    void registerEdgeStartPoint(TupleTag<?> tupleTag, Vertex vertex) {
        Vertex prevValue = edgeStartPoints.put(tupleTag, vertex);
        if (prevValue != null) throw new RuntimeException("Oops!");
    }

    void registerEdgeEndPoint(TupleTag<?> tupleTag, Vertex vertex) {
        edgeEndPoints.compute(
                tupleTag,
                (v, oldPoints) -> {
                    if (oldPoints == null) return Collections.singleton(vertex);

                    Set<Vertex> newPoints = new HashSet<>(oldPoints);
                    newPoints.add(vertex);

                    return newPoints;
                }
        );
    }

    Vertex addVertex(String id, ProcessorMetaSupplier processorMetaSupplier) {
        return dag.newVertex(id, processorMetaSupplier);
    }

    Vertex addVertex(String id, DistributedSupplier<Processor> processor) {
        return dag
                .newVertex(id, processor)
                .localParallelism(1) //todo: quick and dirty hack for now, can't leave it like this
                ;
    }

    private void vireUp() {
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
                boolean highPriorityEdge = edgeId.toString().contains("PCollectionView"); //todo: this is a hack!
                Vertex sourceVertex = edgeStartPoints.get(edgeId);
                if (sourceVertex == null) continue;

                Set<Vertex> destinationVertices = edgeEndPoints.get(edgeId);
                if (destinationVertices == null || destinationVertices.isEmpty()) continue;

                for (Vertex destinationVertex : destinationVertices) {
                    addEdge(sourceVertex, destinationVertex, highPriorityEdge);
                }
            }
        }

        private void addEdge(Vertex sourceVertex, Vertex destinationVertex, boolean highPriority) {
            //todo: set up the edges properly, including other aspects too, like parallelism

            try {
                Edge edge = Edge
                        .from(sourceVertex, getNextFreeOrdinal(sourceVertex, false))
                        .to(destinationVertex, getNextFreeOrdinal(destinationVertex, true)).priority(highPriority ? 1 : 2);
                dag.edge(edge);

                String sourceVertexName = sourceVertex.getName();
                String destinationVertexName = destinationVertex.getName();
                for (WiringListener listener : listeners) {
                    listener.isInboundEdgeOfVertex(edge, sourceVertexName);
                    listener.isOutboundEdgeOfVerted(edge, destinationVertexName);
                }
            } catch (Exception e) {
                e.printStackTrace(); //todo: what more to do with the error
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

        void isOutboundEdgeOfVerted(Edge edge, String vertexId);

        void isInboundEdgeOfVertex(Edge edge, String vertexId);
    }

}
