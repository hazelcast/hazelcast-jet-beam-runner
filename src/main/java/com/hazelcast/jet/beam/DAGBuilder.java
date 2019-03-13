package com.hazelcast.jet.beam;

import com.hazelcast.jet.core.*;
import com.hazelcast.jet.function.DistributedSupplier;
import org.apache.beam.sdk.values.TupleTag;

import java.util.*;

public class DAGBuilder {

    private final DAG dag = new DAG();

    private final Map<TupleTag, Vertex> edgeStartPoints = new HashMap<>();
    private final Map<TupleTag, Set<Vertex>> edgeEndPoints = new HashMap<>();
    private final Map<TupleTag, TupleTag> pCollsOfEdges = new HashMap<>();

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
        edgeEndPoints.compute(
                edgeId,
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
                TupleTag pCollId = pCollsOfEdges.get(edgeId);
                if (pCollId == null) throw new RuntimeException("Oops!");

                Vertex sourceVertex = edgeStartPoints.get(edgeId);
                if (sourceVertex == null) throw new RuntimeException("Oops!");

                Set<Vertex> destinationVertices = edgeEndPoints.get(edgeId);
                if (destinationVertices == null || destinationVertices.isEmpty()) continue;

                boolean highPriorityEdge = edgeId.toString().contains("PCollectionView"); //todo: this is a hack!
                for (Vertex destinationVertex : destinationVertices) {
                    addEdge(sourceVertex, destinationVertex, pCollId, highPriorityEdge);
                }
            }
        }

        private void addEdge(Vertex sourceVertex, Vertex destinationVertex, TupleTag pCollId, boolean highPriority) {
            //todo: set up the edges properly, including other aspects too, like parallelism

            try {
                Edge edge = Edge
                        .from(sourceVertex, getNextFreeOrdinal(sourceVertex, false))
                        .to(destinationVertex, getNextFreeOrdinal(destinationVertex, true)).priority(highPriority ? 1 : 2);
                dag.edge(edge);

                String sourceVertexName = sourceVertex.getName();
                String destinationVertexName = destinationVertex.getName();
                for (WiringListener listener : listeners) {
                    listener.isInboundEdgeOfVertex(edge, pCollId, destinationVertexName);
                    listener.isOutboundEdgeOfVertex(edge, pCollId, sourceVertexName);
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

        void isOutboundEdgeOfVertex(Edge edge, TupleTag pCollId, String vertexId);

        void isInboundEdgeOfVertex(Edge edge, TupleTag pCollId, String vertexId);
    }

}