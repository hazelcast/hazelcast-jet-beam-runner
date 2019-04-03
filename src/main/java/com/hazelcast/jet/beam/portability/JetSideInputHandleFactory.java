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

package com.hazelcast.jet.beam.portability;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JetSideInputHandleFactory implements StateRequestHandlers.SideInputHandlerFactory {

    static StateRequestHandlers.SideInputHandlerFactory forStage(ExecutableStage executableStage) {
        ImmutableMap.Builder<RunnerApi.ExecutableStagePayload.SideInputId, PipelineNode.PCollectionNode> sideInputBuilder = ImmutableMap.builder();
        for (SideInputReference sideInput : executableStage.getSideInputs()) {
            sideInputBuilder.put(
                    RunnerApi.ExecutableStagePayload.SideInputId.newBuilder()
                            .setTransformId(sideInput.transform().getId())
                            .setLocalName(sideInput.localName())
                            .build(),
                    sideInput.collection());
        }
        return new JetSideInputHandleFactory(sideInputBuilder.build());
    }

    // Map from side input id to global PCollection id.
    private final Map<RunnerApi.ExecutableStagePayload.SideInputId, PipelineNode.PCollectionNode> sideInputToCollection;

    private JetSideInputHandleFactory(Map<RunnerApi.ExecutableStagePayload.SideInputId, PipelineNode.PCollectionNode> sideInputToCollection) {
        this.sideInputToCollection = sideInputToCollection;
    }

    @Override
    public <T, V, W extends BoundedWindow> StateRequestHandlers.SideInputHandler<V, W> forSideInput(
            String transformId,
            String sideInputId,
            RunnerApi.FunctionSpec accessPattern,
            Coder<T> elementCoder, Coder<W> windowCoder
    ) {
        PipelineNode.PCollectionNode collectionNode = sideInputToCollection.get(
                RunnerApi.ExecutableStagePayload.SideInputId.newBuilder()
                        .setTransformId(transformId)
                        .setLocalName(sideInputId)
                        .build()
        );
        if (collectionNode != null)
            throw new RuntimeException("No side input for [" + transformId + "/" + sideInputId + "]!");

        if (PTransformTranslation.ITERABLE_SIDE_INPUT.equals(accessPattern.getUrn())) {
            Coder<V> outputCoder = (Coder<V>) elementCoder;
            return forIterableSideInput(getBroadcastVariable(collectionNode.getId()), outputCoder, windowCoder);
        } else if (PTransformTranslation.MULTIMAP_SIDE_INPUT.equals(accessPattern.getUrn()) || Materializations.MULTIMAP_MATERIALIZATION_URN.equals(accessPattern.getUrn())) {
            KvCoder<?, V> kvCoder = (KvCoder<?, V>) elementCoder;
            return forMultimapSideInput(getBroadcastVariable(collectionNode.getId()),
                    kvCoder.getKeyCoder(),
                    kvCoder.getValueCoder(),
                    windowCoder);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown side input access pattern: %s", accessPattern));
        }
    }

    private <RT> List<RT> getBroadcastVariable(String name) {
        throw new UnsupportedOperationException(); //todo: this is a Flink thing, need to figure out the Jet equivalent
    }

    private <T, W extends BoundedWindow> StateRequestHandlers.SideInputHandler<T, W> forIterableSideInput(
            List<WindowedValue<T>> broadcastVariable, Coder<T> elementCoder, Coder<W> windowCoder) {
        ImmutableMultimap.Builder<Object, T> windowToValuesBuilder = ImmutableMultimap.builder();
        for (WindowedValue<T> windowedValue : broadcastVariable) {
            for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
                @SuppressWarnings("unchecked")
                W window = (W) boundedWindow;
                windowToValuesBuilder.put(windowCoder.structuralValue(window), windowedValue.getValue());
            }
        }
        ImmutableMultimap<Object, T> windowToValues = windowToValuesBuilder.build();

        return new StateRequestHandlers.SideInputHandler<T, W>() {
            @Override
            public Iterable<T> get(byte[] key, W window) {
                return windowToValues.get(windowCoder.structuralValue(window));
            }

            @Override
            public Coder<T> resultCoder() {
                return elementCoder;
            }
        };
    }

    private <K, V, W extends BoundedWindow> StateRequestHandlers.SideInputHandler<V, W> forMultimapSideInput(
            List<WindowedValue<KV<K, V>>> broadcastVariable,
            Coder<K> keyCoder,
            Coder<V> valueCoder,
            Coder<W> windowCoder) {
        ImmutableMultimap.Builder<SideInputKey, V> multimap = ImmutableMultimap.builder();
        for (WindowedValue<KV<K, V>> windowedValue : broadcastVariable) {
            K key = windowedValue.getValue().getKey();
            V value = windowedValue.getValue().getValue();

            for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
                @SuppressWarnings("unchecked")
                W window = (W) boundedWindow;
                multimap.put(
                        new SideInputKey(keyCoder.structuralValue(key), windowCoder.structuralValue(window)),
                        value
                );
            }
        }

        return new MultimapSideInputHandler<>(multimap.build(), keyCoder, valueCoder, windowCoder);
    }

    private static class MultimapSideInputHandler<K, V, W extends BoundedWindow> implements StateRequestHandlers.SideInputHandler<V, W> {

        private final Multimap<SideInputKey, V> collection;
        private final Coder<K> keyCoder;
        private final Coder<V> valueCoder;
        private final Coder<W> windowCoder;

        private MultimapSideInputHandler(
                Multimap<SideInputKey, V> collection,
                Coder<K> keyCoder,
                Coder<V> valueCoder,
                Coder<W> windowCoder) {
            this.collection = collection;
            this.keyCoder = keyCoder;
            this.valueCoder = valueCoder;
            this.windowCoder = windowCoder;
        }

        @Override
        public Iterable<V> get(byte[] keyBytes, W window) {
            K key;
            try {
                // TODO: We could skip decoding and just compare encoded values for deterministic keyCoders.
                key = keyCoder.decode(new ByteArrayInputStream(keyBytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return collection.get(new SideInputKey(keyCoder.structuralValue(key), windowCoder.structuralValue(window)));
        }

        @Override
        public Coder<V> resultCoder() {
            return valueCoder;
        }
    }

    private static class SideInputKey {

        private final Object key;
        private final Object window;

        SideInputKey(Object key, Object window) {
            this.key = key;
            this.window = window;
        }

        public Object getKey() {
            return key;
        }

        public Object getWindow() {
            return window;
        }
    }
}
