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

import com.hazelcast.jet.beam.DAGBuilder;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExecuteStageP implements Processor {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteStageP.class);

    private final RunnerApi.ExecutableStagePayload stagePayload;
    private final JobInfo jobInfo;
    private final Map<String, int[]> outputCollToOrdinals;
    private final String ownerId; //do not remove, useful for debugging

    private transient OutputHandler outputHandler;
    private transient ExecutableStage executableStage;
    private transient StageBundleFactory stageBundleFactory;
    private transient StateRequestHandler stateRequestHandler;
    private transient BundleProgressHandler progressHandler;

    private ExecuteStageP(
            RunnerApi.ExecutableStagePayload stagePayload,
            JobInfo jobInfo,
            Map<String, int[]> outputCollToOrdinals,
            String ownerId
    ) {
        this.stagePayload = stagePayload;
        this.jobInfo = jobInfo;
        this.outputCollToOrdinals = outputCollToOrdinals;
        this.ownerId = ownerId;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        outputHandler = new OutputHandler(outbox, outputCollToOrdinals);

        executableStage = ExecutableStage.fromPayload(stagePayload);
        stageBundleFactory = JetStageBundleFactories.get(jobInfo, executableStage);
        stateRequestHandler = getStateRequestHandler(executableStage, stageBundleFactory.getProcessBundleDescriptor());

        progressHandler = new BundleProgressHandler() {
            @Override
            public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {
                //todo: update metrics
            }

            @Override
            public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
                //todo: update metrics
                outputHandler.tryFlush();
            }
        };

        //System.out.println(ExecuteStageP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
        //if (ownerId.startsWith("8 ")) System.out.println(ExecuteStageP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful for debugging
    }

    private StateRequestHandler getStateRequestHandler(
            ExecutableStage executableStage,
            ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor
    ) {
        StateRequestHandler sideInputHandler;
        StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory = JetSideInputHandleFactory.forStage(executableStage);
        try {
            sideInputHandler = StateRequestHandlers.forSideInputHandlerFactory(
                    ProcessBundleDescriptors.getSideInputs(executableStage),
                    sideInputHandlerFactory
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to setup state handler", e);
        }

        final StateRequestHandler userStateHandler;
        if (executableStage.getUserStates().size() > 0) {
            throw new UnsupportedOperationException(); //todo: support user state?
        } else {
            userStateHandler = StateRequestHandler.unsupported();
        }

        EnumMap<BeamFnApi.StateKey.TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(BeamFnApi.StateKey.TypeCase.class);
        handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);
        handlerMap.put(BeamFnApi.StateKey.TypeCase.BAG_USER_STATE, userStateHandler);

        return StateRequestHandlers.delegateBasedUponType(handlerMap);
    }

    @Override
    public boolean isCooperative() {
        return false; //todo: is this ok?
    }

    @Override
    public void close() {
        outputHandler = null;
        executableStage = null;
        JetStageBundleFactories.close(jobInfo);
        stageBundleFactory = close(stageBundleFactory);
        stateRequestHandler = null;
        progressHandler = null;
    }

    private static <T extends AutoCloseable> T close(T t) {
        if (t == null) return null;

        try {
            t.close();
            return null;
        } catch (Exception e) {
            LOG.error("Error while closing: ", e);
            throw new RuntimeException("Error while closing: ", e);
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!outputHandler.tryFlush()) {
            // don't process more items until outputManager is empty
            return;
        }

        Collection<WindowedValue> inputElements = new ArrayList<>();
        inbox.drainTo(inputElements);

        try (RemoteBundle bundle = stageBundleFactory.getBundle(outputHandler, stateRequestHandler, progressHandler)) {
            processElements(inputElements, bundle);
        } catch (Exception e) {
            throw new RuntimeException("Oops!", e); //todo: better handling
        }
    }

    private void processElements(Iterable<WindowedValue> iterable, RemoteBundle bundle) throws Exception {
        String inputPCollectionId = executableStage.getInputPCollection().getId();
        FnDataReceiver<WindowedValue<?>> mainReceiver = bundle.getInputReceivers().get(inputPCollectionId);
        if (mainReceiver == null) throw new RuntimeException("Main input receiver for [" + inputPCollectionId + "] could not be initialized");
        for (WindowedValue input : iterable) {
            mainReceiver.accept(input);
        }
    }

    @Override
    public boolean tryProcess() {
        return outputHandler.tryFlush();
    }

    @Override
    public boolean complete() {
        //System.out.println(ExecutionStageP.class.getSimpleName() + " COMPLETE ownerId = " + ownerId); //useful for debugging
        //if (ownerId.startsWith("8 ")) System.out.println(ExecutionStageP.class.getSimpleName() + " COMPLETE ownerId = " + ownerId); //useful for debugging
        return outputHandler.tryFlush();
    }

    public static class Supplier implements SupplierEx<Processor>, DAGBuilder.WiringListener {

        private final RunnerApi.ExecutableStagePayload stagePayload;
        private final JobInfo jobInfo;
        private final Map<String, List<Integer>> outputCollToOrdinals;
        private final String ownerId;

        Supplier(RunnerApi.ExecutableStagePayload stagePayload, JobInfo jobInfo, Collection<String> outputs, String ownerId) {
            this.stagePayload = stagePayload;
            this.jobInfo = jobInfo;
            this.outputCollToOrdinals = outputs.stream().collect(Collectors.toMap(Function.identity(), t -> new ArrayList<>()));
            this.ownerId = ownerId;
        }

        @Override
        public Processor getEx() {
            return new ExecuteStageP(
                    stagePayload,
                    jobInfo,
                    outputCollToOrdinals.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().mapToInt(i -> i).toArray())),
                    ownerId
            );
        }

        @Override
        public void isOutboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId) {
            if (ownerId.equals(vertexId)) {
                List<Integer> ordinals = outputCollToOrdinals.get(pCollId);
                if (ordinals == null) throw new RuntimeException("Oops"); //todo

                ordinals.add(edge.getSourceOrdinal());
            }
        }

        @Override
        public void isInboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId) {
            //do nothin
        }
    }

    private static class OutputHandler implements OutputReceiverFactory {

        private final Outbox outbox;
        private final List<Object>[] outputBuckets;
        private final Map<String, PCollectionReceiver> outputReceivers;

        // the flush position to continue flushing to outbox
        private int currentBucket, currentItem;

        OutputHandler(Outbox outbox, Map<String, int[]> outputCollToOrdinals) {
            this.outbox = outbox;
            this.outputBuckets = initOutputBuckets(outputCollToOrdinals);
            this.outputReceivers = initOutputReceivers(outputCollToOrdinals, outputBuckets);
        }

        @Override
        public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
            PCollectionReceiver pCollectionReceiver = outputReceivers.get(pCollectionId);
            if (pCollectionReceiver == null) throw new RuntimeException("Oops!");
            return pCollectionReceiver;
        }

        boolean tryFlush() {
            synchronized (outputBuckets) { //todo: we will need to do something smarter here, at least synchronize per individual bucket
                for (; currentBucket < outputBuckets.length; currentBucket++) {
                    List bucket = outputBuckets[currentBucket];
                    for (; currentItem < bucket.size(); currentItem++) {
                        if (!outbox.offer(currentBucket, bucket.get(currentItem))) {
                            return false;
                        }
                    }
                    bucket.clear();
                    currentItem = 0;
                }
                currentBucket = 0;
                int sum = 0;
                for (List outputBucket : outputBuckets) {
                    sum += outputBucket.size();
                }
                return sum == 0;
            }
        }

        private static List<Object>[] initOutputBuckets(Map<String, int[]> outputCollToOrdinals) {
            int maxOrdinal = outputCollToOrdinals.values().stream().flatMapToInt(IntStream::of).max().orElse(-1);
            List<Object>[] outputBuckets = new List[maxOrdinal + 1];
            Arrays.setAll(outputBuckets, i -> new ArrayList<>());
            return outputBuckets;
        }

        private static Map<String, PCollectionReceiver> initOutputReceivers(Map<String, int[]> outputCollToOrdinals, List<Object>[] outputBuckets) {
            return outputCollToOrdinals.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> new PCollectionReceiver(e.getValue(), outputBuckets)
                            )
                    );
        }

    }

    private static class PCollectionReceiver implements FnDataReceiver {

        private final int[] ordinals;
        private final List<Object>[] outputBuckets;

        PCollectionReceiver(int[] ordinals, List<Object>[] outputBuckets) {
            this.ordinals = ordinals;
            this.outputBuckets = outputBuckets;
        }

        @Override
        public void accept(Object output) {
            synchronized (outputBuckets) { //todo: we will need to do something smarter here, at least synchronize per individual bucket
                for (int ordinal : ordinals) {
                    outputBuckets[ordinal].add(output);
                }
            }
        }
    }
}
