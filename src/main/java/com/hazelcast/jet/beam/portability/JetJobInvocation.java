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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.beam.JetPipelineOptions;
import com.hazelcast.jet.beam.JetPipelineResult;
import com.hazelcast.jet.beam.metrics.JetMetricsContainer;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables.getRootCause;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables.getStackTraceAsString;

public class JetJobInvocation implements JobInvocation {

    private static final Logger LOG = LoggerFactory.getLogger(JetJobInvocation.class);

    private final String id;
    private final ListeningExecutorService executorService;
    private final RunnerApi.Pipeline pipeline;
    private final JetTranslationPortabilityContext translationContext;
    private final List<Consumer<JobState.Enum>> stateObservers = new ArrayList<>(); //todo: when should it be cleaned up?
    private final List<Consumer<JobMessage>> messageObservers = new ArrayList<>(); //todo: when should it be cleaned up?

    private JobState.Enum jobState;
    private ListenableFuture<PipelineResult> invocationFuture;

    JetJobInvocation(
            String id,
            String retrievalToken,
            ListeningExecutorService executorService,
            RunnerApi.Pipeline pipeline,
            JetPipelineOptions pipelineOptions
    ) {
        this.id = id;
        this.executorService = executorService;
        this.pipeline = pipeline;
        this.translationContext = new JetTranslationPortabilityContext(
                pipelineOptions,
                JobInfo.create(
                        id,
                        pipelineOptions.getJobName(),
                        retrievalToken,
                        PipelineOptionsTranslation.toProto(pipelineOptions)
                )
        );
        this.invocationFuture = null;
        this.jobState = JobState.Enum.STOPPED;
    }

    @Override
    public synchronized void start() {
        LOG.info("Starting job invocation {}", getId());
        if (getState() != JobState.Enum.STOPPED) {
            throw new IllegalStateException(String.format("Job %s already running.", getId()));
        }
        setState(JobState.Enum.STARTING);
        invocationFuture = executorService.submit(this::runPipeline);
        // TODO: Defer transitioning until the pipeline is up and running.
        setState(JobState.Enum.RUNNING);
        Futures.addCallback(
                invocationFuture,
                new FutureCallback<PipelineResult>() {
                    @Override
                    public void onSuccess(@Nullable PipelineResult pipelineResult) {
                        if (pipelineResult != null) {
                            checkArgument(
                                    pipelineResult.getState() == PipelineResult.State.DONE,
                                    "Success on non-Done state: " + pipelineResult.getState());
                            setState(JobState.Enum.DONE);
                        } else {
                            setState(JobState.Enum.UNSPECIFIED);
                        }
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        String message = String.format("Error during job invocation %s.", getId());
                        LOG.error(message, throwable);
                        sendMessage(
                                JobMessage.newBuilder()
                                        .setMessageText(getStackTraceAsString(throwable))
                                        .setImportance(JobMessage.MessageImportance.JOB_MESSAGE_DEBUG)
                                        .build());
                        sendMessage(
                                JobMessage.newBuilder()
                                        .setMessageText(getRootCause(throwable).toString())
                                        .setImportance(JobMessage.MessageImportance.JOB_MESSAGE_ERROR)
                                        .build());
                        setState(JobState.Enum.FAILED);
                    }
                },
                executorService
        );
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public synchronized void cancel() {
        LOG.info("Canceling job invocation {}", getId());
        if (this.invocationFuture != null) {
            this.invocationFuture.cancel(true /* mayInterruptIfRunning */);
            Futures.addCallback(
                    invocationFuture,
                    new FutureCallback<PipelineResult>() {
                        @Override
                        public void onSuccess(@Nullable PipelineResult pipelineResult) {
                            if (pipelineResult != null) {
                                try {
                                    pipelineResult.cancel();
                                } catch (IOException exn) {
                                    throw new RuntimeException(exn);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                        }
                    },
                    executorService
            );
        }
    }

    @Override
    public JobState.Enum getState() {
        return jobState;
    }

    @Override
    public synchronized void addStateListener(Consumer<JobState.Enum> stateStreamObserver) {
        stateStreamObserver.accept(getState());
        stateObservers.add(stateStreamObserver);
    }

    @Override
    public synchronized void addMessageListener(Consumer<JobMessage> messageStreamObserver) {
        messageObservers.add(messageStreamObserver);
    }

    private synchronized void setState(JobState.Enum state) {
        this.jobState = state;
        for (Consumer<JobState.Enum> observer : stateObservers) {
            observer.accept(state);
        }
    }

    private synchronized void sendMessage(JobMessage message) {
        for (Consumer<JobMessage> observer : messageObservers) {
            observer.accept(message);
        }
    }

    private PipelineResult runPipeline() {
        DAG dag = translate(pipeline);
        return run(dag);
    }

    private DAG translate(RunnerApi.Pipeline pipeline) {
        boolean isPipelineAlreadyFused = pipeline.getComponents().getTransformsMap().values().stream()
                .anyMatch(proto -> ExecutableStage.URN.equals(proto.getSpec().getUrn()));
        RunnerApi.Pipeline fusedPipeline = isPipelineAlreadyFused ? pipeline : GreedyPipelineFuser.fuse(pipeline).toPipeline();

        QueryablePipeline p = QueryablePipeline.forTransforms(fusedPipeline.getRootTransformIdsList(), fusedPipeline.getComponents());
        for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
            PTransformTranslators.translate(transform, fusedPipeline, translationContext);
        }

        return translationContext.getDagBuilder().getDag();
    }

    private JetPipelineResult run(DAG dag) {
        JetInstance jet = getJetInstance();

        IMapJet<String, MetricUpdates> metricsAccumulator = jet.getMap(JetMetricsContainer.METRICS_ACCUMULATOR_NAME);
        Job job = jet.newJob(dag);

        JetPipelineResult result = new JetPipelineResult(metricsAccumulator);
        result.setJob(job);

        job.join();
        result.setJob(null);
        job.cancel();
        jet.shutdown();

        return result;
    }

    private JetInstance getJetInstance() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(JetConfig.DEFAULT_GROUP_NAME);
        return Jet.newJetClient(clientConfig);
    }
}
