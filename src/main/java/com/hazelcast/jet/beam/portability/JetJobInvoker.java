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

import com.hazelcast.jet.beam.JetPipelineOptions;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.UUID;

class JetJobInvoker implements JobInvoker {

    private static final Logger LOG = LoggerFactory.getLogger(JetJobInvoker.class);

    private final ListeningExecutorService executor;
    private final JetJobServerDriver.ServerConfiguration configuration;

    JetJobInvoker(ListeningExecutorService executor, JetJobServerDriver.ServerConfiguration configuration) {
        this.executor = executor;
        this.configuration = configuration;
    }

    @Override
    public JobInvocation invoke(RunnerApi.Pipeline pipeline, Struct options, @Nullable String retrievalToken) {
        LOG.trace("Parsing pipeline options");
        JetPipelineOptions jetPipelineOptions = PipelineOptionsTranslation.fromProto(options).as(JetPipelineOptions.class);

        String invocationId = String.format("%s_%s", jetPipelineOptions.getJobName(), UUID.randomUUID().toString());
        LOG.info("Invoking job {}", invocationId);

        PortablePipelineOptions portableOptions = jetPipelineOptions.as(PortablePipelineOptions.class);
        if (portableOptions.getSdkWorkerParallelism() == null) {
            portableOptions.setSdkWorkerParallelism(configuration.getSdkWorkerParallelism());
        }

        return new JetJobInvocation(invocationId, retrievalToken, executor, pipeline, jetPipelineOptions);
    }
}
