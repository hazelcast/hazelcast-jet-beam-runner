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
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ExternalEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

class JetStageBundleFactories {

    //todo: this is basically a hack, but ExecuteStageP needs instances, they are not serializable, so I saw no other way...

    private static final Map<String, DefaultJobBundleFactory> INSTANCES = new HashMap<>();

    static StageBundleFactory get(JobInfo jobInfo, ExecutableStage executableStage) {
        synchronized (INSTANCES) {
            String jobId = jobInfo.jobId();
            DefaultJobBundleFactory jobBundleFactory = INSTANCES.computeIfAbsent(jobId, id -> initJobBundleFactory(jobInfo));
            return jobBundleFactory.forStage(executableStage);
        }
    }

    private static DefaultJobBundleFactory initJobBundleFactory(JobInfo jobInfo) {
        ImmutableMap.Builder<String, EnvironmentFactory.Provider> environmentFactoryProviderMap = ImmutableMap.builder();
        environmentFactoryProviderMap.put(
                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER),
                new DockerEnvironmentFactory.Provider(PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions()))
        );
        environmentFactoryProviderMap.put(
                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.PROCESS),
                new ProcessEnvironmentFactory.Provider()
        );
        environmentFactoryProviderMap.put(
                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.EXTERNAL),
                new ExternalEnvironmentFactory.Provider()
        );
        environmentFactoryProviderMap.put(
                Environments.ENVIRONMENT_EMBEDDED,
                new EmbeddedEnvironmentFactory.Provider(PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions()))
        );
        return DefaultJobBundleFactory.create(jobInfo, environmentFactoryProviderMap.build());
    }

    static void close(JobInfo jobInfo) {
        synchronized (INSTANCES) {
            DefaultJobBundleFactory factory = INSTANCES.remove(jobInfo);
            if (factory != null) {
                try {
                    factory.close();
                } catch (Exception e) {
                    throw new RuntimeException("Error while closing: ", e);
                }
            }
        }
    }
}
