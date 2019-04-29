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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class TestJetRunner extends PipelineRunner<PipelineResult> {

    public static JetTestInstanceFactory EXTERNAL_FACTORY;

    private final JetTestInstanceFactory factory;
    private final JetRunner delegate;

    private TestJetRunner(PipelineOptions options) {
        JetPipelineOptions jetPipelineOptions = options.as(JetPipelineOptions.class);
        jetPipelineOptions.setJetStartOwnCluster(false);

        this.factory = EXTERNAL_FACTORY == null ? new JetTestInstanceFactory() : null;
        this.delegate = JetRunner.fromOptions(options, (EXTERNAL_FACTORY == null ? factory : EXTERNAL_FACTORY)::newClient);
    }

    public static TestJetRunner fromOptions(PipelineOptions options) {
        return new TestJetRunner(options);
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        Collection<JetInstance> instances = initMemberInstances(EXTERNAL_FACTORY, factory);
        try {
            PipelineResult result = delegate.run(pipeline);
            if (result instanceof FailedRunningPipelineResults) {
                RuntimeException failureCause = ((FailedRunningPipelineResults) result).getCause();
                throw failureCause;
            }
            return result;
        } finally {
            killMemberInstances(instances, factory);
        }
    }

    private static Collection<JetInstance> initMemberInstances(JetTestInstanceFactory externalFactory, JetTestInstanceFactory internalFactory) {
        if (externalFactory != null) {
            //no need to create own member instances
            return Collections.emptyList();
        }

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("map"));

        return Arrays.asList(
                internalFactory.newMember(config),
                internalFactory.newMember(config)
        );
    }

    private static void killMemberInstances(Collection<JetInstance> instances, JetTestInstanceFactory internalFactory) {
        if (!instances.isEmpty()) {
            //there are own member instances to kill
            internalFactory.shutdownAll();
        }
    }

}
