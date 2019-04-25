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

public class TestJetRunner extends PipelineRunner<PipelineResult> {

    private final JetTestInstanceFactory factory = new JetTestInstanceFactory();

    public static TestJetRunner fromOptions(PipelineOptions options) {
        return new TestJetRunner(options);
    }

    private final JetRunner delegate;

    private TestJetRunner(PipelineOptions options) {
        this.delegate = JetRunner.fromOptions(options, factory::newClient);
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        Collection<JetInstance> instances = initInstances(factory);
        System.out.println("Created " + instances.size() + " instances.");
        try {
            PipelineResult result = delegate.run(pipeline);
            if (result instanceof FailedRunningPipelineResults) {
                RuntimeException failureCause = ((FailedRunningPipelineResults) result).getCause();
                throw failureCause;
            }
            return result;
        } finally {
            System.out.println("Shutting down " + instances.size() + " instances...");
            factory.shutdownAll();
        }
    }

    private Collection<JetInstance> initInstances(JetTestInstanceFactory factory) {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("map"));

        return  Arrays.asList(
                factory.newMember(config),
                factory.newMember(config)
        );
    }

}
