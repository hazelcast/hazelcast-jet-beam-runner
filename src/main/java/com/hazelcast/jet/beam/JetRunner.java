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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.beam.metrics.JetMetricsContainer;
import com.hazelcast.jet.core.DAG;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class JetRunner extends PipelineRunner<JetPipelineResult> {

    public static JetRunner fromOptions(PipelineOptions options) {
        return fromOptions(options, Jet::newJetClient);
    }

    public static JetRunner fromOptions(PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
        return new JetRunner(options, jetClientSupplier);
    }

    private final JetRunnerOptions options;
    private final Function<ClientConfig, JetInstance> jetClientSupplier;

    private JetRunner(PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
        this.options = validate(options.as(JetRunnerOptions.class));
        this.jetClientSupplier = jetClientSupplier;
    }

    public JetPipelineResult run(Pipeline pipeline) {
        normalize(pipeline);
        DAG dag = translate(pipeline);
        return run(dag);
    }

    private void normalize(Pipeline pipeline) {
        pipeline.replaceAll(getDefaultOverrides());
        UnconsumedReads.ensureAllReadsConsumed(pipeline);
    }

    private DAG translate(Pipeline pipeline) {
        /*PrintGraphVisitor printVisitor = new PrintGraphVisitor();
        pipeline.traverseTopologically(printVisitor);
        System.out.println("Beam pipeline:" + printVisitor.print()); //todo: remove*/

        /*PrintFullGraphVisitor printFullVisitor = new PrintFullGraphVisitor();
        pipeline.traverseTopologically(printFullVisitor);
        System.out.println("Beam pipeline:" + printFullVisitor.print()); //todo: remove*/

        JetGraphVisitor graphVisitor = new JetGraphVisitor(options);
        pipeline.traverseTopologically(graphVisitor);
        return graphVisitor.getDAG();
    }

    private JetPipelineResult run(DAG dag) {
        JetInstance jet = getJetInstance(options);
        IMapJet<String, MetricUpdates> metricsAccumulator = jet.getMap(JetMetricsContainer.METRICS_ACCUMULATOR_NAME);
        Job job = jet.newJob(dag);
        JetPipelineResult result = new JetPipelineResult(job, metricsAccumulator);
        job.join();
        return result;
    }

    private JetInstance getJetInstance(JetRunnerOptions options) {
        String jetGroupName = options.getJetGroupName();
        String jetGroupPassword = options.getJetGroupPassword();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(jetGroupName);
        clientConfig.getGroupConfig().setPassword(jetGroupPassword);
        return jetClientSupplier.apply(clientConfig);
    }

    private static List<PTransformOverride> getDefaultOverrides() {
        //todo: Combine.globally(SumLongs) is built up from 7 nodes... see Combine.Globally.expand()
        //todo: more replacements?
        return Collections.emptyList();
    }

    private static JetRunnerOptions validate(JetRunnerOptions options) {
        if (options.getJetGroupName() == null) throw new IllegalArgumentException("Jet group NAME not set in options!");
        if (options.getJetGroupPassword() == null) throw new IllegalArgumentException("Jet group PASSWORD not set in options!");
        return options;
    }

}
