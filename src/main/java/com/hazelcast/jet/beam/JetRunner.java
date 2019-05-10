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
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class JetRunner extends PipelineRunner<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);

    public static JetRunner fromOptions(PipelineOptions options) {
        return fromOptions(options, Jet::newJetClient);
    }

    public static JetRunner fromOptions(PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
        return new JetRunner(options, jetClientSupplier);
    }

    private final JetPipelineOptions options;
    private final Function<ClientConfig, JetInstance> jetClientSupplier;

    private Function<PTransform<?, ?>,  JetTransformTranslator<?>> translatorProvider;

    private JetRunner(PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
        this.options = validate(options.as(JetPipelineOptions.class));
        this.jetClientSupplier = jetClientSupplier;
        this.translatorProvider = JetTransformTranslators::getTranslator;
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        Boolean startOwnCluster = options.getJetStartOwnCluster();
        if (startOwnCluster) {
            JetInstance jet1 = Jet.newJetInstance();
            JetInstance jet2 = Jet.newJetInstance();
        }
        try {
            return runInternal(pipeline);
        } finally {
            if (startOwnCluster) {
                Jet.shutdownAll();
            }
        }
    }

    void addExtraTranslators(Function<PTransform<?, ?>, JetTransformTranslator<?>> extraTranslatorProvider) {
        Function<PTransform<?, ?>, JetTransformTranslator<?>> initialTranslatorProvider = this.translatorProvider;
        this.translatorProvider = transform -> {
            JetTransformTranslator<?> translator = initialTranslatorProvider.apply(transform);
            if (translator == null) {
                translator = extraTranslatorProvider.apply(transform);
            }
            return translator;
        };
    }

    private PipelineResult runInternal(Pipeline pipeline) {
        try {
            normalize(pipeline);
            DAG dag = translate(pipeline);
            return run(dag);
        } catch (UnsupportedOperationException uoe) {
            LOG.error("Failed running pipeline!", uoe);
            return new FailedRunningPipelineResults(uoe);
        }
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
        System.out.println("Beam pipeline:" + printFullVisitor.print());*/ //todo: remove

        //Set<ExecutableStage> fusedStages = GreedyPipelineFuser.fuse(PipelineTranslation.toProto(pipeline)).getFusedStages();
        //System.out.println("Pipeline fused into " + fusedStages.size() + " stages"); //todo: remove

        JetGraphVisitor graphVisitor = new JetGraphVisitor(options, translatorProvider);
        pipeline.traverseTopologically(graphVisitor);
        return graphVisitor.getDAG();
    }

    private JetPipelineResult run(DAG dag) {
        // todo: we use single client for each job, it might be better to have a shared client with refcount
        JetInstance jet = getJetInstance(options);

        Job job = jet.newJob(dag);
        IMapJet<String, MetricUpdates> metricsAccumulator = jet.getMap(JetMetricsContainer.getMetricsMapName(job.getId()));
        JetPipelineResult pipelineResult = new JetPipelineResult(job, metricsAccumulator);
        job.getFuture().whenComplete(
                (r, f) -> {
                    pipelineResult.freeze(f);
                    metricsAccumulator.destroy();
                    jet.shutdown();
                }
        );

        return pipelineResult;
    }

    private JetInstance getJetInstance(JetPipelineOptions options) {
        String jetGroupName = options.getJetGroupName();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(jetGroupName);
        return jetClientSupplier.apply(clientConfig);
    }

    private static List<PTransformOverride> getDefaultOverrides() {
//        return Collections.singletonList(JavaReadViaImpulse.boundedOverride()); //todo: needed once we start using GreedyPipelineFuser
        return Collections.emptyList();
    }

    private static JetPipelineOptions validate(JetPipelineOptions options) {
        if (options.getJetGroupName() == null) throw new IllegalArgumentException("Jet group name not set in options");

        Integer localParallelism = options.getJetLocalParallelism();
        if (localParallelism == null) throw new IllegalArgumentException("Jet node local parallelism must be specified");
        if (localParallelism != -1 && localParallelism < 1) throw new IllegalArgumentException("Jet node local parallelism must be >1 or -1");

        return options;
    }
}
