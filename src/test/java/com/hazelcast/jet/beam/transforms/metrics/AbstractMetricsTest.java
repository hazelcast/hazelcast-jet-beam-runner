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

package com.hazelcast.jet.beam.transforms.metrics;

import com.hazelcast.jet.beam.transforms.AbstractTransformTest;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.distributionMinMax;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.metricsResult;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

abstract class AbstractMetricsTest extends AbstractTransformTest {

    private static final String NAMESPACE = MetricsTest.class.getName();

    static void assertAllMetrics(MetricQueryResults metrics, boolean isCommitted) {
        assertCounterMetrics(metrics, isCommitted);
        assertDistributionMetrics(metrics, isCommitted);
        assertGaugeMetrics(metrics, isCommitted);
    }

    static void assertCounterMetrics(MetricQueryResults metrics, boolean isCommitted) {
        Iterable<MetricResult<Long>> counters = metrics.getCounters();
        assertThat(
                counters,
                hasItem(metricsResult(NAMESPACE, "count", "MyStep1", 3L, isCommitted)));
        assertThat(
                counters,
                hasItem(metricsResult(NAMESPACE, "count", "MyStep2", 6L, isCommitted)));
    }

    static void assertGaugeMetrics(MetricQueryResults metrics, boolean isCommitted) {
        Iterable<MetricResult<GaugeResult>> gauges = metrics.getGauges();
        assertThat(
                gauges,
                hasItem(
                        metricsResult(
                                NAMESPACE,
                                "my-gauge",
                                "MyStep2",
                                GaugeResult.create(12L, Instant.now()),
                                isCommitted)));
    }

    static void assertDistributionMetrics(MetricQueryResults metrics, boolean isCommitted) {
        Iterable<MetricResult<DistributionResult>> distributions = metrics.getDistributions();
        assertThat(
                distributions,
                hasItem(
                        metricsResult(
                                NAMESPACE,
                                "input",
                                "MyStep1",
                                DistributionResult.create(26L, 3L, 5L, 13L),
                                isCommitted)));

        assertThat(
                distributions,
                hasItem(
                        metricsResult(
                                NAMESPACE,
                                "input",
                                "MyStep2",
                                DistributionResult.create(52L, 6L, 5L, 13L),
                                isCommitted)));
        assertThat(
                distributions,
                hasItem(distributionMinMax(NAMESPACE, "bundle", "MyStep1", 10L, 40L, isCommitted)));
    }

    PipelineResult runPipelineWithMetrics() {
        final Counter count = Metrics.counter(MetricsTest.class, "count");
        final TupleTag<Integer> output1 = new TupleTag<Integer>() {};
        final TupleTag<Integer> output2 = new TupleTag<Integer>() {};
        pipeline
                .apply(Create.of(5, 8, 13))
                .apply(
                        "MyStep1",
                        ParDo.of(
                                new DoFn<Integer, Integer>() {
                                    Distribution bundleDist = Metrics.distribution(MetricsTest.class, "bundle");

                                    @StartBundle
                                    public void startBundle() {
                                        bundleDist.update(10L);
                                    }

                                    @SuppressWarnings("unused")
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        Distribution values = Metrics.distribution(MetricsTest.class, "input");
                                        count.inc();
                                        values.update(c.element());

                                        c.output(c.element());
                                        c.output(c.element());
                                    }

                                    @DoFn.FinishBundle
                                    public void finishBundle() {
                                        bundleDist.update(40L);
                                    }
                                }))
                .apply(
                        "MyStep2",
                        ParDo.of(
                                new DoFn<Integer, Integer>() {
                                    @SuppressWarnings("unused")
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        Distribution values = Metrics.distribution(MetricsTest.class, "input");
                                        Gauge gauge = Metrics.gauge(MetricsTest.class, "my-gauge");
                                        Integer element = c.element();
                                        count.inc();
                                        values.update(element);
                                        gauge.set(12L);
                                        c.output(element);
                                        c.output(output2, element);
                                    }
                                })
                                .withOutputTags(output1, TupleTagList.of(output2)));
        PipelineResult result = pipeline.run();

        result.waitUntilFinish();
        return result;
    }

    static MetricQueryResults queryTestMetrics(PipelineResult result) {
        return result
                .metrics()
                .queryMetrics(
                        MetricsFilter.builder()
                                .addNameFilter(MetricNameFilter.inNamespace(MetricsTest.class))
                                .build());
    }
}
