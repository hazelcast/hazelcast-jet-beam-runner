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

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.junit.Test;

/* "Inspired" by org.apache.beam.sdk.metrics.MetricsTest.CommittedMetricTests */
public class CommittedMetricsTest extends AbstractMetricsTest {

    @Test
    public void testAllCommittedMetrics() {
        PipelineResult result = runPipelineWithMetrics();
        MetricQueryResults metrics = queryTestMetrics(result);

        assertAllMetrics(metrics, true);
    }

    @Test
    public void testCommittedCounterMetrics() {
        PipelineResult result = runPipelineWithMetrics();
        MetricQueryResults metrics = queryTestMetrics(result);
        assertCounterMetrics(metrics, true);
    }

    @Test
    public void testCommittedDistributionMetrics() {
        PipelineResult result = runPipelineWithMetrics();
        MetricQueryResults metrics = queryTestMetrics(result);
        assertDistributionMetrics(metrics, true);
    }

    @Test
    public void testCommittedGaugeMetrics() {
        PipelineResult result = runPipelineWithMetrics();
        MetricQueryResults metrics = queryTestMetrics(result);
        assertGaugeMetrics(metrics, true);
    }

}
