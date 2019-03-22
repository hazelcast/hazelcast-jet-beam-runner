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

package com.hazelcast.jet.beam.metrics;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.core.Processor;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class JetMetricsContainer implements MetricsContainer {

    public static String toStepName(String ownerId, int globalIndex) {
        return ownerId + "/" + globalIndex;
    }

    public static String ownerIdFromStepName(String stepName) {
        return stepName.substring(0, stepName.indexOf('/'));
    }

    public static final String METRICS_ACCUMULATOR_NAME = "metrics"; //todo: should be unique for the current pipeline, I guess

    private final String stepName;

    private final Map<MetricName, CounterImpl> counters = new HashMap<>();
    private final Map<MetricName, DistributionImpl> distributions = new HashMap<>();
    private final Map<MetricName, GaugeImpl> gauges = new HashMap<>();

    private final IMapJet<String, MetricUpdates> accumulator;

    public JetMetricsContainer(String ownerId, Processor.Context context) {
        this.stepName = toStepName(ownerId, context.globalProcessorIndex());
        this.accumulator = context.jetInstance().getMap(METRICS_ACCUMULATOR_NAME);
    }

    @Override
    public Counter getCounter(MetricName metricName) {
        return counters.computeIfAbsent(metricName, CounterImpl::new);
    }

    @Override
    public Distribution getDistribution(MetricName metricName) {
        return distributions.computeIfAbsent(metricName, DistributionImpl::new);
    }

    @Override
    public Gauge getGauge(MetricName metricName) {
        return gauges.computeIfAbsent(metricName, GaugeImpl::new);
    }

    public void flush() {
        MetricUpdates updates = new MetricUpdatesImpl(
                extractUpdates(counters), extractUpdates(distributions), extractUpdates(gauges)
        );
        accumulator.put(stepName, updates);
    }

    private <UpdateT, CellT extends AbstractMetric<UpdateT>> ImmutableList<MetricUpdates.MetricUpdate<UpdateT>> extractUpdates(Map<MetricName, CellT> cells) {
        ImmutableList.Builder<MetricUpdates.MetricUpdate<UpdateT>> updates = ImmutableList.builder();
        for (CellT cell : cells.values()) {
            MetricUpdates.MetricUpdate<UpdateT> update = MetricUpdates.MetricUpdate.create(
                    MetricKey.create(stepName, cell.getName()),
                    cell.getValue()
            );
            updates.add(update);
        }
        return updates.build();
    }

    private static class MetricUpdatesImpl extends MetricUpdates implements Serializable {

        private final Iterable<MetricUpdate<Long>> counters;
        private final Iterable<MetricUpdate<DistributionData>> distributions;
        private final Iterable<MetricUpdate<GaugeData>> gauges;

        public MetricUpdatesImpl(Iterable<MetricUpdate<Long>> counters, Iterable<MetricUpdate<DistributionData>> distributions, Iterable<MetricUpdate<GaugeData>> gauges) {
            this.counters = counters;
            this.distributions = distributions;
            this.gauges = gauges;
        }

        @Override
        public Iterable<MetricUpdate<Long>> counterUpdates() {
            return counters;
        }

        @Override
        public Iterable<MetricUpdate<DistributionData>> distributionUpdates() {
            return distributions;
        }

        @Override
        public Iterable<MetricUpdate<GaugeData>> gaugeUpdates() {
            return gauges;
        }
    }
}
