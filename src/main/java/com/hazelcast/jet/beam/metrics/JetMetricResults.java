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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapClearedListener;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.FluentIterable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class JetMetricResults extends MetricResults implements EntryAddedListener<String, MetricUpdates>, MapClearedListener {

    private final Counters counters = new Counters();
    private final Distributions distributions = new Distributions();
    private final Gauges gauges = new Gauges();

    @Override
    public void entryAdded(EntryEvent<String, MetricUpdates> event) {
        MetricUpdates metricUpdates = event.getValue();
        counters.merge(metricUpdates.counterUpdates());
        distributions.merge(metricUpdates.distributionUpdates());
        gauges.merge(metricUpdates.gaugeUpdates());
    }

    @Override
    public void mapCleared(MapEvent mapEvent) {
        counters.clear();
        distributions.clear();
        gauges.clear();
    }

    @Override
    public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
        return new QueryResults(filter);
    }

    private static MetricKey normalizeStepName(MetricKey key) {
        return MetricKey.create(
                JetMetricsContainer.ownerIdFromStepName(key.stepName()),
                key.metricName()
        );
    }

    private static Predicate<Map.Entry<MetricKey, ?>> matchesFilter(final MetricsFilter filter) {
        return entry -> MetricFiltering.matches(filter, entry.getKey());
    }

    private class QueryResults extends MetricQueryResults {
        private final MetricsFilter filter;

        private QueryResults(MetricsFilter filter) {
            this.filter = filter;
        }

        @Override
        public Iterable<MetricResult<Long>> getCounters() {
            return counters.filter(filter);
        }

        @Override
        public Iterable<MetricResult<DistributionResult>> getDistributions() {
            return distributions.filter(filter);
        }

        @Override
        public Iterable<MetricResult<GaugeResult>> getGauges() {
            return gauges.filter(filter);
        }
    }

    /**
     * Thread-safe storage of gathered counters
     */
    private static class Counters {

        private final Map<MetricKey, Long> counters = new HashMap<>();

        synchronized void merge(Iterable<MetricUpdate<Long>> updates) {
            for (MetricUpdate<Long> update : updates) {
                MetricKey key = normalizeStepName(update.getKey());
                Long oldValue = counters.getOrDefault(key, 0L);
                Long updatedValue = oldValue + update.getUpdate();
                counters.put(key, updatedValue);
            }
        }

        synchronized void clear() {
            counters.clear();
        }

        synchronized Iterable<MetricResult<Long>> filter(MetricsFilter filter) {
            return FluentIterable.from(counters.entrySet())
                    .filter(matchesFilter(filter))
                    .transform(this::toUpdateResult)
                    .toList();
        }

        private MetricResult<Long> toUpdateResult(Map.Entry<MetricKey, Long> entry) {
            MetricKey key = entry.getKey();
            Long counter = entry.getValue();
            return MetricResult.create(key, counter, counter);
        }

    }

    /**
     * Thread-safe storage of gathered distributions
     */
    private static class Distributions {

        private final Map<MetricKey, DistributionData> distributions = new HashMap<>();

        synchronized void merge(Iterable<MetricUpdate<DistributionData>> updates) {
            for (MetricUpdate<DistributionData> update : updates) {
                MetricKey key = normalizeStepName(update.getKey());
                DistributionData oldDistribution = distributions.getOrDefault(key, DistributionData.EMPTY);
                DistributionData updatedDistribution = update.getUpdate().combine(oldDistribution);
                distributions.put(key, updatedDistribution);
            }
        }

        synchronized void clear() {
            distributions.clear();
        }

        synchronized Iterable<MetricResult<DistributionResult>> filter(MetricsFilter filter) {
            return FluentIterable.from(distributions.entrySet())
                    .filter(matchesFilter(filter))
                    .transform(this::toUpdateResult)
                    .toList();
        }

        private MetricResult<DistributionResult> toUpdateResult(Map.Entry<MetricKey, DistributionData> entry) {
            MetricKey key = entry.getKey();
            DistributionResult distributionResult = entry.getValue().extractResult();
            return MetricResult.create(key, distributionResult, distributionResult);
        }

    }

    /**
     * Thread-safe storage of gathered gauges
     */
    private static class Gauges {

        private final Map<MetricKey, GaugeData> gauges = new HashMap<>();

        synchronized void merge(Iterable<MetricUpdate<GaugeData>> updates) {
            for (MetricUpdate<GaugeData> update : updates) {
                MetricKey key = normalizeStepName(update.getKey());
                GaugeData oldGauge = gauges.getOrDefault(key, GaugeData.empty());
                GaugeData updatedGauge = update.getUpdate().combine(oldGauge);
                gauges.put(key, updatedGauge);
            }
        }

        synchronized void clear() {
            gauges.clear();
        }

        synchronized Iterable<MetricResult<GaugeResult>> filter(MetricsFilter filter) {
            return FluentIterable.from(gauges.entrySet())
                    .filter(matchesFilter(filter))
                    .transform(this::toUpdateResult)
                    .toList();
        }

        private MetricResult<GaugeResult> toUpdateResult(Map.Entry<MetricKey, GaugeData> entry) {
            MetricKey key = entry.getKey();
            GaugeResult gaugeResult = entry.getValue().extractResult();
            return MetricResult.create(key, gaugeResult, gaugeResult);
        }

    }
}
