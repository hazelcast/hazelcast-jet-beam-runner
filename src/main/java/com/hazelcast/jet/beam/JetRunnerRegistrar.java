package com.hazelcast.jet.beam;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/**
 * Contains the {@link PipelineRunnerRegistrar} and {@link PipelineOptionsRegistrar} for the {@link
 * JetRunner}.
 *
 * <p>{@link AutoService} will register Apex's implementations of the {@link PipelineRunner} and
 * {@link PipelineOptions} as available pipeline runner services.
 */
public final class JetRunnerRegistrar {
    private JetRunnerRegistrar() {}

    /** Registers the {@link JetRunner}. */
    @AutoService(PipelineRunnerRegistrar.class)
    public static class Runner implements PipelineRunnerRegistrar {
        @Override
        public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
            return ImmutableList.of(JetRunner.class);
        }
    }

    /** Registers the {@link JetRunnerOptions}. */
    @AutoService(PipelineOptionsRegistrar.class)
    public static class Options implements PipelineOptionsRegistrar {
        @Override
        public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
            return ImmutableList.of(JetRunnerOptions.class);
        }
    }
}
