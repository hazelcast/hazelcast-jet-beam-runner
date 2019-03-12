package com.hazelcast.jet.beam;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.JetInstance;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.function.Function;

public class TestJetRunner extends PipelineRunner<JetPipelineResult> {

    public static Function<ClientConfig, JetInstance> JET_CLIENT_SUPPLIER = null;

    public static TestJetRunner fromOptions(PipelineOptions options) {
        return new TestJetRunner(options);
    }

    private final JetRunner delegate;

    private TestJetRunner(PipelineOptions options) {
        this.delegate = JetRunner.fromOptions(options, JET_CLIENT_SUPPLIER);
    }

    @Override
    public JetPipelineResult run(Pipeline pipeline) {
        return delegate.run(pipeline);
    }
}
