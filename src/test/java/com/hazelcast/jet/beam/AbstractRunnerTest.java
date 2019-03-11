package com.hazelcast.jet.beam;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JetConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.GsonBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

public abstract class AbstractRunnerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10); // 10 seconds max per method tested

    @Rule
    public TestPipeline p = getTestPipeline();

    private static TestPipeline getTestPipeline() {
        System.setProperty(
                TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
                new GsonBuilder().create().toJson(
                        new String[]{
                                "--runner=JetRunner",
                                "--jetGroupName=" + JetConfig.DEFAULT_GROUP_NAME,
                                "--jetGroupPassword=" + JetConfig.DEFAULT_GROUP_PASSWORD
                        }
                )
        );
        return TestPipeline.create();
    }

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setProperty("hazelcast.logging.type", "log4j");

        Jet.newJetInstance(config);
    }

    @After
    public void after() {
        Jet.shutdownAll();
    }

    protected static class FormatLongAsTextFn extends SimpleFunction<Long, String> {
        @Override
        public String apply(Long input) {
            return Long.toString(input);
        }
    }

    protected static class FormatKVAsTextFn extends SimpleFunction<KV<?, ?>, String> {
        @Override
        public String apply(KV<?, ?> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

}
