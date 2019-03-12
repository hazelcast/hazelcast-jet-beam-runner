package com.hazelcast.jet.beam;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.GsonBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
public abstract class AbstractRunnerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10); // 10 seconds max per method tested

    @Rule
    public TestPipeline pipeline = getTestPipeline();

    private static TestPipeline getTestPipeline() {
        System.setProperty(
                TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
                new GsonBuilder().create().toJson(
                        new String[]{
                                "--runner=TestJetRunner",
                                "--jetGroupName=" + JetConfig.DEFAULT_GROUP_NAME,
                                "--jetGroupPassword=" + JetConfig.DEFAULT_GROUP_PASSWORD
                        }
                )
        );
        return TestPipeline.create();
    }

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    static {
        TestJetRunner.JET_CLIENT_SUPPLIER = factory::newClient;
    }

    private static JetInstance instance;

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setProperty("hazelcast.logging.type", "log4j");
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("map"));
        instance = factory.newMember(config);
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
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
