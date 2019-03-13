package com.hazelcast.jet.beam;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.GsonBuilder;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

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

    public static class KvMatcher<K, V> extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
        final Matcher<? super K> keyMatcher;
        final Matcher<? super V> valueMatcher;

        public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
            return new KvMatcher<>(keyMatcher, valueMatcher);
        }

        public KvMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
            this.keyMatcher = keyMatcher;
            this.valueMatcher = valueMatcher;
        }

        @Override
        public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
            return keyMatcher.matches(kv.getKey()) && valueMatcher.matches(kv.getValue());
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("a KV(")
                    .appendValue(keyMatcher)
                    .appendText(", ")
                    .appendValue(valueMatcher)
                    .appendText(")");
        }
    }

}
