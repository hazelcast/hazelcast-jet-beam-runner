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

package com.hazelcast.jet.beam.transforms;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.beam.JetPipelineOptions;
import com.hazelcast.jet.beam.TestJetRunner;
import com.hazelcast.jet.config.JetConfig;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.reference.PortableRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.Serializable;
import java.util.Map;

public abstract class AbstractTransformTest implements Serializable { //has to be set Serializable because the way how side inputs are being used via capturing lambdas

    @Rule
    public transient Timeout globalTimeout = Timeout.seconds(1000); // 10 seconds max per method tested

    @Rule
    public transient TestPipeline pipeline = getTestPipeline();

    @Rule public transient ExpectedException thrown = ExpectedException.none();

    private static TestPipeline getTestPipeline() {
        PipelineOptions options = PipelineOptionsFactory.create();

        //setupDirectRunner(options);
        //setupClassicalJetRunner(options);
        setupPortableRunner(options);

        return TestPipeline.fromOptions(options);
    }

    private static void setupDirectRunner(PipelineOptions options) {
        options.setRunner(DirectRunner.class);
    }

    private static void setupClassicalJetRunner(PipelineOptions options) {
        options.setRunner(TestJetRunner.class);
        JetPipelineOptions jetOptions = options.as(JetPipelineOptions.class);
        jetOptions.setJetGroupName(JetConfig.DEFAULT_GROUP_NAME);
        jetOptions.setJetLocalParallelism(2);
    }

    private static void setupPortableRunner(PipelineOptions options) {
        options.setRunner(PortableRunner.class);
        PortablePipelineOptions portablePipelineOptions = options.as(PortablePipelineOptions.class);
        portablePipelineOptions.setJobEndpoint("localhost:8099");

        SdkHarnessOptions sdkHarnessOptions = options.as(SdkHarnessOptions.class);
        sdkHarnessOptions.setDefaultSdkHarnessLogLevel(SdkHarnessOptions.LogLevel.INFO);
    }

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();

    static {
        TestJetRunner.JET_CLIENT_SUPPLIER = factory::newClient;
    }

    private static JetInstance instance;

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("map"));
        instance = factory.newMember(config);

        printEnv();
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
    }

    private static void printEnv() {
        StringBuilder sb = new StringBuilder("The environment: ");
        Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            sb.append("\n\t").append(envName).append("=").append(env.get(envName));
        }
        System.out.println(sb.toString());
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
