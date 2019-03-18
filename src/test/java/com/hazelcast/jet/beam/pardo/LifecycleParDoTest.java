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

package com.hazelcast.jet.beam.pardo;

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.LifecycleTests & ParDoLifecycleTest */
@SuppressWarnings("ALL")
public class LifecycleParDoTest extends AbstractParDoTest {

    @Test
    public void testWindowingInStartAndFinishBundle() {
        final FixedWindows windowFn = FixedWindows.of(Duration.millis(1));
        PCollection<String> output =
                pipeline
                        .apply(Create.timestamped(TimestampedValue.of("elem", new Instant(1))))
                        .apply(Window.into(windowFn))
                        .apply(
                                ParDo.of(
                                        new DoFn<String, String>() {
                                            @ProcessElement
                                            public void processElement(
                                                    @Element String element,
                                                    @Timestamp Instant timestamp,
                                                    OutputReceiver<String> r) {
                                                r.output(element);
                                                System.out.println("Process: " + element + ":" + timestamp.getMillis());
                                            }

                                            @FinishBundle
                                            public void finishBundle(FinishBundleContext c) {
                                                Instant ts = new Instant(3);
                                                c.output("finish", ts, windowFn.assignWindow(ts));
                                                System.out.println("Finish: 3");
                                            }
                                        }))
                        .apply(ParDo.of(new PrintingDoFn()));

        PAssert.that(output).satisfies(new Checker());

        pipeline.run();
    }

    @Test
    public void testFnCallSequence() {
        PCollectionList.of(pipeline.apply("Impolite", Create.of(1, 2, 4)))
                .and(pipeline.apply("Polite", Create.of(3, 5, 6, 7)))
                .apply(Flatten.pCollections())
                .apply(ParDo.of(new CallSequenceEnforcingFn<>()));

        pipeline.run();
    }

    @Test
    public void testFnCallSequenceMulti() {
        PCollectionList.of(pipeline.apply("Impolite", Create.of(1, 2, 4)))
                .and(pipeline.apply("Polite", Create.of(3, 5, 6, 7)))
                .apply(Flatten.pCollections())
                .apply(
                        ParDo.of(new CallSequenceEnforcingFn<Integer>())
                                .withOutputTags(new TupleTag<Integer>() {}, TupleTagList.empty()));

        pipeline.run();
    }

    @Test
    public void testTeardownCalledAfterExceptionInStartBundle() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.START_BUNDLE);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    public void testTeardownCalledAfterExceptionInProcessElement() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.PROCESS_ELEMENT);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    public void testTeardownCalledAfterExceptionInFinishBundle() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.FINISH_BUNDLE);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    public void testWithContextTeardownCalledAfterExceptionInSetup() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.SETUP);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    public void testWithContextTeardownCalledAfterExceptionInStartBundle() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.START_BUNDLE);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    public void testWithContextTeardownCalledAfterExceptionInProcessElement() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.PROCESS_ELEMENT);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    public void testWithContextTeardownCalledAfterExceptionInFinishBundle() {
        ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.FINISH_BUNDLE);
        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
        try {
            pipeline.run();
            fail("Pipeline should have failed with an exception");
        } catch (Exception e) {
            assertThat(
                    "Function should have been torn down after exception",
                    ExceptionThrowingOldFn.teardownCalled.get(),
                    is(true));
        }
    }

    @Test
    @Ignore //todo: enable after state is implemented
    public void testFnCallSequenceStateful() {
        PCollectionList.of(pipeline.apply("Impolite", Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 4))))
                .and(
                        pipeline.apply(
                                "Polite", Create.of(KV.of("b", 3), KV.of("a", 5), KV.of("c", 6), KV.of("c", 7))))
                .apply(Flatten.pCollections())
                .apply(
                        ParDo.of(new CallSequenceEnforcingStatefulFn<String, Integer>())
                                .withOutputTags(new TupleTag<KV<String, Integer>>() {}, TupleTagList.empty()));

        pipeline.run();
    }

    private static class Checker implements SerializableFunction<Iterable<String>, Void> {
        @Override
        public Void apply(Iterable<String> input) {
            boolean foundElement = false;
            boolean foundFinish = false;
            for (String str : input) {
                if ("elem:1:1".equals(str)) {
                    if (foundElement) {
                        throw new AssertionError("Received duplicate element");
                    }
                    foundElement = true;
                } else if ("finish:3:3".equals(str)) {
                    foundFinish = true;
                } else {
                    throw new AssertionError("Got unexpected value: " + str);
                }
            }
            if (!foundElement) {
                throw new AssertionError("Missing \"elem:1:1\"");
            }
            if (!foundFinish) {
                throw new AssertionError("Missing \"finish:3:3\"");
            }

            return null;
        }
    }

    private static class PrintingDoFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(
                @Element String element,
                @Timestamp Instant timestamp,
                BoundedWindow window,
                OutputReceiver<String> receiver) {
            receiver.output(
                    element + ":" + timestamp.getMillis() + ":" + window.maxTimestamp().getMillis());
        }
    }

    private static class CallSequenceEnforcingDoFn<T> extends DoFn<T, T> {
        private boolean setupCalled = false;
        private int startBundleCalls = 0;
        private int finishBundleCalls = 0;
        private boolean teardownCalled = false;

        @Setup
        public void setup() {
            assertThat("setup should not be called twice", setupCalled, is(false));
            assertThat("setup should be called before startBundle", startBundleCalls, equalTo(0));
            assertThat("setup should be called before finishBundle", finishBundleCalls, equalTo(0));
            assertThat("setup should be called before teardown", teardownCalled, is(false));
            setupCalled = true;
        }

        @StartBundle
        public void startBundle() {
            assertThat("setup should have been called", setupCalled, is(true));
            assertThat(
                    "Even number of startBundle and finishBundle calls in startBundle",
                    startBundleCalls,
                    equalTo(finishBundleCalls));
            assertThat("teardown should not have been called", teardownCalled, is(false));
            startBundleCalls++;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
            assertThat(
                    "there should be one startBundle call with no call to finishBundle",
                    startBundleCalls,
                    equalTo(finishBundleCalls + 1));
            assertThat("teardown should not have been called", teardownCalled, is(false));
        }

        @FinishBundle
        public void finishBundle() {
            assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
            assertThat(
                    "there should be one bundle that has been started but not finished",
                    startBundleCalls,
                    equalTo(finishBundleCalls + 1));
            assertThat("teardown should not have been called", teardownCalled, is(false));
            finishBundleCalls++;
        }

        @Teardown
        public void teardown() {
            assertThat(setupCalled, is(true));
            assertThat(startBundleCalls, anyOf(equalTo(finishBundleCalls)));
            assertThat(teardownCalled, is(false));
            teardownCalled = true;
        }
    }

    private static class CallSequenceEnforcingStatefulFn<K, V> extends CallSequenceEnforcingDoFn<KV<K, V>> {
        private static final String STATE_ID = "foo";

        @DoFn.StateId(STATE_ID)
        private final StateSpec<ValueState<String>> valueSpec = StateSpecs.value();
    }

    private static class CallSequenceEnforcingFn<T> extends DoFn<T, T> {
        private boolean setupCalled = false;
        private int startBundleCalls = 0;
        private int finishBundleCalls = 0;
        private boolean teardownCalled = false;

        @Setup
        public void before() {
            assertThat("setup should not be called twice", setupCalled, is(false));
            assertThat("setup should be called before startBundle", startBundleCalls, equalTo(0));
            assertThat("setup should be called before finishBundle", finishBundleCalls, equalTo(0));
            assertThat("setup should be called before teardown", teardownCalled, is(false));
            setupCalled = true;
        }

        @StartBundle
        public void begin() {
            assertThat("setup should have been called", setupCalled, is(true));
            assertThat(
                    "Even number of startBundle and finishBundle calls in startBundle",
                    startBundleCalls,
                    equalTo(finishBundleCalls));
            assertThat("teardown should not have been called", teardownCalled, is(false));
            startBundleCalls++;
        }

        @ProcessElement
        public void process(ProcessContext c) throws Exception {
            assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
            assertThat(
                    "there should be one startBundle call with no call to finishBundle",
                    startBundleCalls,
                    equalTo(finishBundleCalls + 1));
            assertThat("teardown should not have been called", teardownCalled, is(false));
        }

        @FinishBundle
        public void end() {
            assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
            assertThat(
                    "there should be one bundle that has been started but not finished",
                    startBundleCalls,
                    equalTo(finishBundleCalls + 1));
            assertThat("teardown should not have been called", teardownCalled, is(false));
            finishBundleCalls++;
        }

        @Teardown
        public void after() {
            assertThat(setupCalled, is(true));
            assertThat(startBundleCalls, anyOf(equalTo(finishBundleCalls)));
            assertThat(teardownCalled, is(false));
            teardownCalled = true;
        }
    }

    private static class ExceptionThrowingOldFn extends DoFn<Object, Object> {
        static AtomicBoolean teardownCalled = new AtomicBoolean(false);

        private final MethodForException toThrow;
        private boolean thrown;

        private ExceptionThrowingOldFn(MethodForException toThrow) {
            this.toThrow = toThrow;
        }

        @Setup
        public void setup() throws Exception {
            throwIfNecessary(MethodForException.SETUP);
        }

        @StartBundle
        public void startBundle() throws Exception {
            throwIfNecessary(MethodForException.START_BUNDLE);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            throwIfNecessary(MethodForException.PROCESS_ELEMENT);
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            throwIfNecessary(MethodForException.FINISH_BUNDLE);
        }

        private void throwIfNecessary(MethodForException method) throws Exception {
            if (toThrow == method && !thrown) {
                thrown = true;
                throw new Exception("Hasn't yet thrown");
            }
        }

        @Teardown
        public void teardown() {
            if (!thrown) {
                fail("Excepted to have a processing method throw an exception");
            }
            teardownCalled.set(true);
        }
    }

    private enum MethodForException {
        SETUP,
        START_BUNDLE,
        PROCESS_ELEMENT,
        FINISH_BUNDLE
    }

}
