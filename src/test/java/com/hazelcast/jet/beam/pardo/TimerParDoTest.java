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

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.TimerTests */
@SuppressWarnings("ALL")
public class TimerParDoTest extends AbstractParDoTest {

    //todo: enable tests after state & timers are implemented

    @Test
    @Ignore
    public void testEventTimeTimerBounded() {
        final String timerId = "foo";

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
                        timer.offset(Duration.standardSeconds(1)).setRelative();
                        r.output(3);
                    }

                    @OnTimer(timerId)
                    public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
                        if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                            r.output(42);
                        }
                    }
                };

        PCollection<Integer> output =
                pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
        PAssert.that(output).containsInAnyOrder(3, 42);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testGbkFollowedByUserTimers() {

        DoFn<KV<String, Iterable<Integer>>, Integer> fn =
                new DoFn<KV<String, Iterable<Integer>>, Integer>() {

                    public static final String TIMER_ID = "foo";

                    @TimerId(TIMER_ID)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(@TimerId(TIMER_ID) Timer timer, OutputReceiver<Integer> r) {
                        timer.offset(Duration.standardSeconds(1)).setRelative();
                        r.output(3);
                    }

                    @OnTimer(TIMER_ID)
                    public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
                        if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                            r.output(42);
                        }
                    }
                };

        PCollection<Integer> output =
                pipeline
                        .apply(Create.of(KV.of("hello", 37)))
                        .apply(GroupByKey.create())
                        .apply(ParDo.of(fn));
        PAssert.that(output).containsInAnyOrder(3, 42);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testEventTimeTimerAlignBounded() {
        final String timerId = "foo";

        DoFn<KV<String, Integer>, KV<Integer, Instant>> fn =
                new DoFn<KV<String, Integer>, KV<Integer, Instant>>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(
                            @TimerId(timerId) Timer timer,
                            @Timestamp Instant timestamp,
                            OutputReceiver<KV<Integer, Instant>> r) {
                        timer.align(Duration.standardSeconds(1)).offset(Duration.millis(1)).setRelative();
                        r.output(KV.of(3, timestamp));
                    }

                    @OnTimer(timerId)
                    public void onTimer(
                            @Timestamp Instant timestamp, OutputReceiver<KV<Integer, Instant>> r) {
                        r.output(KV.of(42, timestamp));
                    }
                };

        PCollection<KV<Integer, Instant>> output =
                pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of(3, BoundedWindow.TIMESTAMP_MIN_VALUE),
                        KV.of(42, BoundedWindow.TIMESTAMP_MIN_VALUE.plus(1774)));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testTimerReceivedInOriginalWindow() {
        final String timerId = "foo";

        DoFn<KV<String, Integer>, BoundedWindow> fn =
                new DoFn<KV<String, Integer>, BoundedWindow>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(@TimerId(timerId) Timer timer) {
                        timer.offset(Duration.standardSeconds(1)).setRelative();
                    }

                    @OnTimer(timerId)
                    public void onTimer(BoundedWindow window, OutputReceiver<BoundedWindow> r) {
                        r.output(window);
                    }

                    @Override
                    public TypeDescriptor<BoundedWindow> getOutputTypeDescriptor() {
                        return (TypeDescriptor) TypeDescriptor.of(IntervalWindow.class);
                    }
                };

        SlidingWindows windowing =
                SlidingWindows.of(Duration.standardMinutes(3)).every(Duration.standardMinutes(1));
        PCollection<BoundedWindow> output =
                pipeline
                        .apply(Create.timestamped(TimestampedValue.of(KV.of("hello", 24), new Instant(0L))))
                        .apply(Window.into(windowing))
                        .apply(ParDo.of(fn));

        PAssert.that(output)
                .containsInAnyOrder(
                        new IntervalWindow(new Instant(0), Duration.standardMinutes(3)),
                        new IntervalWindow(
                                new Instant(0).minus(Duration.standardMinutes(1)), Duration.standardMinutes(3)),
                        new IntervalWindow(
                                new Instant(0).minus(Duration.standardMinutes(2)), Duration.standardMinutes(3)));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testEventTimeTimerAbsolute() {
        final String timerId = "foo";

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(
                            @TimerId(timerId) Timer timer, BoundedWindow window, OutputReceiver<Integer> r) {
                        timer.set(window.maxTimestamp());
                        r.output(3);
                    }

                    @OnTimer(timerId)
                    public void onTimer(OutputReceiver<Integer> r) {
                        r.output(42);
                    }
                };

        PCollection<Integer> output =
                pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
        PAssert.that(output).containsInAnyOrder(3, 42);
        pipeline.run();
    }

    @Test
    //todo: is ignored in beam too (https://issues.apache.org/jira/browse/BEAM-2791, https://issues.apache.org/jira/browse/BEAM-2535)
    @Ignore
    public void testEventTimeTimerLoop() {
        final String stateId = "count";
        final String timerId = "timer";
        final int loopCount = 5;

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @TimerId(timerId)
                    private final TimerSpec loopSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<Integer> countState,
                            @TimerId(timerId) Timer loopTimer) {
                        loopTimer.offset(Duration.millis(1)).setRelative();
                    }

                    @OnTimer(timerId)
                    public void onLoopTimer(
                            @StateId(stateId) ValueState<Integer> countState,
                            @TimerId(timerId) Timer loopTimer,
                            OutputReceiver<Integer> r) {
                        int count = MoreObjects.firstNonNull(countState.read(), 0);
                        if (count < loopCount) {
                            r.output(count);
                            countState.write(count + 1);
                            loopTimer.offset(Duration.millis(1)).setRelative();
                        }
                    }
                };

        PCollection<Integer> output =
                pipeline.apply(Create.of(KV.of("hello", 42))).apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(0, 1, 2, 3, 4);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testEventTimeTimerMultipleKeys() {
        final String timerId = "foo";
        final String stateId = "sizzle";

        final int offset = 5000;
        final int timerOutput = 4093;

        DoFn<KV<String, Integer>, KV<String, Integer>> fn =
                new DoFn<KV<String, Integer>, KV<String, Integer>>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @StateId(stateId)
                    private final StateSpec<ValueState<String>> stateSpec =
                            StateSpecs.value(StringUtf8Coder.of());

                    @ProcessElement
                    public void processElement(
                            ProcessContext context,
                            @TimerId(timerId) Timer timer,
                            @StateId(stateId) ValueState<String> state,
                            BoundedWindow window) {
                        timer.set(window.maxTimestamp());
                        state.write(context.element().getKey());
                        context.output(
                                KV.of(context.element().getKey(), context.element().getValue() + offset));
                    }

                    @OnTimer(timerId)
                    public void onTimer(
                            @StateId(stateId) ValueState<String> state, OutputReceiver<KV<String, Integer>> r) {
                        r.output(KV.of(state.read(), timerOutput));
                    }
                };

        // Enough keys that we exercise interesting code paths
        int numKeys = 50;
        List<KV<String, Integer>> input = new ArrayList<>();
        List<KV<String, Integer>> expectedOutput = new ArrayList<>();

        for (Integer key = 0; key < numKeys; ++key) {
            // Each key should have just one final output at GC time
            expectedOutput.add(KV.of(key.toString(), timerOutput));

            for (int i = 0; i < 15; ++i) {
                // Each input should be output with the offset added
                input.add(KV.of(key.toString(), i));
                expectedOutput.add(KV.of(key.toString(), i + offset));
            }
        }

        Collections.shuffle(input);

        PCollection<KV<String, Integer>> output =
                pipeline.apply(Create.of(input)).apply(ParDo.of(fn));
        PAssert.that(output).containsInAnyOrder(expectedOutput);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testAbsoluteProcessingTimeTimerRejected() {
        final String timerId = "foo";

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

                    @ProcessElement
                    public void processElement(@TimerId(timerId) Timer timer) {
                        try {
                            timer.set(new Instant(0));
                            fail("Should have failed due to processing time with absolute timer.");
                        } catch (RuntimeException e) {
                            String message = e.getMessage();
                            List<String> expectedSubstrings =
                                    Arrays.asList("relative timers", "processing time");
                            expectedSubstrings.forEach(
                                    str ->
                                            Preconditions.checkState(
                                                    message.contains(str),
                                                    "Pipeline didn't fail with the expected strings: %s",
                                                    expectedSubstrings));
                        }
                    }

                    @OnTimer(timerId)
                    public void onTimer() {}
                };

        pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testOutOfBoundsEventTimeTimer() {
        final String timerId = "foo";

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(
                            ProcessContext context, BoundedWindow window, @TimerId(timerId) Timer timer) {
                        try {
                            timer.set(window.maxTimestamp().plus(1L));
                            fail("Should have failed due to processing time with absolute timer.");
                        } catch (RuntimeException e) {
                            String message = e.getMessage();
                            List<String> expectedSubstrings = Arrays.asList("event time timer", "expiration");
                            expectedSubstrings.forEach(
                                    str ->
                                            Preconditions.checkState(
                                                    message.contains(str),
                                                    "Pipeline didn't fail with the expected strings: %s",
                                                    expectedSubstrings));
                        }
                    }

                    @OnTimer(timerId)
                    public void onTimer() {}
                };

        pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testProcessingTimeTimerCanBeReset() {
        final String timerId = "foo";

        DoFn<KV<String, String>, String> fn =
                new DoFn<KV<String, String>, String>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

                    @ProcessElement
                    public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
                        timer.offset(Duration.standardSeconds(1)).setRelative();
                        context.output(context.element().getValue());
                    }

                    @OnTimer(timerId)
                    public void onTimer(OutputReceiver<String> r) {
                        r.output("timer_output");
                    }
                };

        TestStream<KV<String, String>> stream =
                TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                        .addElements(KV.of("key", "input1"))
                        .addElements(KV.of("key", "input2"))
                        .advanceProcessingTime(Duration.standardSeconds(2))
                        .advanceWatermarkToInfinity();

        PCollection<String> output = pipeline.apply(stream).apply(ParDo.of(fn));
        // Timer "foo" is set twice because input1 and input 2 are outputted. However, only one
        // "timer_output" is outputted. Therefore, the timer is overwritten.
        PAssert.that(output).containsInAnyOrder("input1", "input2", "timer_output");
        pipeline.run();
    }

    @Test
    @Ignore
    public void testEventTimeTimerCanBeReset() {
        final String timerId = "foo";

        DoFn<KV<String, String>, String> fn =
                new DoFn<KV<String, String>, String>() {

                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
                        timer.offset(Duration.standardSeconds(1)).setRelative();
                        context.output(context.element().getValue());
                    }

                    @OnTimer(timerId)
                    public void onTimer(OutputReceiver<String> r) {
                        r.output("timer_output");
                    }
                };

        TestStream<KV<String, String>> stream =
                TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                        .advanceWatermarkTo(new Instant(0))
                        .addElements(KV.of("hello", "input1"))
                        .addElements(KV.of("hello", "input2"))
                        .advanceWatermarkToInfinity();

        PCollection<String> output = pipeline.apply(stream).apply(ParDo.of(fn));
        // Timer "foo" is set twice because input1 and input 2 are outputted. However, only one
        // "timer_output" is outputted. Therefore, the timer is overwritten.
        PAssert.that(output).containsInAnyOrder("input1", "input2", "timer_output");
        pipeline.run();
    }

    @Test
    @Ignore
    public void testPipelineOptionsParameterOnTimer() {
        final String timerId = "thisTimer";

        PCollection<String> results =
                pipeline
                        .apply(Create.of(KV.of(0, 0)))
                        .apply(
                                ParDo.of(
                                        new DoFn<KV<Integer, Integer>, String>() {
                                            @TimerId(timerId)
                                            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                                            @ProcessElement
                                            public void process(
                                                    ProcessContext c, BoundedWindow w, @TimerId(timerId) Timer timer) {
                                                timer.set(w.maxTimestamp());
                                            }

                                            @OnTimer(timerId)
                                            public void onTimer(OutputReceiver<String> r, PipelineOptions options) {
                                                r.output(options.as(MyOptions.class).getFakeOption());
                                            }
                                        }));

        String testOptionValue = "not fake anymore";
        pipeline.getOptions().as(MyOptions.class).setFakeOption(testOptionValue);
        PAssert.that(results).containsInAnyOrder("not fake anymore");

        pipeline.run();
    }

    @Test
    @Ignore
    public void duplicateTimerSetting() {
        TestStream<KV<String, String>> stream =
                TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                        .addElements(KV.of("key1", "v1"))
                        .advanceWatermarkToInfinity();

        PCollection<String> result = pipeline.apply(stream).apply(ParDo.of(new TwoTimerDoFn()));
        PAssert.that(result).containsInAnyOrder("It works");

        pipeline.run().waitUntilFinish();
    }

    private static class TwoTimerDoFn extends DoFn<KV<String, String>, String> {
        @TimerId("timer")
        private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement
        public void process(ProcessContext c, @TimerId("timer") Timer timer) {
            timer.offset(Duration.standardMinutes(10)).setRelative();
            timer.offset(Duration.standardMinutes(30)).setRelative();
        }

        @OnTimer("timer")
        public void onTimer(OutputReceiver<String> r, @TimerId("timer") Timer timer) {
            r.output("It works");
        }
    }

}
