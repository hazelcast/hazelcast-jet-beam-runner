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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.TimestampTests */
public class TimestampParDoTest extends AbstractParDoTest {

    @Test
    public void testParDoShiftTimestamp() {

        PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

        PCollection<String> output =
                input
                        .apply(ParDo.of(new TestOutputTimestampDoFn<>()))
                        .apply(
                                ParDo.of(
                                        new TestShiftTimestampDoFn<>(Duration.millis(1000), Duration.millis(-1000))))
                        .apply(ParDo.of(new TestFormatTimestampDoFn<>()));

        PAssert.that(output)
                .containsInAnyOrder(
                        "processing: 3, timestamp: -997",
                        "processing: 42, timestamp: -958",
                        "processing: 6, timestamp: -994");

        pipeline.run();
    }

    @Test
    public void testParDoShiftTimestampUnlimited() {
        PCollection<Long> outputs =
                pipeline
                        .apply(
                                Create.of(
                                        Arrays.asList(
                                                0L,
                                                BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(),
                                                GlobalWindow.INSTANCE.maxTimestamp().getMillis())))
                        .apply("AssignTimestampToValue", ParDo.of(new TestOutputTimestampDoFn<>()))
                        .apply(
                                "ReassignToMinimumTimestamp",
                                ParDo.of(
                                        new DoFn<Long, Long>() {
                                            @ProcessElement
                                            public void reassignTimestamps(
                                                    ProcessContext context, @Element Long element) {
                                                // Shift the latest element as far backwards in time as the model permits
                                                context.outputWithTimestamp(element, BoundedWindow.TIMESTAMP_MIN_VALUE);
                                            }

                                            @Override
                                            public Duration getAllowedTimestampSkew() {
                                                return Duration.millis(Long.MAX_VALUE);
                                            }
                                        }));

        PAssert.that(outputs)
                .satisfies(
                        input -> {
                            // This element is not shifted backwards in time. It must be present in the output.
                            assertThat(input, hasItem(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));
                            for (Long elem : input) {
                                // Sanity check the outputs. 0L and the end of the global window are shifted
                                // backwards in time and theoretically could be dropped.
                                assertThat(
                                        elem,
                                        anyOf(
                                                equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()),
                                                equalTo(GlobalWindow.INSTANCE.maxTimestamp().getMillis()),
                                                equalTo(0L)));
                            }
                            return null;
                        });

        pipeline.run();
    }

    static class TestOutputTimestampDoFn<T extends Number> extends DoFn<T, T> {
        @ProcessElement
        public void processElement(@Element T value, OutputReceiver<T> r) {
            r.outputWithTimestamp(value, new Instant(value.longValue()));
        }
    }

    static class TestShiftTimestampDoFn<T extends Number> extends DoFn<T, T> {
        private Duration allowedTimestampSkew;
        private Duration durationToShift;

        public TestShiftTimestampDoFn(Duration allowedTimestampSkew, Duration durationToShift) {
            this.allowedTimestampSkew = allowedTimestampSkew;
            this.durationToShift = durationToShift;
        }

        @Override
        public Duration getAllowedTimestampSkew() {
            return allowedTimestampSkew;
        }

        @ProcessElement
        public void processElement(
                @Element T value, @Timestamp Instant timestamp, OutputReceiver<T> r) {
            checkNotNull(timestamp);
            r.outputWithTimestamp(value, timestamp.plus(durationToShift));
        }
    }

    static class TestFormatTimestampDoFn<T extends Number> extends DoFn<T, String> {
        @ProcessElement
        public void processElement(
                @Element T element, @Timestamp Instant timestamp, OutputReceiver<String> r) {
            checkNotNull(timestamp);
            r.output("processing: " + element + ", timestamp: " + timestamp.getMillis());
        }
    }

}
