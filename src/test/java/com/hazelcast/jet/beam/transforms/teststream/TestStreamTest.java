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

package com.hazelcast.jet.beam.transforms.teststream;

import com.hazelcast.jet.beam.transforms.AbstractTransformTest;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TestStreamTest extends AbstractTransformTest {
    @Test
    @Category({ValidatesRunner.class, UsesTestStream.class})
    public void testReshuffleWithTimestampsStreaming() {
        TestStream<Long> stream =
                TestStream.create(VarLongCoder.of())
                          .advanceWatermarkTo(new Instant(0L).plus(Duration.standardDays(48L)))
                          .addElements(
                                  TimestampedValue.of(0L, new Instant(0L)),
                                  TimestampedValue.of(1L, new Instant(0L).plus(Duration.standardDays(48L))),
                                  TimestampedValue.of(
                                          2L, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(48L))))
                          .advanceWatermarkToInfinity();
                pipeline
                        .apply(stream)
                        .apply(ParDo.of(new DoFn<Long, Void>() {
                            @ProcessElement
                            public void processElement(@Element Long element, @Timestamp Instant timestamp, OutputReceiver<Void> r) {
                                System.out.println(timestamp + ": " + element);
                            }
                        }));

        pipeline.run();
    }
}
