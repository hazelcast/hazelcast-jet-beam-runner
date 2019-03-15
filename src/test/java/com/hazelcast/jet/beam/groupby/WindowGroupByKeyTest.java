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

package com.hazelcast.jet.beam.groupby;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/* "Inspired" by org.apache.beam.sdk.transforms.GroupByKeyTest.WindowTests */
@SuppressWarnings("ALL")
public class WindowGroupByKeyTest extends AbstractGroupByKeyTest {

    @Test
    public void testGroupByKeyAndWindows() {
        List<KV<String, Integer>> ungroupedPairs =
                Arrays.asList(
                        KV.of("k1", 3), // window [0, 5)
                        KV.of("k5", Integer.MAX_VALUE), // window [0, 5)
                        KV.of("k5", Integer.MIN_VALUE), // window [0, 5)
                        KV.of("k2", 66), // window [0, 5)
                        KV.of("k1", 4), // window [5, 10)
                        KV.of("k2", -33), // window [5, 10)
                        KV.of("k3", 0)); // window [5, 10)

        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.timestamped(ungroupedPairs, Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L))
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
        PCollection<KV<String, Iterable<Integer>>> output =
                input.apply(Window.into(FixedWindows.of(new Duration(5)))).apply(GroupByKey.create());

        PAssert.that(output)
                .satisfies(
                        containsKvs(
                                kv("k1", 3),
                                kv("k1", 4),
                                kv("k5", Integer.MAX_VALUE, Integer.MIN_VALUE),
                                kv("k2", 66),
                                kv("k2", -33),
                                kv("k3", 0)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(0L), Duration.millis(5L)))
                .satisfies(
                        containsKvs(
                                kv("k1", 3), kv("k5", Integer.MIN_VALUE, Integer.MAX_VALUE), kv("k2", 66)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(5L), Duration.millis(5L)))
                .satisfies(containsKvs(kv("k1", 4), kv("k2", -33), kv("k3", 0)));

        pipeline.run();
    }

    @Test
    public void testGroupByKeyMultipleWindows() {
        PCollection<KV<String, Integer>> windowedInput =
                pipeline.apply(
                        Create.timestamped(
                                TimestampedValue.of(KV.of("foo", 1), new Instant(1)),
                                TimestampedValue.of(KV.of("foo", 4), new Instant(4)),
                                TimestampedValue.of(KV.of("bar", 3), new Instant(3))))
                        .apply(
                                Window.into(SlidingWindows.of(Duration.millis(5L)).every(Duration.millis(3L))));

        PCollection<KV<String, Iterable<Integer>>> output = windowedInput.apply(GroupByKey.create());

        PAssert.that(output)
                .satisfies(
                        containsKvs(kv("foo", 1, 4), kv("foo", 1), kv("foo", 4), kv("bar", 3), kv("bar", 3)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(-3L), Duration.millis(5L)))
                .satisfies(containsKvs(kv("foo", 1)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(0L), Duration.millis(5L)))
                .satisfies(containsKvs(kv("foo", 1, 4), kv("bar", 3)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(3L), Duration.millis(5L)))
                .satisfies(containsKvs(kv("foo", 4), kv("bar", 3)));

        pipeline.run();
    }

    @Test
    public void testGroupByKeyMergingWindows() {
        PCollection<KV<String, Integer>> windowedInput =
                pipeline.apply(
                        Create.timestamped(
                                TimestampedValue.of(KV.of("foo", 1), new Instant(1)),
                                TimestampedValue.of(KV.of("foo", 4), new Instant(4)),
                                TimestampedValue.of(KV.of("bar", 3), new Instant(3)),
                                TimestampedValue.of(KV.of("foo", 9), new Instant(9))))
                        .apply(Window.into(Sessions.withGapDuration(Duration.millis(4L))));

        PCollection<KV<String, Iterable<Integer>>> output = windowedInput.apply(GroupByKey.create());

        PAssert.that(output).satisfies(containsKvs(kv("foo", 1, 4), kv("foo", 9), kv("bar", 3)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(1L), new Instant(8L)))
                .satisfies(containsKvs(kv("foo", 1, 4)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(3L), new Instant(7L)))
                .satisfies(containsKvs(kv("bar", 3)));
        PAssert.that(output)
                .inWindow(new IntervalWindow(new Instant(9L), new Instant(13L)))
                .satisfies(containsKvs(kv("foo", 9)));

        pipeline.run();
    }

}
