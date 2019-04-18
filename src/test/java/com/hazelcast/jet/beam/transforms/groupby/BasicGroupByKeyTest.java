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

package com.hazelcast.jet.beam.transforms.groupby;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/* "Inspired" by org.apache.beam.sdk.transforms.GroupByKeyTest.BasicTests */
@SuppressWarnings("ALL")
public class BasicGroupByKeyTest extends AbstractGroupByKeyTest {

    @Test
    public void testGroupByKey() {
        List<KV<String, Integer>> ungroupedPairs =
                Arrays.asList(
                        KV.of("k1", 3),
                        KV.of("k5", Integer.MAX_VALUE),
                        KV.of("k5", Integer.MIN_VALUE),
                        KV.of("k2", 66),
                        KV.of("k1", 4),
                        KV.of("k2", -33),
                        KV.of("k3", 0));

        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.of(ungroupedPairs)
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

        PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

        SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> checker =
                containsKvs(
                        kv("k1", 3, 4),
                        kv("k5", Integer.MIN_VALUE, Integer.MAX_VALUE),
                        kv("k2", 66, -33),
                        kv("k3", 0));
        PAssert.that(output).satisfies(checker);
        PAssert.that(output).inWindow(GlobalWindow.INSTANCE).satisfies(checker);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testGroupByKeyEmpty() {
        List<KV<String, Integer>> ungroupedPairs = Collections.emptyList();

        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.of(ungroupedPairs)
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

        PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

        PAssert.that(output).empty();

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore //todo advanceProcessingTime
    public void testCombiningAccumulatingProcessingTime() {
        PCollection<Integer> triggeredSums =
                pipeline.apply(
                        TestStream.create(VarIntCoder.of())
                                .advanceWatermarkTo(new Instant(0))
                                .addElements(
                                        TimestampedValue.of(2, new Instant(2)),
                                        TimestampedValue.of(5, new Instant(5)))
                                .advanceWatermarkTo(new Instant(100))
                                .advanceProcessingTime(Duration.millis(10))
                                .advanceWatermarkToInfinity())
                        .apply(
                                Window.<Integer>into(FixedWindows.of(Duration.millis(100)))
                                        .withTimestampCombiner(TimestampCombiner.EARLIEST)
                                        .accumulatingFiredPanes()
                                        .withAllowedLateness(Duration.ZERO)
                                        .triggering(
                                                Repeatedly.forever(
                                                        AfterProcessingTime.pastFirstElementInPane()
                                                                .plusDelayOf(Duration.millis(10)))))
                        .apply(Sum.integersGlobally().withoutDefaults());

        PAssert.that(triggeredSums).containsInAnyOrder(7);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testTimestampCombinerEarliest() {
        pipeline.apply(
                Create.timestamped(
                        TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                        TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
                .apply(
                        Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
                                .withTimestampCombiner(TimestampCombiner.EARLIEST))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new AssertTimestamp(new Instant(0))));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    /**
     * Tests that when two elements are combined via a GroupByKey their output timestamp agrees with
     * the windowing function customized to use the latest value.
     */
    @Test
    public void testTimestampCombinerLatest() {
        pipeline.apply(
                Create.timestamped(
                        TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                        TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
                .apply(
                        Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
                                .withTimestampCombiner(TimestampCombiner.LATEST))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new AssertTimestamp(new Instant(10))));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore //todo: what to do with this one?
    public void testGroupByKeyWithBadEqualsHashCode() {
        final int numValues = 10;
        final int numKeys = 5;

        pipeline.getCoderRegistry()
                .registerCoderProvider(
                        CoderProviders.fromStaticMethods(BadEqualityKey.class, DeterministicKeyCoder.class));

        // construct input data
        List<KV<BadEqualityKey, Long>> input = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            for (int key = 0; key < numKeys; key++) {
                input.add(KV.of(new BadEqualityKey(key), 1L));
            }
        }

        // We first ensure that the values are randomly partitioned in the beginning.
        // Some runners might otherwise keep all values on the machine where
        // they are initially created.
        PCollection<KV<BadEqualityKey, Long>> dataset1 =
                pipeline.apply(Create.of(input))
                        .apply(ParDo.of(new AssignRandomKey()))
                        .apply(Reshuffle.of())
                        .apply(Values.create());

        // Make the GroupByKey and Count implicit, in real-world code
        // this would be a Count.perKey()
        PCollection<KV<BadEqualityKey, Long>> result =
                dataset1.apply(GroupByKey.create()).apply(Combine.groupedValues(new CountFn()));

        PAssert.that(result).satisfies(new AssertThatCountPerKeyCorrect(numValues));

        PAssert.that(result.apply(Keys.create())).satisfies(new AssertThatAllKeysExist(numKeys));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testLargeKeys10KB() {
        runLargeKeysTest(pipeline, 10 << 10);
    }

    @Test
    public void testLargeKeys100KB() {
        runLargeKeysTest(pipeline, 100 << 10);
    }

    @Test
    public void testLargeKeys1MB() {
        runLargeKeysTest(pipeline, 1 << 20);
    }

    @Test
    public void testLargeKeys10MB() {
        runLargeKeysTest(pipeline, 10 << 20);
    }

    private static void runLargeKeysTest(TestPipeline pipeline, final int keySize) {
        PCollection<KV<String, Integer>> result =
                pipeline.apply(Create.of("a", "a", "b"))
                        .apply(
                                "Expand",
                                ParDo.of(
                                        new DoFn<String, KV<String, String>>() {
                                            @ProcessElement
                                            public void process(ProcessContext c) {
                                                c.output(KV.of(bigString(c.element().charAt(0), keySize), c.element()));
                                            }
                                        }))
                        .apply(GroupByKey.create())
                        .apply(
                                "Count",
                                ParDo.of(
                                        new DoFn<KV<String, Iterable<String>>, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void process(ProcessContext c) {
                                                int size = 0;
                                                for (String value : c.element().getValue()) {
                                                    size++;
                                                }
                                                c.output(KV.of(c.element().getKey(), size));
                                            }
                                        }));

        PAssert.that(result)
                .satisfies(
                        values -> {
                            assertThat(
                                    values,
                                    containsInAnyOrder(
                                            KV.of(bigString('a', keySize), 2), KV.of(bigString('b', keySize), 1)));
                            return null;
                        });

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    private static String bigString(char c, int size) {
        char[] buf = new char[size];
        for (int i = 0; i < size; i++) {
            buf[i] = c;
        }
        return new String(buf);
    }

    private static class AssertTimestamp<K, V> extends DoFn<KV<K, V>, Void> {
        private final Instant timestamp;

        AssertTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            assertThat(c.timestamp(), equalTo(timestamp));
        }
    }

}
