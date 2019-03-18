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

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.StateTests */
@SuppressWarnings("ALL")
public class StateParDoTest extends AbstractParDoTest {

    //todo: enable tests after state & timers are implemented

    @Test
    @Ignore
    public void testValueStateSimple() {
        final String stateId = "foo";

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> intState =
                            StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<Integer> state, OutputReceiver<Integer> r) {
                        Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
                        r.output(currentValue);
                        state.write(currentValue + 1);
                    }
                };

        PCollection<Integer> output =
                pipeline
                        .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
                        .apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(0, 1, 2);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testValueStateDedup() {
        final String stateId = "foo";

        DoFn<KV<Integer, Integer>, Integer> onePerKey =
                new DoFn<KV<Integer, Integer>, Integer>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> seenSpec =
                            StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @Element KV<Integer, Integer> element,
                            @StateId(stateId) ValueState<Integer> seenState,
                            OutputReceiver<Integer> r) {
                        Integer seen = MoreObjects.firstNonNull(seenState.read(), 0);

                        if (seen == 0) {
                            seenState.write(seen + 1);
                            r.output(element.getValue());
                        }
                    }
                };

        int numKeys = 50;
        // A big enough list that we can see some deduping
        List<KV<Integer, Integer>> input = new ArrayList<>();

        // The output should have no dupes
        Set<Integer> expectedOutput = new HashSet<>();

        for (int key = 0; key < numKeys; ++key) {
            int output = 1000 + key;
            expectedOutput.add(output);

            for (int i = 0; i < 15; ++i) {
                input.add(KV.of(key, output));
            }
        }

        Collections.shuffle(input);

        PCollection<Integer> output = pipeline.apply(Create.of(input)).apply(ParDo.of(onePerKey));

        PAssert.that(output).containsInAnyOrder(expectedOutput);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testCoderInferenceOfList() {
        final String stateId = "foo";
        MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();
        pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

        DoFn<KV<String, Integer>, List<MyInteger>> fn =
                new DoFn<KV<String, Integer>, List<MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<List<MyInteger>>> intState = StateSpecs.value();

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) ValueState<List<MyInteger>> state,
                            OutputReceiver<List<MyInteger>> r) {
                        MyInteger myInteger = new MyInteger(element.getValue());
                        List<MyInteger> currentValue = state.read();
                        List<MyInteger> newValue =
                                currentValue != null
                                        ? ImmutableList.<MyInteger>builder()
                                        .addAll(currentValue)
                                        .add(myInteger)
                                        .build()
                                        : Collections.singletonList(myInteger);
                        r.output(newValue);
                        state.write(newValue);
                    }
                };

        pipeline
                .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
                .apply(ParDo.of(fn))
                .setCoder(ListCoder.of(myIntegerCoder));

        pipeline.run();
    }

    @Test
    @Ignore
    public void testValueStateFixedWindows() {
        final String stateId = "foo";

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> intState =
                            StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<Integer> state, OutputReceiver<Integer> r) {
                        Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
                        r.output(currentValue);
                        state.write(currentValue + 1);
                    }
                };

        IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
        IntervalWindow secondWindow = new IntervalWindow(new Instant(10), new Instant(20));

        PCollection<Integer> output =
                pipeline
                        .apply(
                                Create.timestamped(
                                        // first window
                                        TimestampedValue.of(KV.of("hello", 7), new Instant(1)),
                                        TimestampedValue.of(KV.of("hello", 14), new Instant(2)),
                                        TimestampedValue.of(KV.of("hello", 21), new Instant(3)),

                                        // second window
                                        TimestampedValue.of(KV.of("hello", 28), new Instant(11)),
                                        TimestampedValue.of(KV.of("hello", 35), new Instant(13))))
                        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply("Stateful ParDo", ParDo.of(fn));

        PAssert.that(output).inWindow(firstWindow).containsInAnyOrder(0, 1, 2);
        PAssert.that(output).inWindow(secondWindow).containsInAnyOrder(0, 1);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testValueStateSameId() {
        final String stateId = "foo";

        DoFn<KV<String, Integer>, KV<String, Integer>> fn =
                new DoFn<KV<String, Integer>, KV<String, Integer>>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> intState =
                            StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<Integer> state,
                            OutputReceiver<KV<String, Integer>> r) {
                        Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
                        r.output(KV.of("sizzle", currentValue));
                        state.write(currentValue + 1);
                    }
                };

        DoFn<KV<String, Integer>, Integer> fn2 =
                new DoFn<KV<String, Integer>, Integer>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> intState =
                            StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<Integer> state, OutputReceiver<Integer> r) {
                        Integer currentValue = MoreObjects.firstNonNull(state.read(), 13);
                        r.output(currentValue);
                        state.write(currentValue + 13);
                    }
                };

        PCollection<KV<String, Integer>> intermediate =
                pipeline
                        .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
                        .apply("First stateful ParDo", ParDo.of(fn));

        PCollection<Integer> output = intermediate.apply("Second stateful ParDo", ParDo.of(fn2));

        PAssert.that(intermediate)
                .containsInAnyOrder(KV.of("sizzle", 0), KV.of("sizzle", 1), KV.of("sizzle", 2));
        PAssert.that(output).containsInAnyOrder(13, 26, 39);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testValueStateTaggedOutput() {
        final String stateId = "foo";

        final TupleTag<Integer> evenTag = new TupleTag<Integer>() {};
        final TupleTag<Integer> oddTag = new TupleTag<Integer>() {};

        DoFn<KV<String, Integer>, Integer> fn =
                new DoFn<KV<String, Integer>, Integer>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<Integer>> intState =
                            StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<Integer> state, MultiOutputReceiver r) {
                        Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
                        if (currentValue % 2 == 0) {
                            r.get(evenTag).output(currentValue);
                        } else {
                            r.get(oddTag).output(currentValue);
                        }
                        state.write(currentValue + 1);
                    }
                };

        PCollectionTuple output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", 42),
                                        KV.of("hello", 97),
                                        KV.of("hello", 84),
                                        KV.of("goodbye", 33),
                                        KV.of("hello", 859),
                                        KV.of("goodbye", 83945)))
                        .apply(ParDo.of(fn).withOutputTags(evenTag, TupleTagList.of(oddTag)));

        PCollection<Integer> evens = output.get(evenTag);
        PCollection<Integer> odds = output.get(oddTag);

        // There are 0 and 2 from "hello" and just 0 from "goodbye"
        PAssert.that(evens).containsInAnyOrder(0, 2, 0);

        // There are 1 and 3 from "hello" and just "1" from "goodbye"
        PAssert.that(odds).containsInAnyOrder(1, 3, 1);
        pipeline.run();
    }

    @Test
    @Ignore
    public void testBagState() {
        final String stateId = "foo";

        DoFn<KV<String, Integer>, List<Integer>> fn =
                new DoFn<KV<String, Integer>, List<Integer>>() {

                    @StateId(stateId)
                    private final StateSpec<BagState<Integer>> bufferState =
                            StateSpecs.bag(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) BagState<Integer> state,
                            OutputReceiver<List<Integer>> r) {
                        ReadableState<Boolean> isEmpty = state.isEmpty();
                        state.add(element.getValue());
                        assertFalse(isEmpty.read());
                        Iterable<Integer> currentValue = state.read();
                        if (Iterables.size(currentValue) >= 4) {
                            // Make sure that the cached Iterable doesn't change when new elements are added.
                            state.add(-1);
                            assertEquals(4, Iterables.size(currentValue));
                            assertEquals(5, Iterables.size(state.read()));

                            List<Integer> sorted = Lists.newArrayList(currentValue);
                            Collections.sort(sorted);
                            r.output(sorted);
                        }
                    }
                };

        PCollection<List<Integer>> output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", 97),
                                        KV.of("hello", 42),
                                        KV.of("hello", 84),
                                        KV.of("hello", 12)))
                        .apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(Lists.newArrayList(12, 42, 84, 97));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testSetState() {
        final String stateId = "foo";
        final String countStateId = "count";

        DoFn<KV<String, Integer>, Set<Integer>> fn =
                new DoFn<KV<String, Integer>, Set<Integer>>() {

                    @StateId(stateId)
                    private final StateSpec<SetState<Integer>> setState = StateSpecs.set(VarIntCoder.of());

                    @StateId(countStateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                            StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) SetState<Integer> state,
                            @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                            OutputReceiver<Set<Integer>> r) {
                        ReadableState<Boolean> isEmpty = state.isEmpty();
                        state.add(element.getValue());
                        assertFalse(isEmpty.read());
                        count.add(1);
                        if (count.read() >= 4) {
                            // Make sure that the cached Iterable doesn't change when new elements are added.
                            Iterable<Integer> ints = state.read();
                            state.add(-1);
                            assertEquals(3, Iterables.size(ints));
                            assertEquals(4, Iterables.size(state.read()));

                            Set<Integer> set = Sets.newHashSet(ints);
                            r.output(set);
                        }
                    }
                };

        PCollection<Set<Integer>> output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", 97),
                                        KV.of("hello", 42),
                                        KV.of("hello", 42),
                                        KV.of("hello", 12)))
                        .apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(Sets.newHashSet(97, 42, 12));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testMapState() {
        final String stateId = "foo";
        final String countStateId = "count";

        DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>> fn =
                new DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>>() {

                    @StateId(stateId)
                    private final StateSpec<MapState<String, Integer>> mapState =
                            StateSpecs.map(StringUtf8Coder.of(), VarIntCoder.of());

                    @StateId(countStateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                            StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            ProcessContext c,
                            @Element KV<String, KV<String, Integer>> element,
                            @StateId(stateId) MapState<String, Integer> state,
                            @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                            OutputReceiver<KV<String, Integer>> r) {
                        KV<String, Integer> value = element.getValue();
                        ReadableState<Iterable<Map.Entry<String, Integer>>> entriesView = state.entries();
                        state.put(value.getKey(), value.getValue());
                        count.add(1);
                        if (count.read() >= 4) {
                            Iterable<Map.Entry<String, Integer>> iterate = state.entries().read();
                            // Make sure that the cached Iterable doesn't change when new elements are added,
                            // but that cached ReadableState views of the state do change.
                            state.put("BadKey", -1);
                            assertEquals(3, Iterables.size(iterate));
                            assertEquals(4, Iterables.size(entriesView.read()));
                            assertEquals(4, Iterables.size(state.entries().read()));

                            for (Map.Entry<String, Integer> entry : iterate) {
                                r.output(KV.of(entry.getKey(), entry.getValue()));
                            }
                        }
                    }
                };

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                                        KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
                        .apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testCombiningState() {
        final String stateId = "foo";

        DoFn<KV<String, Double>, String> fn =
                new DoFn<KV<String, Double>, String>() {

                    private static final double EPSILON = 0.0001;

                    @StateId(stateId)
                    private final StateSpec<CombiningState<Double, CountSum<Double>, Double>>
                            combiningState = StateSpecs.combining(new CountSumCoder<>(), new MeanFn<>());

                    @ProcessElement
                    public void processElement(
                            ProcessContext c,
                            @Element KV<String, Double> element,
                            @StateId(stateId) CombiningState<Double, CountSum<Double>, Double> state,
                            OutputReceiver<String> r) {
                        state.add(element.getValue());
                        Double currentValue = state.read();
                        if (Math.abs(currentValue - 0.5) < EPSILON) {
                            r.output("right on");
                        }
                    }
                };

        PCollection<String> output =
                pipeline
                        .apply(Create.of(KV.of("hello", 0.3), KV.of("hello", 0.6), KV.of("hello", 0.6)))
                        .apply(ParDo.of(fn));

        // There should only be one moment at which the average is exactly 0.5
        PAssert.that(output).containsInAnyOrder("right on");
        pipeline.run();
    }

    @Test
    @Ignore
    public void testCombiningStateParameterSuperclass() {
        final String stateId = "foo";

        DoFn<KV<Integer, Integer>, String> fn =
                new DoFn<KV<Integer, Integer>, String>() {
                    private static final int EXPECTED_SUM = 8;

                    @StateId(stateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> state =
                            StateSpecs.combining(Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            @Element KV<Integer, Integer> element,
                            @StateId(stateId) GroupingState<Integer, Integer> state,
                            OutputReceiver<String> r) {
                        state.add(element.getValue());
                        Integer currentValue = state.read();
                        if (currentValue == EXPECTED_SUM) {
                            r.output("right on");
                        }
                    }
                };

        PCollection<String> output =
                pipeline
                        .apply(Create.of(KV.of(123, 4), KV.of(123, 7), KV.of(123, -3)))
                        .apply(ParDo.of(fn));

        // There should only be one moment at which the sum is exactly 8
        PAssert.that(output).containsInAnyOrder("right on");
        pipeline.run();
    }

    @Test
    @Ignore
    public void testBagStateSideInput() {

        final PCollectionView<List<Integer>> listView =
                pipeline.apply("Create list for side input", Create.of(2, 1, 0)).apply(View.asList());

        final String stateId = "foo";
        DoFn<KV<String, Integer>, List<Integer>> fn =
                new DoFn<KV<String, Integer>, List<Integer>>() {

                    @StateId(stateId)
                    private final StateSpec<BagState<Integer>> bufferState =
                            StateSpecs.bag(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(
                            ProcessContext c,
                            @Element KV<String, Integer> element,
                            @StateId(stateId) BagState<Integer> state,
                            OutputReceiver<List<Integer>> r) {
                        state.add(element.getValue());
                        Iterable<Integer> currentValue = state.read();
                        if (Iterables.size(currentValue) >= 4) {
                            List<Integer> sorted = Lists.newArrayList(currentValue);
                            Collections.sort(sorted);
                            r.output(sorted);

                            List<Integer> sideSorted = Lists.newArrayList(c.sideInput(listView));
                            Collections.sort(sideSorted);
                            r.output(sideSorted);
                        }
                    }
                };

        PCollection<List<Integer>> output =
                pipeline
                        .apply(
                                "Create main input",
                                Create.of(
                                        KV.of("hello", 97),
                                        KV.of("hello", 42),
                                        KV.of("hello", 84),
                                        KV.of("hello", 12)))
                        .apply(ParDo.of(fn).withSideInputs(listView));

        PAssert.that(output)
                .containsInAnyOrder(Lists.newArrayList(12, 42, 84, 97), Lists.newArrayList(0, 1, 2));
        pipeline.run();
    }

    static class CountSum<NumT extends Number> implements Combine.AccumulatingCombineFn.Accumulator<NumT, CountSum<NumT>, Double> {

        long count = 0;
        double sum = 0.0;

        public CountSum() {
            this(0, 0);
        }

        public CountSum(long count, double sum) {
            this.count = count;
            this.sum = sum;
        }

        @Override
        public void addInput(NumT element) {
            count++;
            sum += element.doubleValue();
        }

        @Override
        public void mergeAccumulator(CountSum<NumT> accumulator) {
            count += accumulator.count;
            sum += accumulator.sum;
        }

        @Override
        public Double extractOutput() {
            return count == 0 ? Double.NaN : sum / count;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CountSum)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            CountSum<?> otherCountSum = (CountSum<?>) other;
            return (count == otherCountSum.count) && (sum == otherCountSum.sum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, sum);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
        }
    }

    static class CountSumCoder<NumT extends Number> extends AtomicCoder<CountSum<NumT>> {
        private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
        private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

        @Override
        public void encode(CountSum<NumT> value, OutputStream outStream)
                throws CoderException, IOException {
            LONG_CODER.encode(value.count, outStream);
            DOUBLE_CODER.encode(value.sum, outStream);
        }

        @Override
        public CountSum<NumT> decode(InputStream inStream) throws CoderException, IOException {
            return new CountSum<>(LONG_CODER.decode(inStream), DOUBLE_CODER.decode(inStream));
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            LONG_CODER.verifyDeterministic();
            DOUBLE_CODER.verifyDeterministic();
        }
    }

    private static class MeanFn<NumT extends Number>
            extends Combine.AccumulatingCombineFn<NumT, CountSum<NumT>, Double> {
        /**
         * Constructs a combining function that computes the mean over a collection of values of type
         * {@code N}.
         */
        @Override
        public CountSum<NumT> createAccumulator() {
            return new CountSum<>();
        }

        @Override
        public Coder<CountSum<NumT>> getAccumulatorCoder(
                CoderRegistry registry, Coder<NumT> inputCoder) {
            return new CountSumCoder<>();
        }
    }

}
