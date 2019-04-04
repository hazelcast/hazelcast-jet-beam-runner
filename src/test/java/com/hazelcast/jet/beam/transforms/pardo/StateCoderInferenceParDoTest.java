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

package com.hazelcast.jet.beam.transforms.pardo;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.StateCoderInferenceTests */
@SuppressWarnings("ALL")
public class StateCoderInferenceParDoTest extends AbstractParDoTest {

    //todo: enable tests after state & timers are implemented

    @Test
    @Ignore
    public void testBagStateCoderInference() {
        final String stateId = "foo";
        Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
        pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

        DoFn<KV<String, Integer>, List<MyInteger>> fn =
                new DoFn<KV<String, Integer>, List<MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<BagState<MyInteger>> bufferState = StateSpecs.bag();

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) BagState<MyInteger> state,
                            OutputReceiver<List<MyInteger>> r) {
                        state.add(new MyInteger(element.getValue()));
                        Iterable<MyInteger> currentValue = state.read();
                        if (Iterables.size(currentValue) >= 4) {
                            List<MyInteger> sorted = Lists.newArrayList(currentValue);
                            Collections.sort(sorted);
                            r.output(sorted);
                        }
                    }
                };

        PCollection<List<MyInteger>> output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", 97),
                                        KV.of("hello", 42),
                                        KV.of("hello", 84),
                                        KV.of("hello", 12)))
                        .apply(ParDo.of(fn))
                        .setCoder(ListCoder.of(myIntegerCoder));

        PAssert.that(output)
                .containsInAnyOrder(
                        Lists.newArrayList(
                                new MyInteger(12), new MyInteger(42),
                                new MyInteger(84), new MyInteger(97)));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testBagStateCoderInferenceFailure() throws Exception {
        final String stateId = "foo";
        Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

        DoFn<KV<String, Integer>, List<MyInteger>> fn =
                new DoFn<KV<String, Integer>, List<MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<BagState<MyInteger>> bufferState = StateSpecs.bag();

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) BagState<MyInteger> state,
                            OutputReceiver<List<MyInteger>> r) {
                        state.add(new MyInteger(element.getValue()));
                        Iterable<MyInteger> currentValue = state.read();
                        if (Iterables.size(currentValue) >= 4) {
                            List<MyInteger> sorted = Lists.newArrayList(currentValue);
                            Collections.sort(sorted);
                            r.output(sorted);
                        }
                    }
                };

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Unable to infer a coder for BagState and no Coder was specified.");

        pipeline
                .apply(
                        Create.of(
                                KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 84), KV.of("hello", 12)))
                .apply(ParDo.of(fn))
                .setCoder(ListCoder.of(myIntegerCoder));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testSetStateCoderInference() {
        final String stateId = "foo";
        final String countStateId = "count";
        Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
        pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

        DoFn<KV<String, Integer>, Set<MyInteger>> fn =
                new DoFn<KV<String, Integer>, Set<MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<SetState<MyInteger>> setState = StateSpecs.set();

                    @StateId(countStateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                            StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) SetState<MyInteger> state,
                            @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                            OutputReceiver<Set<MyInteger>> r) {
                        state.add(new MyInteger(element.getValue()));
                        count.add(1);
                        if (count.read() >= 4) {
                            Set<MyInteger> set = Sets.newHashSet(state.read());
                            r.output(set);
                        }
                    }
                };

        PCollection<Set<MyInteger>> output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", 97),
                                        KV.of("hello", 42),
                                        KV.of("hello", 42),
                                        KV.of("hello", 12)))
                        .apply(ParDo.of(fn))
                        .setCoder(SetCoder.of(myIntegerCoder));

        PAssert.that(output)
                .containsInAnyOrder(
                        Sets.newHashSet(new MyInteger(97), new MyInteger(42), new MyInteger(12)));
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testSetStateCoderInferenceFailure() throws Exception {
        final String stateId = "foo";
        final String countStateId = "count";
        Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

        DoFn<KV<String, Integer>, Set<MyInteger>> fn =
                new DoFn<KV<String, Integer>, Set<MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<SetState<MyInteger>> setState = StateSpecs.set();

                    @StateId(countStateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                            StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) SetState<MyInteger> state,
                            @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                            OutputReceiver<Set<MyInteger>> r) {
                        state.add(new MyInteger(element.getValue()));
                        count.add(1);
                        if (count.read() >= 4) {
                            Set<MyInteger> set = Sets.newHashSet(state.read());
                            r.output(set);
                        }
                    }
                };

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Unable to infer a coder for SetState and no Coder was specified.");

        pipeline
                .apply(
                        Create.of(
                                KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 42), KV.of("hello", 12)))
                .apply(ParDo.of(fn))
                .setCoder(SetCoder.of(myIntegerCoder));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testMapStateCoderInference() {
        final String stateId = "foo";
        final String countStateId = "count";
        Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
        pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

        DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>> fn =
                new DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<MapState<String, MyInteger>> mapState = StateSpecs.map();

                    @StateId(countStateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                            StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, KV<String, Integer>> element,
                            @StateId(stateId) MapState<String, MyInteger> state,
                            @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                            OutputReceiver<KV<String, MyInteger>> r) {
                        KV<String, Integer> value = element.getValue();
                        state.put(value.getKey(), new MyInteger(value.getValue()));
                        count.add(1);
                        if (count.read() >= 4) {
                            Iterable<Map.Entry<String, MyInteger>> iterate = state.entries().read();
                            for (Map.Entry<String, MyInteger> entry : iterate) {
                                r.output(KV.of(entry.getKey(), entry.getValue()));
                            }
                        }
                    }
                };

        PCollection<KV<String, MyInteger>> output =
                pipeline
                        .apply(
                                Create.of(
                                        KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                                        KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
                        .apply(ParDo.of(fn))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), myIntegerCoder));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("a", new MyInteger(97)),
                        KV.of("b", new MyInteger(42)),
                        KV.of("c", new MyInteger(12)));
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testMapStateCoderInferenceFailure() throws Exception {
        final String stateId = "foo";
        final String countStateId = "count";
        Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

        DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>> fn =
                new DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>>() {

                    @StateId(stateId)
                    private final StateSpec<MapState<String, MyInteger>> mapState = StateSpecs.map();

                    @StateId(countStateId)
                    private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                            StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

                    @ProcessElement
                    public void processElement(
                            ProcessContext c,
                            @Element KV<String, KV<String, Integer>> element,
                            @StateId(stateId) MapState<String, MyInteger> state,
                            @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                            OutputReceiver<KV<String, MyInteger>> r) {
                        KV<String, Integer> value = element.getValue();
                        state.put(value.getKey(), new MyInteger(value.getValue()));
                        count.add(1);
                        if (count.read() >= 4) {
                            Iterable<Map.Entry<String, MyInteger>> iterate = state.entries().read();
                            for (Map.Entry<String, MyInteger> entry : iterate) {
                                r.output(KV.of(entry.getKey(), entry.getValue()));
                            }
                        }
                    }
                };

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Unable to infer a coder for MapState and no Coder was specified.");

        pipeline
                .apply(
                        Create.of(
                                KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                                KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
                .apply(ParDo.of(fn))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), myIntegerCoder));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testCombiningStateCoderInference() {
        pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, MyIntegerCoder.of());

        final String stateId = "foo";

        DoFn<KV<String, Integer>, String> fn =
                new DoFn<KV<String, Integer>, String>() {
                    private static final int EXPECTED_SUM = 16;

                    @StateId(stateId)
                    private final StateSpec<CombiningState<Integer, MyInteger, Integer>> combiningState =
                            StateSpecs.combining(
                                    new Combine.CombineFn<Integer, MyInteger, Integer>() {
                                        @Override
                                        public MyInteger createAccumulator() {
                                            return new MyInteger(0);
                                        }

                                        @Override
                                        public MyInteger addInput(MyInteger accumulator, Integer input) {
                                            return new MyInteger(accumulator.getValue() + input);
                                        }

                                        @Override
                                        public MyInteger mergeAccumulators(Iterable<MyInteger> accumulators) {
                                            int newValue = 0;
                                            for (MyInteger myInteger : accumulators) {
                                                newValue += myInteger.getValue();
                                            }
                                            return new MyInteger(newValue);
                                        }

                                        @Override
                                        public Integer extractOutput(MyInteger accumulator) {
                                            return accumulator.getValue();
                                        }
                                    });

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) CombiningState<Integer, MyInteger, Integer> state,
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
                        .apply(Create.of(KV.of("hello", 3), KV.of("hello", 6), KV.of("hello", 7)))
                        .apply(ParDo.of(fn));

        // There should only be one moment at which the average is exactly 16
        PAssert.that(output).containsInAnyOrder("right on");
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    @Ignore
    public void testCombiningStateCoderInferenceFailure()  {
        final String stateId = "foo";

        DoFn<KV<String, Integer>, String> fn =
                new DoFn<KV<String, Integer>, String>() {
                    private static final int EXPECTED_SUM = 16;

                    @StateId(stateId)
                    private final StateSpec<CombiningState<Integer, MyInteger, Integer>> combiningState =
                            StateSpecs.combining(
                                    new Combine.CombineFn<Integer, MyInteger, Integer>() {
                                        @Override
                                        public MyInteger createAccumulator() {
                                            return new MyInteger(0);
                                        }

                                        @Override
                                        public MyInteger addInput(MyInteger accumulator, Integer input) {
                                            return new MyInteger(accumulator.getValue() + input);
                                        }

                                        @Override
                                        public MyInteger mergeAccumulators(Iterable<MyInteger> accumulators) {
                                            int newValue = 0;
                                            for (MyInteger myInteger : accumulators) {
                                                newValue += myInteger.getValue();
                                            }
                                            return new MyInteger(newValue);
                                        }

                                        @Override
                                        public Integer extractOutput(MyInteger accumulator) {
                                            return accumulator.getValue();
                                        }
                                    });

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, Integer> element,
                            @StateId(stateId) CombiningState<Integer, MyInteger, Integer> state,
                            OutputReceiver<String> r) {
                        state.add(element.getValue());
                        Integer currentValue = state.read();
                        if (currentValue == EXPECTED_SUM) {
                            r.output("right on");
                        }
                    }
                };

        thrown.expect(RuntimeException.class);
        thrown.expectMessage(
                "Unable to infer a coder for CombiningState and no Coder was specified.");

        pipeline
                .apply(Create.of(KV.of("hello", 3), KV.of("hello", 6), KV.of("hello", 7)))
                .apply(ParDo.of(fn));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

}
