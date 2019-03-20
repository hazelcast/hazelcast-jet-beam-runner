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

import com.hazelcast.jet.beam.transforms.AbstractTransformTest;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings("ALL")
public abstract class AbstractParDoTest extends AbstractTransformTest {

    @Rule
    public transient ExpectedException thrown = ExpectedException.none();

    /** A {@link PipelineOptions} subclass for testing passing to a {@link DoFn}. */
    public interface MyOptions extends PipelineOptions {
        @Default.String("fake option")
        String getFakeOption();

        void setFakeOption(String value);
    }

    /**
     * PAssert "matcher" for expected output.
     */
    static class HasExpectedOutput implements SerializableFunction<Iterable<String>, Void>, Serializable {
        private final List<Integer> inputs;
        private final List<Integer> sideInputs;
        private final String additionalOutput;

        private HasExpectedOutput(List<Integer> inputs, List<Integer> sideInputs, String additionalOutput) {
            this.inputs = inputs;
            this.sideInputs = sideInputs;
            this.additionalOutput = additionalOutput;
        }

        static HasExpectedOutput forInput(List<Integer> inputs) {
            return new HasExpectedOutput(new ArrayList<>(inputs), new ArrayList<>(), "");
        }

        public HasExpectedOutput andSideInputs(Integer... sideInputValues) {
            return new HasExpectedOutput(inputs, Arrays.asList(sideInputValues), additionalOutput);
        }

        public HasExpectedOutput fromOutput(TupleTag<String> outputTag) {
            return fromOutput(outputTag.getId());
        }

        HasExpectedOutput fromOutput(String outputId) {
            return new HasExpectedOutput(inputs, sideInputs, outputId);
        }

        @Override
        public Void apply(Iterable<String> outputs) {
            List<String> processeds = new ArrayList<>();
            List<String> finisheds = new ArrayList<>();
            for (String output : outputs) {
                if (output.contains("finished")) {
                    finisheds.add(output);
                } else {
                    processeds.add(output);
                }
            }

            String sideInputsSuffix;
            if (sideInputs.isEmpty()) {
                sideInputsSuffix = "";
            } else {
                sideInputsSuffix = ": " + sideInputs;
            }

            String additionalOutputPrefix;
            if (additionalOutput.isEmpty()) {
                additionalOutputPrefix = "";
            } else {
                additionalOutputPrefix = additionalOutput + ": ";
            }

            List<String> expectedProcesseds = new ArrayList<>();
            for (Integer input : inputs) {
                expectedProcesseds.add(additionalOutputPrefix + "processing: " + input + sideInputsSuffix);
            }
            String[] expectedProcessedsArray =
                    expectedProcesseds.toArray(new String[expectedProcesseds.size()]);
            assertThat(processeds, containsInAnyOrder(expectedProcessedsArray));

            for (String finished : finisheds) {
                assertEquals(additionalOutputPrefix + "finished", finished);
            }

            return null;
        }
    }

    static class TestNoOutputDoFn extends DoFn<Integer, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {}
    }

    static class TestDoFn extends DoFn<Integer, String> {
        final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
        final List<TupleTag<String>> additionalOutputTupleTags = new ArrayList<>();
        State state = State.NOT_SET_UP;
        TestDoFn() {
        }

        public TestDoFn(
                List<PCollectionView<Integer>> sideInputViews,
                List<TupleTag<String>> additionalOutputTupleTags) {
            this.sideInputViews.addAll(sideInputViews);
            this.additionalOutputTupleTags.addAll(additionalOutputTupleTags);
        }

        @Setup
        public void prepare() {
            assertEquals(State.NOT_SET_UP, state);
            state = State.UNSTARTED;
        }

        @StartBundle
        public void startBundle() {
            assertThat(state, anyOf(equalTo(State.UNSTARTED), equalTo(State.FINISHED)));

            state = State.STARTED;
        }

        @ProcessElement
        public void processElement(ProcessContext c, @Element Integer element) {
            assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
            state = State.PROCESSING;
            outputToAllWithSideInputs(c, "processing: " + element);
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
            state = State.FINISHED;
            c.output("finished", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
            for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
                c.output(
                        additionalOutputTupleTag,
                        additionalOutputTupleTag.getId() + ": " + "finished",
                        BoundedWindow.TIMESTAMP_MIN_VALUE,
                        GlobalWindow.INSTANCE);
            }
        }

        private void outputToAllWithSideInputs(ProcessContext c, String value) {
            if (!sideInputViews.isEmpty()) {
                List<Integer> sideInputValues = new ArrayList<>();
                for (PCollectionView<Integer> sideInputView : sideInputViews) {
                    sideInputValues.add(c.sideInput(sideInputView));
                }
                value += ": " + sideInputValues;
            }
            c.output(value);
            for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
                c.output(additionalOutputTupleTag, additionalOutputTupleTag.getId() + ": " + value);
            }
        }

        enum State {
            NOT_SET_UP,
            UNSTARTED,
            STARTED,
            PROCESSING,
            FINISHED
        }
    }

    static class FnWithSideInputs extends DoFn<String, String> {
        private final PCollectionView<Integer> view;

        FnWithSideInputs(PCollectionView<Integer> view) {
            this.view = view;
        }

        @ProcessElement
        public void processElement(ProcessContext c, @Element String element) {
            c.output(element + ":" + c.sideInput(view));
        }
    }

    static class MyIntegerCoder extends AtomicCoder<MyInteger> {
        private static final MyIntegerCoder INSTANCE = new MyIntegerCoder();

        private final VarIntCoder delegate = VarIntCoder.of();

        public static MyIntegerCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(MyInteger value, OutputStream outStream) throws CoderException, IOException {
            delegate.encode(value.getValue(), outStream);
        }

        @Override
        public MyInteger decode(InputStream inStream) throws CoderException, IOException {
            return new MyInteger(delegate.decode(inStream));
        }
    }

    static class MyInteger implements Comparable<MyInteger> {
        private final int value;

        MyInteger(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof MyInteger)) {
                return false;
            }

            MyInteger myInteger = (MyInteger) o;

            return value == myInteger.value;
        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public int compareTo(MyInteger o) {
            return Integer.compare(this.getValue(), o.getValue());
        }

        @Override
        public String toString() {
            return "MyInteger{" + "value=" + value + '}';
        }
    }
}
