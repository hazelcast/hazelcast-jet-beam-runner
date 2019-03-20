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

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.MultipleInputsAndOutputTests */
public class MultipleInputsAndOutputParDoTest extends AbstractParDoTest {

    @Test
    public void testParDoWithTaggedOutput() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
        TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1") {};
        TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2") {};
        TupleTag<String> additionalOutputTag3 = new TupleTag<String>("additional3") {};
        TupleTag<String> additionalOutputTagUnwritten = new TupleTag<String>("unwrittenOutput") {};

        PCollectionTuple outputs =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                ParDo.of(
                                        new TestDoFn(
                                                Arrays.asList(),
                                                Arrays.asList(
                                                        additionalOutputTag1,
                                                        additionalOutputTag2,
                                                        additionalOutputTag3)))
                                        .withOutputTags(
                                                mainOutputTag,
                                                TupleTagList.of(additionalOutputTag3)
                                                        .and(additionalOutputTag1)
                                                        .and(additionalOutputTagUnwritten)
                                                        .and(additionalOutputTag2)));

        PAssert.that(outputs.get(mainOutputTag))
                .satisfies(HasExpectedOutput.forInput(inputs));

        PAssert.that(outputs.get(additionalOutputTag1))
                .satisfies(HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag1));
        PAssert.that(outputs.get(additionalOutputTag2))
                .satisfies(HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag2));
        PAssert.that(outputs.get(additionalOutputTag3))
                .satisfies(HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag3));
        PAssert.that(outputs.get(additionalOutputTagUnwritten)).empty();

        pipeline.run();
    }

    @Test
    public void testParDoEmptyWithTaggedOutput() {
        TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
        TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1") {};
        TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2") {};
        TupleTag<String> additionalOutputTag3 = new TupleTag<String>("additional3") {};
        TupleTag<String> additionalOutputTagUnwritten = new TupleTag<String>("unwrittenOutput") {};

        PCollectionTuple outputs =
                pipeline
                        .apply(Create.empty(VarIntCoder.of()))
                        .apply(
                                ParDo.of(
                                        new TestDoFn(
                                                Arrays.asList(),
                                                Arrays.asList(
                                                        additionalOutputTag1,
                                                        additionalOutputTag2,
                                                        additionalOutputTag3)))
                                        .withOutputTags(
                                                mainOutputTag,
                                                TupleTagList.of(additionalOutputTag3)
                                                        .and(additionalOutputTag1)
                                                        .and(additionalOutputTagUnwritten)
                                                        .and(additionalOutputTag2)));

        List<Integer> inputs = Collections.emptyList();
        PAssert.that(outputs.get(mainOutputTag))
                .satisfies(HasExpectedOutput.forInput(inputs));

        PAssert.that(outputs.get(additionalOutputTag1))
                .satisfies(HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag1));
        PAssert.that(outputs.get(additionalOutputTag2))
                .satisfies(HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag2));
        PAssert.that(outputs.get(additionalOutputTag3))
                .satisfies(HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag3));
        PAssert.that(outputs.get(additionalOutputTagUnwritten)).empty();

        pipeline.run();
    }

    @Test
    public void testParDoWithEmptyTaggedOutput() {
        TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
        TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1") {};
        TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2") {};

        PCollectionTuple outputs =
                pipeline
                        .apply(Create.empty(VarIntCoder.of()))
                        .apply(
                                ParDo.of(new TestNoOutputDoFn())
                                        .withOutputTags(
                                                mainOutputTag,
                                                TupleTagList.of(additionalOutputTag1).and(additionalOutputTag2)));

        PAssert.that(outputs.get(mainOutputTag)).empty();

        PAssert.that(outputs.get(additionalOutputTag1)).empty();
        PAssert.that(outputs.get(additionalOutputTag2)).empty();

        pipeline.run();
    }

    @Test
    public void testParDoWithOnlyTaggedOutput() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        final TupleTag<Void> mainOutputTag = new TupleTag<Void>("main") {};
        final TupleTag<Integer> additionalOutputTag = new TupleTag<Integer>("additional") {};

        PCollectionTuple outputs =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                ParDo.of(
                                        new DoFn<Integer, Void>() {
                                            @ProcessElement
                                            public void processElement(
                                                    @Element Integer element, MultiOutputReceiver r) {
                                                r.get(additionalOutputTag).output(element);
                                            }
                                        })
                                        .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

        PAssert.that(outputs.get(mainOutputTag)).empty();
        PAssert.that(outputs.get(additionalOutputTag)).containsInAnyOrder(inputs);

        pipeline.run();
    }

    @Test
    public void testParDoWithSideInputs() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        PCollectionView<Integer> sideInput1 =
                pipeline
                        .apply("CreateSideInput1", Create.of(11))
                        .apply("ViewSideInput1", View.asSingleton());
        PCollectionView<Integer> sideInputUnread =
                pipeline
                        .apply("CreateSideInputUnread", Create.of(-3333))
                        .apply("ViewSideInputUnread", View.asSingleton());
        PCollectionView<Integer> sideInput2 =
                pipeline
                        .apply("CreateSideInput2", Create.of(222))
                        .apply("ViewSideInput2", View.asSingleton());

        PCollection<String> output =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                                        .withSideInputs(sideInput1, sideInputUnread, sideInput2));

        PAssert.that(output)
                .satisfies(HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

        pipeline.run();
    }

    @Test
    public void testParDoWithSideInputsIsCumulative() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        PCollectionView<Integer> sideInput1 =
                pipeline
                        .apply("CreateSideInput1", Create.of(11))
                        .apply("ViewSideInput1", View.asSingleton());
        PCollectionView<Integer> sideInputUnread =
                pipeline
                        .apply("CreateSideInputUnread", Create.of(-3333))
                        .apply("ViewSideInputUnread", View.asSingleton());
        PCollectionView<Integer> sideInput2 =
                pipeline
                        .apply("CreateSideInput2", Create.of(222))
                        .apply("ViewSideInput2", View.asSingleton());

        PCollection<String> output =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                                        .withSideInputs(sideInput1)
                                        .withSideInputs(sideInputUnread)
                                        .withSideInputs(sideInput2));

        PAssert.that(output)
                .satisfies(HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

        pipeline.run();
    }

    @Test
    public void testMultiOutputParDoWithSideInputs() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        final TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
        final TupleTag<Void> additionalOutputTag = new TupleTag<Void>("output") {};

        PCollectionView<Integer> sideInput1 =
                pipeline
                        .apply("CreateSideInput1", Create.of(11))
                        .apply("ViewSideInput1", View.asSingleton());
        PCollectionView<Integer> sideInputUnread =
                pipeline
                        .apply("CreateSideInputUnread", Create.of(-3333))
                        .apply("ViewSideInputUnread", View.asSingleton());
        PCollectionView<Integer> sideInput2 =
                pipeline
                        .apply("CreateSideInput2", Create.of(222))
                        .apply("ViewSideInput2", View.asSingleton());

        PCollectionTuple outputs =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                                        .withSideInputs(sideInput1)
                                        .withSideInputs(sideInputUnread)
                                        .withSideInputs(sideInput2)
                                        .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

        PAssert.that(outputs.get(mainOutputTag))
                .satisfies(HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

        pipeline.run();
    }

    @Test
    public void testMultiOutputParDoWithSideInputsIsCumulative() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        final TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
        final TupleTag<Void> additionalOutputTag = new TupleTag<Void>("output") {};

        PCollectionView<Integer> sideInput1 =
                pipeline
                        .apply("CreateSideInput1", Create.of(11))
                        .apply("ViewSideInput1", View.asSingleton());
        PCollectionView<Integer> sideInputUnread =
                pipeline
                        .apply("CreateSideInputUnread", Create.of(-3333))
                        .apply("ViewSideInputUnread", View.asSingleton());
        PCollectionView<Integer> sideInput2 =
                pipeline
                        .apply("CreateSideInput2", Create.of(222))
                        .apply("ViewSideInput2", View.asSingleton());

        PCollectionTuple outputs =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                                        .withSideInputs(sideInput1)
                                        .withSideInputs(sideInputUnread)
                                        .withSideInputs(sideInput2)
                                        .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

        PAssert.that(outputs.get(mainOutputTag))
                .satisfies(HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

        pipeline.run();
    }

    @Test
    public void testSideInputsWithMultipleWindows() {
        // Tests that the runner can safely run a DoFn that uses side inputs
        // on an input where the element is in multiple windows. The complication is
        // that side inputs are per-window, so the runner has to make sure
        // to process each window individually.

        MutableDateTime mutableNow = Instant.now().toMutableDateTime();
        mutableNow.setMillisOfSecond(0);
        Instant now = mutableNow.toInstant();

        SlidingWindows windowFn = SlidingWindows.of(Duration.standardSeconds(5)).every(Duration.standardSeconds(1));
        PCollectionView<Integer> view = pipeline.apply(Create.of(1)).apply(View.asSingleton());
        PCollection<String> res =
                pipeline
                        .apply(Create.timestamped(TimestampedValue.of("a", now)))
                        .apply(Window.into(windowFn))
                        .apply(ParDo.of(new FnWithSideInputs(view)).withSideInputs(view));

        for (int i = 0; i < 4; ++i) {
            Instant base = now.minus(Duration.standardSeconds(i));
            IntervalWindow window = new IntervalWindow(base, base.plus(Duration.standardSeconds(5)));
            PAssert.that(res).inWindow(window).containsInAnyOrder("a:1");
        }

        pipeline.run();
    }

}
