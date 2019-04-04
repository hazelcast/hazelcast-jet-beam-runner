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
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.BasicTests */
@SuppressWarnings("ALL")
public class BasicParDoTest extends AbstractParDoTest {

    @Test
    public void testParDo() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        PCollection<String> output =
                pipeline.apply(Create.of(inputs)).apply(ParDo.of(new TestDoFn()));

        PAssert.that(output).satisfies(HasExpectedOutput.forInput(inputs));
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testParDoEmpty() {
        List<Integer> inputs = Collections.emptyList();

        PCollection<String> output =
                pipeline
                        .apply(Create.of(inputs).withCoder(VarIntCoder.of()))
                        .apply("TestDoFn", ParDo.of(new TestDoFn()));

        PAssert.that(output).satisfies(HasExpectedOutput.forInput(inputs));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testParDoEmptyOutputs() {

        List<Integer> inputs = Arrays.asList();

        PCollection<String> output =
                pipeline
                        .apply(Create.of(inputs).withCoder(VarIntCoder.of()))
                        .apply("TestDoFn", ParDo.of(new TestNoOutputDoFn()));

        PAssert.that(output).empty();

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testParDoInCustomTransform() {

        List<Integer> inputs = Arrays.asList(3, -42, 666);

        PCollection<String> output =
                pipeline
                        .apply(Create.of(inputs))
                        .apply(
                                "CustomTransform",
                                new PTransform<PCollection<Integer>, PCollection<String>>() {
                                    @Override
                                    public PCollection<String> expand(PCollection<Integer> input) {
                                        return input.apply(ParDo.of(new TestDoFn()));
                                    }
                                });

        // Test that Coder inference of the result works through
        // user-defined PTransforms.
        PAssert.that(output).satisfies(HasExpectedOutput.forInput(inputs));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testPipelineOptionsParameter() {
        PCollection<String> results =
                pipeline
                        .apply(Create.of(1))
                        .apply(
                                ParDo.of(
                                        new DoFn<Integer, String>() {
                                            @ProcessElement
                                            public void process(OutputReceiver<String> r, PipelineOptions options) {
                                                r.output(options.as(MyOptions.class).getFakeOption());
                                            }
                                        }));

        String testOptionValue = "not fake anymore";
        pipeline.getOptions().as(MyOptions.class).setFakeOption(testOptionValue);
        PAssert.that(results).containsInAnyOrder("not fake anymore");

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

}
