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

package com.hazelcast.jet.beam.transforms;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/* "Inspired" by org.apache.beam.sdk.transforms.ViewTest */
@SuppressWarnings("ALL")
public class ViewTest extends AbstractTransformTest {

    @Test
    public void testSingletonSideInput() {

        final PCollectionView<Integer> view =
                pipeline.apply("Create47", Create.of(47)).apply(View.asSingleton());

        PCollection<Integer> output =
                pipeline
                        .apply("Create123", Create.of(1, 2, 3))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.sideInput(view));
                                            }
                                        })
                                        .withSideInputs(view));

        //PAssert.that(output).containsInAnyOrder(47, 47, 47); //todo

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedSingletonSideInput() {
        final PCollectionView<Integer> view =
                pipeline
                        .apply(
                                "Create47",
                                Create.timestamped(
                                        TimestampedValue.of(47, new Instant(1)),
                                        TimestampedValue.of(48, new Instant(11))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asSingleton());

        PCollection<Integer> output =
                pipeline
                        .apply(
                                "Create123",
                                Create.timestamped(
                                        TimestampedValue.of(1, new Instant(4)),
                                        TimestampedValue.of(2, new Instant(8)),
                                        TimestampedValue.of(3, new Instant(12))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.sideInput(view));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(47, 47, 48);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testListSideInput() {

        final PCollectionView<List<Integer>> view =
                pipeline.apply("CreateSideInput", Create.of(11, 13, 17, 23)).apply(View.asList());

        PCollection<Integer> output =
                pipeline
                        .apply("CreateMainInput", Create.of(29, 31))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                checkArgument(c.sideInput(view).size() == 4);
                                                checkArgument(
                                                        c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                                                for (Integer i : c.sideInput(view)) {
                                                    c.output(i);
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedListSideInput() {

        final PCollectionView<List<Integer>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(11, new Instant(1)),
                                        TimestampedValue.of(13, new Instant(1)),
                                        TimestampedValue.of(17, new Instant(1)),
                                        TimestampedValue.of(23, new Instant(1)),
                                        TimestampedValue.of(31, new Instant(11)),
                                        TimestampedValue.of(33, new Instant(11)),
                                        TimestampedValue.of(37, new Instant(11)),
                                        TimestampedValue.of(43, new Instant(11))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asList());

        PCollection<Integer> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of(29, new Instant(1)),
                                        TimestampedValue.of(35, new Instant(11))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                checkArgument(c.sideInput(view).size() == 4);
                                                checkArgument(
                                                        c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                                                for (Integer i : c.sideInput(view)) {
                                                    c.output(i);
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 31, 33, 37, 43);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testEmptyListSideInput() throws Exception {

        final PCollectionView<List<Integer>> view =
                pipeline.apply("CreateEmptyView", Create.empty(VarIntCoder.of())).apply(View.asList());

        PCollection<Integer> results =
                pipeline
                        .apply("Create1", Create.of(1))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertTrue(c.sideInput(view).isEmpty());
                                                assertFalse(c.sideInput(view).iterator().hasNext());
                                                c.output(1);
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(results).containsInAnyOrder(1);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testListSideInputIsImmutable() {

        final PCollectionView<List<Integer>> view =
                pipeline.apply("CreateSideInput", Create.of(11)).apply(View.asList());

        PCollection<Integer> output =
                pipeline
                        .apply("CreateMainInput", Create.of(29))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                try {
                                                    c.sideInput(view).clear();
                                                    fail("Expected UnsupportedOperationException on clear()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).add(4);
                                                    fail("Expected UnsupportedOperationException on add()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).addAll(new ArrayList<>());
                                                    fail("Expected UnsupportedOperationException on addAll()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).remove(0);
                                                    fail("Expected UnsupportedOperationException on remove()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                for (Integer i : c.sideInput(view)) {
                                                    c.output(i);
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(output).containsInAnyOrder(11);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testIterableSideInput() {

        final PCollectionView<Iterable<Integer>> view =
                pipeline.apply("CreateSideInput", Create.of(11, 13, 17, 23))
                        .apply(View.asIterable());

        PCollection<Integer> output =
                pipeline
                        .apply("CreateMainInput", Create.of(29, 31))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Integer i : c.sideInput(view)) {
                                                    c.output(i);
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedIterableSideInput() {

        final PCollectionView<Iterable<Integer>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(11, new Instant(1)),
                                        TimestampedValue.of(13, new Instant(1)),
                                        TimestampedValue.of(17, new Instant(1)),
                                        TimestampedValue.of(23, new Instant(1)),
                                        TimestampedValue.of(31, new Instant(11)),
                                        TimestampedValue.of(33, new Instant(11)),
                                        TimestampedValue.of(37, new Instant(11)),
                                        TimestampedValue.of(43, new Instant(11))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asIterable());

        PCollection<Integer> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of(29, new Instant(1)),
                                        TimestampedValue.of(35, new Instant(11))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Integer i : c.sideInput(view)) {
                                                    c.output(i);
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 31, 33, 37, 43);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testEmptyIterableSideInput() throws Exception {

        final PCollectionView<Iterable<Integer>> view =
                pipeline.apply("CreateEmptyView", Create.empty(VarIntCoder.of())).apply(View.asIterable());

        PCollection<Integer> results =
                pipeline
                        .apply("Create1", Create.of(1))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertFalse(c.sideInput(view).iterator().hasNext());
                                                c.output(1);
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(results).containsInAnyOrder(1);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testIterableSideInputIsImmutable() {

        final PCollectionView<Iterable<Integer>> view =
                pipeline.apply("CreateSideInput", Create.of(11)).apply(View.asIterable());

        PCollection<Integer> output =
                pipeline
                        .apply("CreateMainInput", Create.of(29))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                Iterator<Integer> iterator = c.sideInput(view).iterator();
                                                while (iterator.hasNext()) {
                                                    try {
                                                        iterator.remove();
                                                        fail("Expected UnsupportedOperationException on remove()");
                                                    } catch (UnsupportedOperationException expected) {
                                                    }
                                                    c.output(iterator.next());
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(output).containsInAnyOrder(11);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMultimapSideInput() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
                        .apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                                                    c.output(KV.of(c.element(), v));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("apple", 1),
                        KV.of("apple", 1),
                        KV.of("apple", 2),
                        KV.of("banana", 3),
                        KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMultimapAsEntrySetSideInput() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
                        .apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of(2 /* size */))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertEquals((int) c.element(), c.sideInput(view).size());
                                                assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                                                for (Map.Entry<String, Iterable<Integer>> entry :
                                                        c.sideInput(view).entrySet()) {
                                                    for (Integer value : entry.getValue()) {
                                                        c.output(KV.of(entry.getKey(), value));
                                                    }
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMultimapSideInputWithNonDeterministicKeyCoder() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3))
                                        .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
                        .apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                                                    c.output(KV.of(c.element(), v));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("apple", 1),
                        KV.of("apple", 1),
                        KV.of("apple", 2),
                        KV.of("banana", 3),
                        KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedMultimapSideInput() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(2)),
                                        TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                                        TimestampedValue.of(KV.of("b", 3), new Instant(14))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("apple", new Instant(5)),
                                        TimestampedValue.of("banana", new Instant(13)),
                                        TimestampedValue.of("blackberry", new Instant(16))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                                                    c.output(KV.of(c.element(), v));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("apple", 1),
                        KV.of("apple", 1),
                        KV.of("apple", 2),
                        KV.of("banana", 3),
                        KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedMultimapAsEntrySetSideInput() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(2)),
                                        TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                                        TimestampedValue.of(KV.of("b", 3), new Instant(14))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of(1 /* size */, new Instant(5)),
                                        TimestampedValue.of(1 /* size */, new Instant(16))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertEquals((int) c.element(), c.sideInput(view).size());
                                                assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                                                for (Map.Entry<String, Iterable<Integer>> entry :
                                                        c.sideInput(view).entrySet()) {
                                                    for (Integer value : entry.getValue()) {
                                                        c.output(KV.of(entry.getKey(), value));
                                                    }
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedMultimapSideInputWithNonDeterministicKeyCoder() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(2)),
                                        TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                                        TimestampedValue.of(KV.of("b", 3), new Instant(14)))
                                        .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("apple", new Instant(5)),
                                        TimestampedValue.of("banana", new Instant(13)),
                                        TimestampedValue.of("blackberry", new Instant(16))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                                                    c.output(KV.of(c.element(), v));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("apple", 1),
                        KV.of("apple", 1),
                        KV.of("apple", 2),
                        KV.of("banana", 3),
                        KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testEmptyMultimapSideInput() throws Exception {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateEmptyView", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
                        .apply(View.asMultimap());

        PCollection<Integer> results =
                pipeline
                        .apply("Create1", Create.of(1))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertTrue(c.sideInput(view).isEmpty());
                                                assertTrue(c.sideInput(view).entrySet().isEmpty());
                                                assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                                                c.output(c.element());
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(results).containsInAnyOrder(1);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testEmptyMultimapSideInputWithNonDeterministicKeyCoder() throws Exception {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline
                        .apply(
                                "CreateEmptyView",
                                Create.empty(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
                        .apply(View.asMultimap());

        PCollection<Integer> results =
                pipeline
                        .apply("Create1", Create.of(1))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertTrue(c.sideInput(view).isEmpty());
                                                assertTrue(c.sideInput(view).entrySet().isEmpty());
                                                assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                                                c.output(c.element());
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(results).containsInAnyOrder(1);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMultimapSideInputIsImmutable() {

        final PCollectionView<Map<String, Iterable<Integer>>> view =
                pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1))).apply(View.asMultimap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple"))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                try {
                                                    c.sideInput(view).clear();
                                                    fail("Expected UnsupportedOperationException on clear()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).put("c", ImmutableList.of(3));
                                                    fail("Expected UnsupportedOperationException on put()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).remove("c");
                                                    fail("Expected UnsupportedOperationException on remove()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).putAll(new HashMap<>());
                                                    fail("Expected UnsupportedOperationException on putAll()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                                                    c.output(KV.of(c.element(), v));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(output).containsInAnyOrder(KV.of("apple", 1));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMapSideInput() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(
                                                        KV.of(
                                                                c.element(),
                                                                c.sideInput(view).get(c.element().substring(0, 1))));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMapAsEntrySetSideInput() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of(2 /* size */))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertEquals((int) c.element(), c.sideInput(view).size());
                                                assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                                                for (Map.Entry<String, Integer> entry : c.sideInput(view).entrySet()) {
                                                    c.output(KV.of(entry.getKey(), entry.getValue()));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(KV.of("a", 1), KV.of("b", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMapSideInputWithNonDeterministicKeyCoder() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.of(KV.of("a", 1), KV.of("b", 3))
                                        .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(
                                                        KV.of(
                                                                c.element(),
                                                                c.sideInput(view).get(c.element().substring(0, 1))));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedMapSideInput() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                        TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                                        TimestampedValue.of(KV.of("b", 3), new Instant(18))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("apple", new Instant(5)),
                                        TimestampedValue.of("banana", new Instant(4)),
                                        TimestampedValue.of("blackberry", new Instant(16))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(
                                                        KV.of(
                                                                c.element(),
                                                                c.sideInput(view).get(c.element().substring(0, 1))));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 2), KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedMapAsEntrySetSideInput() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                        TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                                        TimestampedValue.of(KV.of("b", 3), new Instant(18))))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of(2 /* size */, new Instant(5)),
                                        TimestampedValue.of(1 /* size */, new Instant(16))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertEquals((int) c.element(), c.sideInput(view).size());
                                                assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                                                for (Map.Entry<String, Integer> entry : c.sideInput(view).entrySet()) {
                                                    c.output(KV.of(entry.getKey(), entry.getValue()));
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(KV.of("a", 1), KV.of("b", 2), KV.of("b", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedMapSideInputWithNonDeterministicKeyCoder() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                        TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                                        TimestampedValue.of(KV.of("b", 3), new Instant(18)))
                                        .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
                        .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("apple", new Instant(5)),
                                        TimestampedValue.of("banana", new Instant(4)),
                                        TimestampedValue.of("blackberry", new Instant(16))))
                        .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(
                                                        KV.of(
                                                                c.element(),
                                                                c.sideInput(view).get(c.element().substring(0, 1))));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 2), KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testEmptyMapSideInput() throws Exception {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply(
                                "CreateEmptyView", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
                        .apply(View.asMap());

        PCollection<Integer> results =
                pipeline
                        .apply("Create1", Create.of(1))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertTrue(c.sideInput(view).isEmpty());
                                                assertTrue(c.sideInput(view).entrySet().isEmpty());
                                                assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                                                c.output(c.element());
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(results).containsInAnyOrder(1);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testEmptyMapSideInputWithNonDeterministicKeyCoder() throws Exception {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply(
                                "CreateEmptyView",
                                Create.empty(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
                        .apply(View.asMap());

        PCollection<Integer> results =
                pipeline
                        .apply("Create1", Create.of(1))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<Integer, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                assertTrue(c.sideInput(view).isEmpty());
                                                assertTrue(c.sideInput(view).entrySet().isEmpty());
                                                assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                                                c.output(c.element());
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(results).containsInAnyOrder(1);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testMapSideInputIsImmutable() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1))).apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple"))
                        .apply(
                                "OutputSideInputs",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                try {
                                                    c.sideInput(view).clear();
                                                    fail("Expected UnsupportedOperationException on clear()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).put("c", 3);
                                                    fail("Expected UnsupportedOperationException on put()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).remove("c");
                                                    fail("Expected UnsupportedOperationException on remove()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                try {
                                                    c.sideInput(view).putAll(new HashMap<>());
                                                    fail("Expected UnsupportedOperationException on putAll()");
                                                } catch (UnsupportedOperationException expected) {
                                                }
                                                c.output(
                                                        KV.of(
                                                                c.element(),
                                                                c.sideInput(view).get(c.element().substring(0, 1))));
                                            }
                                        })
                                        .withSideInputs(view));

        // Pass at least one value through to guarantee that DoFn executes.
        PAssert.that(output).containsInAnyOrder(KV.of("apple", 1));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testCombinedMapSideInput() {

        final PCollectionView<Map<String, Integer>> view =
                pipeline
                        .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 20), KV.of("b", 3)))
                        .apply("SumIntegers", Combine.perKey(Sum.ofIntegers()))
                        .apply(View.asMap());

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
                        .apply(
                                "Output",
                                ParDo.of(
                                        new DoFn<String, KV<String, Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(
                                                        KV.of(
                                                                c.element(),
                                                                c.sideInput(view).get(c.element().substring(0, 1))));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output)
                .containsInAnyOrder(KV.of("apple", 21), KV.of("banana", 3), KV.of("blackberry", 3));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedSideInputFixedToFixed() {

        final PCollectionView<Integer> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(1, new Instant(1)),
                                        TimestampedValue.of(2, new Instant(11)),
                                        TimestampedValue.of(3, new Instant(13))))
                        .apply("WindowSideInput", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(Sum.integersGlobally().withoutDefaults())
                        .apply(View.asSingleton());

        PCollection<String> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("A", new Instant(4)),
                                        TimestampedValue.of("B", new Instant(15)),
                                        TimestampedValue.of("C", new Instant(7))))
                        .apply("WindowMainInput", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputMainAndSideInputs",
                                ParDo.of(
                                        new DoFn<String, String>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.element() + c.sideInput(view));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder("A1", "B5", "C1");

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedSideInputFixedToGlobal() {

        final PCollectionView<Integer> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(1, new Instant(1)),
                                        TimestampedValue.of(2, new Instant(11)),
                                        TimestampedValue.of(3, new Instant(13))))
                        .apply("WindowSideInput", Window.into(new GlobalWindows()))
                        .apply(Sum.integersGlobally())
                        .apply(View.asSingleton());

        PCollection<String> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("A", new Instant(4)),
                                        TimestampedValue.of("B", new Instant(15)),
                                        TimestampedValue.of("C", new Instant(7))))
                        .apply("WindowMainInput", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputMainAndSideInputs",
                                ParDo.of(
                                        new DoFn<String, String>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.element() + c.sideInput(view));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder("A6", "B6", "C6");

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedSideInputFixedToFixedWithDefault() {
        final PCollectionView<Integer> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(2, new Instant(11)),
                                        TimestampedValue.of(3, new Instant(13))))
                        .apply("WindowSideInput", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(Sum.integersGlobally().asSingletonView());

        PCollection<String> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(
                                        TimestampedValue.of("A", new Instant(4)),
                                        TimestampedValue.of("B", new Instant(15)),
                                        TimestampedValue.of("C", new Instant(7))))
                        .apply("WindowMainInput", Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(
                                "OutputMainAndSideInputs",
                                ParDo.of(
                                        new DoFn<String, String>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                Integer integer = c.sideInput(view);
                                                c.output(c.element() + integer);
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder("A0", "B5", "C0");

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testSideInputWithNullDefault() {

        final PCollectionView<Void> view =
                pipeline
                        .apply("CreateSideInput", Create.of((Void) null).withCoder(VoidCoder.of()))
                        .apply(Combine.globally((Iterable<Void> input) -> null).asSingletonView());

        @SuppressWarnings("ObjectToString")
        PCollection<String> output =
                pipeline
                        .apply("CreateMainInput", Create.of(""))
                        .apply(
                                "OutputMainAndSideInputs",
                                ParDo.of(
                                        new DoFn<String, String>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.element() + c.sideInput(view));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder("null");

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testSideInputWithNestedIterables() {
        final PCollectionView<Iterable<Integer>> view1 =
                pipeline
                        .apply("CreateVoid1", Create.of((Void) null).withCoder(VoidCoder.of()))
                        .apply(
                                "OutputOneInteger",
                                ParDo.of(
                                        new DoFn<Void, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(17);
                                            }
                                        }))
                        .apply("View1", View.asIterable());

        final PCollectionView<Iterable<Iterable<Integer>>> view2 =
                pipeline
                        .apply("CreateVoid2", Create.of((Void) null).withCoder(VoidCoder.of()))
                        .apply(
                                "OutputSideInput",
                                ParDo.of(
                                        new DoFn<Void, Iterable<Integer>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.sideInput(view1));
                                            }
                                        })
                                        .withSideInputs(view1))
                        .apply("View2", View.asIterable());

        PCollection<Integer> output =
                pipeline
                        .apply("CreateVoid3", Create.of((Void) null).withCoder(VoidCoder.of()))
                        .apply(
                                "ReadIterableSideInput",
                                ParDo.of(
                                        new DoFn<Void, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (Iterable<Integer> input : c.sideInput(view2)) {
                                                    for (Integer i : input) {
                                                        c.output(i);
                                                    }
                                                }
                                            }
                                        })
                                        .withSideInputs(view2));

        PAssert.that(output).containsInAnyOrder(17);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    private static class NonDeterministicStringCoder extends AtomicCoder<String> {
        @Override
        public void encode(String value, OutputStream outStream) throws CoderException, IOException {
            encode(value, outStream, Coder.Context.NESTED);
        }

        @Override
        public void encode(String value, OutputStream outStream, Coder.Context context)
                throws CoderException, IOException {
            StringUtf8Coder.of().encode(value, outStream, context);
        }

        @Override
        public String decode(InputStream inStream) throws CoderException, IOException {
            return decode(inStream, Coder.Context.NESTED);
        }

        @Override
        public String decode(InputStream inStream, Coder.Context context)
                throws CoderException, IOException {
            return StringUtf8Coder.of().decode(inStream, context);
        }

        @Override
        public void verifyDeterministic()
                throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
            throw new NonDeterministicException(this, "Test coder is not deterministic on purpose.");
        }
    }

}
