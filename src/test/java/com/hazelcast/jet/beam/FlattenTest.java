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

package com.hazelcast.jet.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.apache.beam.sdk.TestUtils.LINES;
import static org.apache.beam.sdk.TestUtils.LINES2;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/* "Inspired" by org.apache.beam.sdk.transforms.FlattenTest */
@SuppressWarnings("ALL")
public class FlattenTest extends AbstractRunnerTest {

    @Test
    public void testFlattenPCollections() {
        List<List<String>> inputs = Arrays.asList(LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES);

        PCollection<String> output =
                makePCollectionListOfStrings(pipeline, inputs).apply(Flatten.pCollections());

        PAssert.that(output).containsInAnyOrder(flattenLists(inputs));
        pipeline.run();
    }

    @Test
    public void testFlattenPCollectionsSingletonList() {
        PCollection<String> input = pipeline.apply(Create.of(LINES));
        PCollection<String> output = PCollectionList.of(input).apply(Flatten.pCollections());

        assertThat(output, not(equalTo(input)));

        PAssert.that(output).containsInAnyOrder(LINES);
        pipeline.run();
    }

    @Test
    public void testFlattenPCollectionsThenParDo() {
        List<List<String>> inputs = Arrays.asList(LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES);

        PCollection<String> output =
                makePCollectionListOfStrings(pipeline, inputs)
                        .apply(Flatten.pCollections())
                        .apply(ParDo.of(new IdentityFn<>()));

        PAssert.that(output).containsInAnyOrder(flattenLists(inputs));
        pipeline.run();
    }

    @Test
    public void testFlattenPCollectionsEmpty() {
        PCollection<String> output =
                PCollectionList.<String>empty(pipeline)
                        .apply(Flatten.pCollections())
                        .setCoder(StringUtf8Coder.of());

        PAssert.that(output).empty();
        pipeline.run();
    }

    @Test
    @Ignore //todo: requires the DAG to be a Multigraph (Flatten's main inputs contain the same PCollection twice), https://github.com/hazelcast/hazelcast-jet/pull/1332
    public void testFlattenInputMultipleCopies() {
        int count = 5;
        PCollection<Long> longs = pipeline.apply("mkLines", GenerateSequence.from(0).to(count));
        PCollection<Long> biggerLongs =
                pipeline.apply("mkOtherLines", GenerateSequence.from(0).to(count))
                        .apply(
                                MapElements.via(
                                        new SimpleFunction<Long, Long>() {
                                            @Override
                                            public Long apply(Long input) {
                                                return input + 10L;
                                            }
                                        }));

        PCollection<Long> flattened =
                PCollectionList.of(longs).and(longs).and(biggerLongs).apply(Flatten.pCollections());

        List<Long> expectedLongs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            // The duplicated input
            expectedLongs.add((long) i);
            expectedLongs.add((long) i);
            // The bigger longs
            expectedLongs.add(i + 10L);
        }
        PAssert.that(flattened).containsInAnyOrder(expectedLongs);

        pipeline.run();
    }

    @Test
    public void testFlattenMultipleCoders() throws CannotProvideCoderException {
        PCollection<Long> bigEndianLongs =
                pipeline.apply(
                        "BigEndianLongs",
                        Create.of(0L, 1L, 2L, 3L, null, 4L, 5L, null, 6L, 7L, 8L, null, 9L)
                                .withCoder(NullableCoder.of(BigEndianLongCoder.of())));
        PCollection<Long> varLongs =
                pipeline.apply("VarLengthLongs", GenerateSequence.from(0).to(5)).setCoder(VarLongCoder.of());

        PCollection<Long> flattened =
                PCollectionList.of(bigEndianLongs)
                        .and(varLongs)
                        .apply(Flatten.pCollections())
                        .setCoder(NullableCoder.of(VarLongCoder.of()));
        PAssert.that(flattened)
                .containsInAnyOrder(
                        0L, 0L, 1L, 1L, 2L, 3L, 2L, 4L, 5L, 3L, 6L, 7L, 4L, 8L, 9L, null, null, null);
        pipeline.run();
    }

    @Test
    public void testEmptyFlattenAsSideInput() {
        final PCollectionView<Iterable<String>> view =
                PCollectionList.<String>empty(pipeline)
                        .apply(Flatten.pCollections())
                        .setCoder(StringUtf8Coder.of())
                        .apply(View.asIterable());

        PCollection<String> output =
                pipeline.apply(Create.of((Void) null).withCoder(VoidCoder.of()))
                        .apply(
                                ParDo.of(
                                        new DoFn<Void, String>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                for (String side : c.sideInput(view)) {
                                                    c.output(side);
                                                }
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).empty();
        pipeline.run();
    }

    @Test
    public void testFlattenPCollectionsEmptyThenParDo() {
        PCollection<String> output =
                PCollectionList.<String>empty(pipeline)
                        .apply(Flatten.pCollections())
                        .setCoder(StringUtf8Coder.of())
                        .apply(ParDo.of(new IdentityFn<>()));

        PAssert.that(output).empty();
        pipeline.run();
    }

    @Test
    public void testFlattenIterables() {
        PCollection<Iterable<String>> input =
                pipeline.apply(
                        Create.<Iterable<String>>of(LINES).withCoder(IterableCoder.of(StringUtf8Coder.of())));

        PCollection<String> output = input.apply(Flatten.iterables());

        PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

        pipeline.run();
    }

    @Test
    public void testFlattenIterablesLists() {
        PCollection<List<String>> input =
                pipeline.apply(Create.<List<String>>of(LINES).withCoder(ListCoder.of(StringUtf8Coder.of())));

        PCollection<String> output = input.apply(Flatten.iterables());

        PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

        pipeline.run();
    }

    @Test
    public void testFlattenIterablesSets() {
        Set<String> linesSet = ImmutableSet.copyOf(LINES);

        PCollection<Set<String>> input =
                pipeline.apply(Create.<Set<String>>of(linesSet).withCoder(SetCoder.of(StringUtf8Coder.of())));

        PCollection<String> output = input.apply(Flatten.iterables());

        PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

        pipeline.run();
    }

    @Test
    public void testFlattenIterablesCollections() {
        Set<String> linesSet = ImmutableSet.copyOf(LINES);

        PCollection<Collection<String>> input =
                pipeline.apply(
                        Create.<Collection<String>>of(linesSet)
                                .withCoder(CollectionCoder.of(StringUtf8Coder.of())));

        PCollection<String> output = input.apply(Flatten.iterables());

        PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

        pipeline.run();
    }

    @Test
    public void testFlattenIterablesEmpty() {
        PCollection<Iterable<String>> input =
                pipeline.apply(
                        Create.<Iterable<String>>of(NO_LINES)
                                .withCoder(IterableCoder.of(StringUtf8Coder.of())));

        PCollection<String> output = input.apply(Flatten.iterables());

        PAssert.that(output).containsInAnyOrder(NO_LINES_ARRAY);

        pipeline.run();
    }

    @Test
    @Ignore //todo: requires the DAG to be a Multigraph (see the exception thrown), https://github.com/hazelcast/hazelcast-jet/pull/1332
    public void testFlattenMultiplePCollectionsHavingMultipleConsumers() {
        PCollection<String> input = pipeline.apply(Create.of("AA", "BBB", "CC"));
        final TupleTag<String> outputEvenLengthTag = new TupleTag<String>() {};
        final TupleTag<String> outputOddLengthTag = new TupleTag<String>() {};

        PCollectionTuple tuple =
                input.apply(
                        ParDo.of(
                                new DoFn<String, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        if (c.element().length() % 2 == 0) {
                                            c.output(c.element());
                                        } else {
                                            c.output(outputOddLengthTag, c.element());
                                        }
                                    }
                                })
                                .withOutputTags(outputEvenLengthTag, TupleTagList.of(outputOddLengthTag)));

        PCollection<String> outputEvenLength = tuple.get(outputEvenLengthTag);
        PCollection<String> outputOddLength = tuple.get(outputOddLengthTag);

        PCollection<String> outputMerged =
                PCollectionList.of(outputEvenLength).and(outputOddLength).apply(Flatten.pCollections());

        PAssert.that(outputMerged).containsInAnyOrder("AA", "BBB", "CC");
        PAssert.that(outputEvenLength).containsInAnyOrder("AA", "CC");
        PAssert.that(outputOddLength).containsInAnyOrder("BBB");

        pipeline.run();
    }

    private PCollectionList<String> makePCollectionListOfStrings(
            Pipeline p, List<List<String>> lists) {
        return makePCollectionList(p, StringUtf8Coder.of(), lists);
    }

    private <T> PCollectionList<T> makePCollectionList(
            Pipeline p, Coder<T> coder, List<List<T>> lists) {
        List<PCollection<T>> pcs = new ArrayList<>();
        int index = 0;
        for (List<T> list : lists) {
            PCollection<T> pc = p.apply("Create" + (index++), Create.of(list).withCoder(coder));
            pcs.add(pc);
        }
        return PCollectionList.of(pcs);
    }

    private <T> List<T> flattenLists(List<List<T>> lists) {
        List<T> flattened = new ArrayList<>();
        for (List<T> list : lists) {
            flattened.addAll(list);
        }
        return flattened;
    }

    private static class IdentityFn<T> extends DoFn<T, T> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element());
        }
    }

}
