package com.hazelcast.jet.beam.transforms.combine;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineTest;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

/* "Inspired" by org.apache.beam.sdk.transforms.CombineTest.WindowingTests */
@SuppressWarnings("ALL")
public class WindowingCombineTest extends AbstractCombineTest {

    @Test
    public void testFixedWindowsCombine() {
        PCollection<KV<String, Integer>> input =
                pipeline
                        .apply(
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1L)),
                                        TimestampedValue.of(KV.of("a", 4), new Instant(6L)),
                                        TimestampedValue.of(KV.of("b", 1), new Instant(7L)),
                                        TimestampedValue.of(KV.of("b", 13), new Instant(8L)))
                                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
                        .apply(Window.into(FixedWindows.of(Duration.millis(2))));

        PCollection<Integer> sum =
                input.apply(Values.create()).apply(Combine.globally(new CombineTest.SharedTestBase.SumInts()).withoutDefaults());

        PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new CombineTest.SharedTestBase.TestCombineFn()));

        PAssert.that(sum).containsInAnyOrder(2, 5, 13);
        PAssert.that(sumPerKey)
                .containsInAnyOrder(
                        Arrays.asList(KV.of("a", "11"), KV.of("a", "4"), KV.of("b", "1"), KV.of("b", "13")));
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testFixedWindowsCombineWithContext() {
        PCollection<KV<String, Integer>> perKeyInput =
                pipeline
                        .apply(
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(1L)),
                                        TimestampedValue.of(KV.of("a", 4), new Instant(6L)),
                                        TimestampedValue.of(KV.of("b", 1), new Instant(7L)),
                                        TimestampedValue.of(KV.of("b", 13), new Instant(8L)))
                                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
                        .apply(Window.into(FixedWindows.of(Duration.millis(2))));

        PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

        PCollection<Integer> sum =
                globallyInput.apply("Sum", Combine.globally(new CombineTest.SharedTestBase.SumInts()).withoutDefaults());

        PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

        PCollection<KV<String, String>> combinePerKeyWithContext =
                perKeyInput.apply(
                        Combine.<String, Integer, String>perKey(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallySumView))
                                .withSideInputs(globallySumView));

        PCollection<String> combineGloballyWithContext =
                globallyInput.apply(
                        Combine.globally(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallySumView))
                                .withoutDefaults()
                                .withSideInputs(globallySumView));

        PAssert.that(sum).containsInAnyOrder(2, 5, 13);
        PAssert.that(combinePerKeyWithContext)
                .containsInAnyOrder(
                        Arrays.asList(
                                KV.of("a", "2:11"), KV.of("a", "5:4"), KV.of("b", "5:1"), KV.of("b", "13:13")));
        PAssert.that(combineGloballyWithContext).containsInAnyOrder("2:11", "5:14", "13:13");
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testSlidingWindowsCombine() {
        PCollection<String> input =
                pipeline
                        .apply(
                                Create.timestamped(
                                        TimestampedValue.of("a", new Instant(1L)),
                                        TimestampedValue.of("b", new Instant(2L)),
                                        TimestampedValue.of("c", new Instant(3L))))
                        .apply(Window.into(SlidingWindows.of(Duration.millis(3)).every(Duration.millis(1L))));
        PCollection<List<String>> combined =
                input.apply(
                        Combine.globally(
                                new Combine.CombineFn<String, List<String>, List<String>>() {
                                    @Override
                                    public List<String> createAccumulator() {
                                        return new ArrayList<>();
                                    }

                                    @Override
                                    public List<String> addInput(List<String> accumulator, String input) {
                                        accumulator.add(input);
                                        return accumulator;
                                    }

                                    @Override
                                    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
                                        // Mutate all of the accumulators. Instances should be used in only one
                                        // place, and not
                                        // reused after merging.
                                        List<String> cur = createAccumulator();
                                        for (List<String> accumulator : accumulators) {
                                            accumulator.addAll(cur);
                                            cur = accumulator;
                                        }
                                        return cur;
                                    }

                                    @Override
                                    public List<String> extractOutput(List<String> accumulator) {
                                        List<String> result = new ArrayList<>(accumulator);
                                        Collections.sort(result);
                                        return result;
                                    }
                                })
                                .withoutDefaults());

        PAssert.that(combined)
                .containsInAnyOrder(
                        ImmutableList.of("a"),
                        ImmutableList.of("a", "b"),
                        ImmutableList.of("a", "b", "c"),
                        ImmutableList.of("b", "c"),
                        ImmutableList.of("c"));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testSlidingWindowsCombineWithContext() {
        // [a: 1, 1], [a: 4; b: 1], [b: 13]
        PCollection<KV<String, Integer>> perKeyInput =
                pipeline
                        .apply(
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(2L)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(3L)),
                                        TimestampedValue.of(KV.of("a", 4), new Instant(8L)),
                                        TimestampedValue.of(KV.of("b", 1), new Instant(9L)),
                                        TimestampedValue.of(KV.of("b", 13), new Instant(10L)))
                                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
                        .apply(Window.into(SlidingWindows.of(Duration.millis(2))));

        PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

        PCollection<Integer> sum =
                globallyInput.apply("Sum", Sum.integersGlobally().withoutDefaults());

        PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

        PCollection<KV<String, String>> combinePerKeyWithContext =
                perKeyInput.apply(
                        Combine.<String, Integer, String>perKey(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallySumView))
                                .withSideInputs(globallySumView));

        PCollection<String> combineGloballyWithContext =
                globallyInput.apply(
                        Combine.globally(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallySumView))
                                .withoutDefaults()
                                .withSideInputs(globallySumView));

        PAssert.that(sum).containsInAnyOrder(1, 2, 1, 4, 5, 14, 13);
        PAssert.that(combinePerKeyWithContext)
                .containsInAnyOrder(
                        Arrays.asList(
                                KV.of("a", "1:1"),
                                KV.of("a", "2:11"),
                                KV.of("a", "1:1"),
                                KV.of("a", "4:4"),
                                KV.of("a", "5:4"),
                                KV.of("b", "5:1"),
                                KV.of("b", "14:113"),
                                KV.of("b", "13:13")));
        PAssert.that(combineGloballyWithContext)
                .containsInAnyOrder("1:1", "2:11", "1:1", "4:4", "5:14", "14:113", "13:13");
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testGlobalCombineWithDefaultsAndTriggers() {
        PCollection<Integer> input = pipeline.apply(Create.of(1, 1));

        PCollection<String> output =
                input
                        .apply(
                                Window.<Integer>into(new GlobalWindows())
                                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                        .accumulatingFiredPanes()
                                        .withAllowedLateness(new Duration(0), Window.ClosingBehavior.FIRE_ALWAYS))
                        .apply(Sum.integersGlobally())
                        .apply(ParDo.of(new FormatPaneInfo()));

        // The actual elements produced are nondeterministic. Could be one, could be two.
        // But it should certainly have a final element with the correct final sum.
        PAssert.that(output)
                .satisfies(
                        input1 -> {
                            assertThat(input1, hasItem("2: true"));
                            return null;
                        });

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testSessionsCombine() {
        PCollection<KV<String, Integer>> input =
                pipeline
                        .apply(
                                Create.timestamped(
                                        TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                                        TimestampedValue.of(KV.of("a", 1), new Instant(4L)),
                                        TimestampedValue.of(KV.of("a", 4), new Instant(7L)),
                                        TimestampedValue.of(KV.of("b", 1), new Instant(10L)),
                                        TimestampedValue.of(KV.of("b", 13), new Instant(16L)))
                                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
                        .apply(Window.into(Sessions.withGapDuration(Duration.millis(5))));

        PCollection<Integer> sum =
                input.apply(Values.create()).apply(Combine.globally(new CombineTest.SharedTestBase.SumInts()).withoutDefaults());

        PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new CombineTest.SharedTestBase.TestCombineFn()));

        PAssert.that(sum).containsInAnyOrder(7, 13);
        PAssert.that(sumPerKey)
                .containsInAnyOrder(Arrays.asList(KV.of("a", "114"), KV.of("b", "1"), KV.of("b", "13")));
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testSessionsCombineWithContext() {
        PCollection<KV<String, Integer>> perKeyInput =
                pipeline.apply(
                        Create.timestamped(
                                TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                                TimestampedValue.of(KV.of("a", 1), new Instant(4L)),
                                TimestampedValue.of(KV.of("a", 4), new Instant(7L)),
                                TimestampedValue.of(KV.of("b", 1), new Instant(10L)),
                                TimestampedValue.of(KV.of("b", 13), new Instant(16L)))
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

        PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

        PCollection<Integer> fixedWindowsSum =
                globallyInput
                        .apply("FixedWindows", Window.into(FixedWindows.of(Duration.millis(5))))
                        .apply("Sum", Combine.globally(new CombineTest.SharedTestBase.SumInts()).withoutDefaults());

        PCollectionView<Integer> globallyFixedWindowsView =
                fixedWindowsSum.apply(View.<Integer>asSingleton().withDefaultValue(0));

        PCollection<KV<String, String>> sessionsCombinePerKey =
                perKeyInput
                        .apply(
                                "PerKey Input Sessions",
                                Window.into(Sessions.withGapDuration(Duration.millis(5))))
                        .apply(
                                Combine.<String, Integer, String>perKey(
                                        new CombineTest.SharedTestBase.TestCombineFnWithContext(globallyFixedWindowsView))
                                        .withSideInputs(globallyFixedWindowsView));

        PCollection<String> sessionsCombineGlobally =
                globallyInput
                        .apply(
                                "Globally Input Sessions",
                                Window.into(Sessions.withGapDuration(Duration.millis(5))))
                        .apply(
                                Combine.globally(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallyFixedWindowsView))
                                        .withoutDefaults()
                                        .withSideInputs(globallyFixedWindowsView));

        PAssert.that(fixedWindowsSum).containsInAnyOrder(2, 4, 1, 13);
        PAssert.that(sessionsCombinePerKey)
                .containsInAnyOrder(
                        Arrays.asList(KV.of("a", "1:114"), KV.of("b", "1:1"), KV.of("b", "0:13")));
        PAssert.that(sessionsCombineGlobally).containsInAnyOrder("1:1114", "0:13");
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedCombineEmpty() {
        PCollection<Double> mean =
                pipeline
                        .apply(Create.empty(BigEndianIntegerCoder.of()))
                        .apply(Window.into(FixedWindows.of(Duration.millis(1))))
                        .apply(Combine.globally(new MeanInts()).withoutDefaults());

        PAssert.that(mean).empty();

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testCombineGloballyAsSingletonView() {
        final PCollectionView<Integer> view =
                pipeline
                        .apply("CreateEmptySideInput", Create.empty(BigEndianIntegerCoder.of()))
                        .apply(Sum.integersGlobally().asSingletonView());

        PCollection<Integer> output =
                pipeline
                        .apply("CreateVoidMainInput", Create.of((Void) null))
                        .apply(
                                "OutputSideInput",
                                ParDo.of(
                                        new DoFn<Void, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                c.output(c.sideInput(view));
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.thatSingleton(output).isEqualTo(0);
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testWindowedCombineGloballyAsSingletonView() {
        FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(1));
        final PCollectionView<Integer> view =
                pipeline
                        .apply(
                                "CreateSideInput",
                                Create.timestamped(
                                        TimestampedValue.of(1, new Instant(100)),
                                        TimestampedValue.of(3, new Instant(100))))
                        .apply("WindowSideInput", Window.into(windowFn))
                        .apply("CombineSideInput", Sum.integersGlobally().asSingletonView());

        TimestampedValue<Void> nonEmptyElement = TimestampedValue.of(null, new Instant(100));
        TimestampedValue<Void> emptyElement = TimestampedValue.atMinimumTimestamp(null);
        PCollection<Integer> output =
                pipeline
                        .apply(
                                "CreateMainInput",
                                Create.timestamped(nonEmptyElement, emptyElement).withCoder(VoidCoder.of()))
                        .apply("WindowMainInput", Window.into(windowFn))
                        .apply(
                                "OutputSideInput",
                                ParDo.of(
                                        new DoFn<Void, Integer>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                Integer sideInputValue = c.sideInput(view);
                                                c.output(sideInputValue);
                                            }
                                        })
                                        .withSideInputs(view));

        PAssert.that(output).containsInAnyOrder(4, 0);
        PAssert.that(output)
                .inWindow(windowFn.assignWindow(nonEmptyElement.getTimestamp()))
                .containsInAnyOrder(4);
        PAssert.that(output)
                .inWindow(windowFn.assignWindow(emptyElement.getTimestamp()))
                .containsInAnyOrder(0);
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    /**
     * Tests creation of a global {@link Combine} via Java 8 lambda.
     */
    @Test
    public void testCombineGloballyLambda() {

        PCollection<Integer> output =
                pipeline
                        .apply(Create.of(1, 2, 3, 4))
                        .apply(
                                Combine.globally(
                                        integers -> {
                                            int sum = 0;
                                            for (int i : integers) {
                                                sum += i;
                                            }
                                            return sum;
                                        }));

        PAssert.that(output).containsInAnyOrder(10);
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    /**
     * Tests creation of a global {@link Combine} via a Java 8 method reference.
     */
    @Test
    public void testCombineGloballyInstanceMethodReference() {

        PCollection<Integer> output =
                pipeline.apply(Create.of(1, 2, 3, 4)).apply(Combine.globally(new Summer()::sum));

        PAssert.that(output).containsInAnyOrder(10);
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    private static class FormatPaneInfo extends DoFn<Integer, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element() + ": " + c.pane().isLast());
        }
    }

    private static class Summer implements Serializable {
        public int sum(Iterable<Integer> integers) {
            int sum = 0;
            for (int i : integers) {
                sum += i;
            }
            return sum;
        }
    }

}
