package com.hazelcast.jet.beam.combine;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

/* "Inspired" by org.apache.beam.sdk.transforms.CombineTest.BasicTests */
@SuppressWarnings({"ALL"})
public class BasicCombineTest extends AbstractCombineTest {

    private static final SerializableFunction<String, Integer> HOT_KEY_FANOUT = input -> "a".equals(input) ? 3 : 0;
    private static final SerializableFunction<String, Integer> SPLIT_HOT_KEY_FANOUT = input -> Math.random() < 0.5 ? 3 : 0;

    @Test
    public void testSimpleCombine() {
        runTestSimpleCombine(
                Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
                20,
                Arrays.asList(KV.of("a", "114"), KV.of("b", "113")));
    }

    @Test
    public void testSimpleCombineEmpty() {
        runTestSimpleCombine(EMPTY_TABLE, 0, Collections.emptyList());
    }

    @Test
    public void testBasicCombine() {
        runTestBasicCombine(
                Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
                ImmutableSet.of(1, 13, 4),
                Arrays.asList(new KV[]{KV.of("a", ImmutableSet.of(1, 4)), KV.of("b", (Set<Integer>) ImmutableSet.of(1, 13))}));
    }

    @Test
    public void testBasicCombineEmpty() {
        runTestBasicCombine(EMPTY_TABLE, ImmutableSet.of(), Collections.emptyList());
    }

    @Test
    public void testHotKeyCombining() {
        PCollection<KV<String, Integer>> input =
                copy(
                        createInput(
                                pipeline,
                                Arrays.asList(
                                        KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13))),
                        10);

        Combine.CombineFn<Integer, ?, Double> mean = new MeanInts();
        PCollection<KV<String, Double>> coldMean =
                input.apply(
                        "ColdMean", Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(0));
        PCollection<KV<String, Double>> warmMean =
                input.apply(
                        "WarmMean",
                        Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(HOT_KEY_FANOUT));
        PCollection<KV<String, Double>> hotMean =
                input.apply("HotMean", Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(5));
        PCollection<KV<String, Double>> splitMean =
                input.apply(
                        "SplitMean",
                        Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(SPLIT_HOT_KEY_FANOUT));

        List<KV<String, Double>> expected = Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0));
        PAssert.that(coldMean).containsInAnyOrder(expected);
        PAssert.that(warmMean).containsInAnyOrder(expected);
        PAssert.that(hotMean).containsInAnyOrder(expected);
        PAssert.that(splitMean).containsInAnyOrder(expected);

        pipeline.run();
    }

    @Test
    public void testHotKeyCombiningWithAccumulationMode() {
        PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5));

        PCollection<Integer> output =
                input
                        .apply(
                                Window.<Integer>into(new GlobalWindows())
                                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                        .accumulatingFiredPanes()
                                        .withAllowedLateness(new Duration(0), Window.ClosingBehavior.FIRE_ALWAYS))
                        .apply(Sum.integersGlobally().withoutDefaults().withFanout(2))
                        .apply(ParDo.of(new GetLast()));

        PAssert.that(output)
                .satisfies(
                        input1 -> {
                            assertThat(input1, hasItem(15));
                            return null;
                        });

        pipeline.run();
    }

    @Test
    public void testCombinePerKeyPrimitiveDisplayData() {
        DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

        CombineTest.SharedTestBase.UniqueInts combineFn = new CombineTest.SharedTestBase.UniqueInts();
        PTransform<PCollection<KV<Integer, Integer>>, ? extends POutput> combine =
                Combine.perKey(combineFn);

        Set<DisplayData> displayData =
                evaluator.displayDataForPrimitiveTransforms(
                        combine, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

        assertThat(
                "Combine.perKey should include the combineFn in its primitive transform",
                displayData,
                hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
    }

    @Test
    public void testCombinePerKeyWithHotKeyFanoutPrimitiveDisplayData() {
        int hotKeyFanout = 2;
        DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

        CombineTest.SharedTestBase.UniqueInts combineFn = new CombineTest.SharedTestBase.UniqueInts();
        PTransform<PCollection<KV<Integer, Integer>>, PCollection<KV<Integer, Set<Integer>>>>
                combine =
                Combine.<Integer, Integer, Set<Integer>>perKey(combineFn)
                        .withHotKeyFanout(hotKeyFanout);

        Set<DisplayData> displayData =
                evaluator.displayDataForPrimitiveTransforms(
                        combine, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

        assertThat(
                "Combine.perKey.withHotKeyFanout should include the combineFn in its primitive "
                        + "transform",
                displayData,
                hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
        assertThat(
                "Combine.perKey.withHotKeyFanout(int) should include the fanout in its primitive "
                        + "transform",
                displayData,
                hasItem(hasDisplayItem("fanout", hotKeyFanout)));
    }

    /**
     * Tests creation of a per-key {@link Combine} via a Java 8 lambda.
     */
    @Test
    public void testCombinePerKeyLambda() {

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
                        .apply(
                                Combine.perKey(
                                        integers -> {
                                            int sum = 0;
                                            for (int i : integers) {
                                                sum += i;
                                            }
                                            return sum;
                                        }));

        PAssert.that(output).containsInAnyOrder(KV.of("a", 4), KV.of("b", 2), KV.of("c", 4));
        pipeline.run();
    }

    /**
     * Tests creation of a per-key {@link Combine} via a Java 8 method reference.
     */
    @Test
    public void testCombinePerKeyInstanceMethodReference() {

        PCollection<KV<String, Integer>> output =
                pipeline
                        .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
                        .apply(Combine.perKey(new Summer()::sum));

        PAssert.that(output).containsInAnyOrder(KV.of("a", 4), KV.of("b", 2), KV.of("c", 4));
        pipeline.run();
    }

    private void runTestSimpleCombine(
            List<KV<String, Integer>> table, int globalSum, List<KV<String, String>> perKeyCombines) {
        PCollection<KV<String, Integer>> input = createInput(pipeline, table);

        PCollection<Integer> sum =
                input.apply(Values.create()).apply(Combine.globally(new CombineTest.SharedTestBase.SumInts()));

        PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new CombineTest.SharedTestBase.TestCombineFn()));

        PAssert.that(sum).containsInAnyOrder(globalSum);
        PAssert.that(sumPerKey).containsInAnyOrder(perKeyCombines);

        pipeline.run();
    }

    private void runTestBasicCombine(
            List<KV<String, Integer>> table,
            Set<Integer> globalUnique,
            List<KV<String, Set<Integer>>> perKeyUnique) {
        PCollection<KV<String, Integer>> input = createInput(pipeline, table);

        PCollection<Set<Integer>> unique =
                input.apply(Values.create()).apply(Combine.globally(new CombineTest.SharedTestBase.UniqueInts()));

        PCollection<KV<String, Set<Integer>>> uniquePerKey =
                input.apply(Combine.perKey(new CombineTest.SharedTestBase.UniqueInts()));

        PAssert.that(unique).containsInAnyOrder(globalUnique);
        PAssert.that(uniquePerKey).containsInAnyOrder(perKeyUnique);

        pipeline.run();
    }

    private static <T> PCollection<T> copy(PCollection<T> pc, final int n) {
        return pc.apply(
                ParDo.of(
                        new DoFn<T, T>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws Exception {
                                for (int i = 0; i < n; i++) {
                                    c.output(c.element());
                                }
                            }
                        }));
    }

    protected static class MeanInts extends Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, Double> {
        private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
        private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

        static class CountSum
                implements Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, Double> {
            long count;
            double sum;

            CountSum(long count, double sum) {
                this.count = count;
                this.sum = sum;
            }

            @Override
            public void addInput(Integer element) {
                count++;
                sum += element.doubleValue();
            }

            @Override
            public void mergeAccumulator(CountSum accumulator) {
                count += accumulator.count;
                sum += accumulator.sum;
            }

            @Override
            public Double extractOutput() {
                return count == 0 ? 0.0 : sum / count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(count, sum);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (!(obj instanceof CountSum)) {
                    return false;
                }
                CountSum other = (CountSum) obj;
                return this.count == other.count && (Math.abs(this.sum - other.sum) < 0.1);
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
            }
        }

        @Override
        public CountSum createAccumulator() {
            return new CountSum(0, 0.0);
        }

        @Override
        public Coder<CountSum> getAccumulatorCoder(
                CoderRegistry registry, Coder<Integer> inputCoder) {
            return new CountSumCoder();
        }

        /** A {@link Coder} for {@link CountSum}. */
        private static class CountSumCoder extends AtomicCoder<CountSum> {
            @Override
            public void encode(CountSum value, OutputStream outStream) throws IOException {
                LONG_CODER.encode(value.count, outStream);
                DOUBLE_CODER.encode(value.sum, outStream);
            }

            @Override
            public CountSum decode(InputStream inStream) throws IOException {
                long count = LONG_CODER.decode(inStream);
                double sum = DOUBLE_CODER.decode(inStream);
                return new CountSum(count, sum);
            }

            @Override
            public void verifyDeterministic() {}

            @Override
            public boolean isRegisterByteSizeObserverCheap(CountSum value) {
                return true;
            }

            @Override
            public void registerByteSizeObserver(CountSum value, ElementByteSizeObserver observer)
                    throws Exception {
                LONG_CODER.registerByteSizeObserver(value.count, observer);
                DOUBLE_CODER.registerByteSizeObserver(value.sum, observer);
            }
        }
    }

    private static class GetLast extends DoFn<Integer, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.pane().isLast()) {
                c.output(c.element());
            }
        }
    }

}
