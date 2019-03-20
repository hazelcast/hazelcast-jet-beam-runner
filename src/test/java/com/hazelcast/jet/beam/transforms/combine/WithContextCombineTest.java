package com.hazelcast.jet.beam.transforms.combine;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineTest;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/* "Inspired" by org.apache.beam.sdk.transforms.CombineTest.CombineWithContextTests */
public class WithContextCombineTest extends AbstractCombineTest {

    @Test
    @SuppressWarnings({"rawtypes"})
    public void testSimpleCombineWithContext() {
        runTestSimpleCombineWithContext(
                Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
                20,
                Arrays.asList(KV.of("a", "20:114"), KV.of("b", "20:113")),
                new String[] {"20:111134"});
    }

    @Test
    public void testSimpleCombineWithContextEmpty() {
        runTestSimpleCombineWithContext(EMPTY_TABLE, 0, Collections.emptyList(), new String[] {});
    }

    private void runTestSimpleCombineWithContext(
            List<KV<String, Integer>> table,
            int globalSum,
            List<KV<String, String>> perKeyCombines,
            String[] globallyCombines) {
        PCollection<KV<String, Integer>> perKeyInput = createInput(pipeline, table);
        PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

        PCollection<Integer> sum = globallyInput.apply("Sum", Combine.globally(new CombineTest.SharedTestBase.SumInts()));

        PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

        PCollection<KV<String, String>> combinePerKey =
                perKeyInput.apply(
                        Combine.<String, Integer, String>perKey(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallySumView))
                                .withSideInputs(globallySumView));

        PCollection<String> combineGlobally =
                globallyInput.apply(
                        Combine.globally(new CombineTest.SharedTestBase.TestCombineFnWithContext(globallySumView))
                                .withoutDefaults()
                                .withSideInputs(globallySumView));

        PAssert.that(sum).containsInAnyOrder(globalSum);
        PAssert.that(combinePerKey).containsInAnyOrder(perKeyCombines);
        PAssert.that(combineGlobally).containsInAnyOrder(globallyCombines);

        pipeline.run();
    }

}
