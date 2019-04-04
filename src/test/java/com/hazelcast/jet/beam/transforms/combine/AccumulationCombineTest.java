package com.hazelcast.jet.beam.transforms.combine;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/* "Inspired" by org.apache.beam.sdk.transforms.CombineTest.AccumulationTests */
@SuppressWarnings({"ALL"})
public class AccumulationCombineTest extends AbstractCombineTest {

    @Test
    public void testAccumulatingCombine() {
        runTestAccumulatingCombine(
                Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
                4.0,
                Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0)));
    }

    @Test
    public void testAccumulatingCombineEmpty() {
        runTestAccumulatingCombine(EMPTY_TABLE, 0.0, Collections.emptyList());
    }

    private void runTestAccumulatingCombine(
            List<KV<String, Integer>> table, Double globalMean, List<KV<String, Double>> perKeyMeans) {
        PCollection<KV<String, Integer>> input = createInput(pipeline, table);

        PCollection<Double> mean =
                input.apply(Values.create()).apply(Combine.globally(new MeanInts()));

        PCollection<KV<String, Double>> meanPerKey = input.apply(Combine.perKey(new MeanInts()));

        PAssert.that(mean).containsInAnyOrder(globalMean);
        PAssert.that(meanPerKey).containsInAnyOrder(perKeyMeans);

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

}
