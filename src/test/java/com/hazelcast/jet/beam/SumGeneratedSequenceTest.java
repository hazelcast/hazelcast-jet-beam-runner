package com.hazelcast.jet.beam;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class SumGeneratedSequenceTest extends AbstractRunnerTest {

    @Test
    public void emptySequence() {
        PCollection<String> output = p
                .apply("Source", GenerateSequence.from(0).to(0))
                .apply(Sum.longsGlobally())
                .apply(MapElements.via(new FormatLongAsTextFn()));

        PAssert.that(output).containsInAnyOrder("0");
        p.run().waitUntilFinish();
    }

    @Test
    public void nonEmptySequence() {
        PCollection<String> output = p
                .apply("Source", GenerateSequence.from(0).to(100))
                .apply(Sum.longsGlobally())
                .apply(MapElements.via(new FormatLongAsTextFn()));

        PAssert.that(output).containsInAnyOrder("4950");
        p.run().waitUntilFinish();
    }

}
