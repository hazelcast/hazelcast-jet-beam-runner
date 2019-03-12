package com.hazelcast.jet.beam.pardo;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

public class MultipleInputsAndOutputParDoTest extends AbstractParDoTest {

    @Test
    @Category(ValidatesRunner.class)
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

}
