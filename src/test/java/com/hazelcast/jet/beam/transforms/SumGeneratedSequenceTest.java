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
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class SumGeneratedSequenceTest extends AbstractTransformTest {

    @Test
    public void emptySequence() {
        PCollection<String> output = pipeline
                .apply("Source", GenerateSequence.from(0).to(0))
                .apply(Sum.longsGlobally())
                .apply(MapElements.via(new FormatLongAsTextFn()));

        PAssert.that(output).containsInAnyOrder("0");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void nonEmptySequence() {
        PCollection<String> output = pipeline
                .apply("Source", GenerateSequence.from(0).to(100))
                .apply(Sum.longsGlobally())
                .apply(MapElements.via(new FormatLongAsTextFn()));

        PAssert.that(output).containsInAnyOrder("4951");
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        System.out.println("state = " + state); //todo: remove
    }

}
