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

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Arrays;

/* "Inspired" by org.apache.beam.examples.WordCountTest */
public class WordCountTest extends AbstractRunnerTest {

    @Test
    public void testCountWords() {
        String[] lines = {"hi there", "hi", "hi sue bob", "hi sue", "", "bob hi"};
        PCollection<String> input = pipeline.apply(Create.of(Arrays.asList(lines)).withCoder(StringUtf8Coder.of()));

        PCollection<String> output = input
                .apply(new CountWords())
                .apply(MapElements.via(new FormatKVAsTextFn()));

        PAssert.that(output).containsInAnyOrder("hi: 5", "there: 1", "sue: 2", "bob: 2");
        pipeline.run().waitUntilFinish();
    }

    static class ExtractWordsFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String[] words = element.split("[^\\p{L}]+", -1);
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }
}
