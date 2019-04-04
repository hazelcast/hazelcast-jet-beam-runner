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
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;

/* "Inspired" by org.apache.beam.sdk.transforms.KeysTest */
@SuppressWarnings("ALL")
public class KeysTest extends AbstractTransformTest {

    private static final KV<String, Integer>[] TABLE =
            new KV[] {
                    KV.of("one", 1), KV.of("two", 2), KV.of("three", 3), KV.of("dup", 4), KV.of("dup", 5)
            };

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {};

    @Test
    public void testKeys() {
        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.of(Arrays.asList(TABLE))
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

        PCollection<String> output = input.apply(Keys.create());
        PAssert.that(output).containsInAnyOrder("one", "two", "three", "dup", "dup");

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

    @Test
    public void testKeysEmpty() {
        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.of(Arrays.asList(EMPTY_TABLE))
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

        PCollection<String> output = input.apply(Keys.create());
        PAssert.that(output).empty();
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, state);
    }

}
