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

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.apache.beam.sdk.TestUtils.LINES;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;

/* "Inspired" by org.apache.beam.sdk.transforms.CreateTest */
@SuppressWarnings("ALL")
public class CreateTest extends AbstractTransformTest {

    @Test
    public void testCreate() {
        PCollection<String> output = pipeline.apply(Create.of(LINES));

        PAssert.that(output).containsInAnyOrder(LINES_ARRAY);
        pipeline.run();
    }

    @Test
    public void testCreateEmpty() {
        PCollection<String> output = pipeline.apply(Create.empty(StringUtf8Coder.of()));

        PAssert.that(output).containsInAnyOrder(NO_LINES_ARRAY);

        assertEquals(StringUtf8Coder.of(), output.getCoder());
        pipeline.run();
    }

    @Test
    public void testCreateWithNullsAndValues() throws Exception {
        PCollection<String> output =
                pipeline.apply(
                        Create.of(null, "test1", null, "test2", null)
                                .withCoder(SerializableCoder.of(String.class)));
        PAssert.that(output).containsInAnyOrder(null, "test1", null, "test2", null);
        pipeline.run();
    }

    @Test
    public void testCreateWithVoidType() throws Exception {
        PCollection<Void> output = pipeline.apply(Create.of(null, (Void) null));
        PAssert.that(output).containsInAnyOrder(null, null);
        pipeline.run();
    }

    @Test
    public void testCreateWithKVVoidType() throws Exception {
        PCollection<KV<Void, Void>> output =
                pipeline.apply(Create.of(KV.of(null, null), KV.of(null, null)));

        PAssert.that(output)
                .containsInAnyOrder(KV.of(null, null), KV.of(null, null));

        pipeline.run();
    }

}
