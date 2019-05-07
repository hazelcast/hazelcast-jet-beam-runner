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

import com.hazelcast.jet.beam.CurrentTimeSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;

public class ManualTest extends AbstractTransformTest {

    @Test
    @Ignore
    public void test() {
        pipeline
                .apply("UnboundedRead", Read.from(new CurrentTimeSource()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply(
                        MapElements.into(
                                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                                .via(v -> KV.of("key", v)))
                .apply(Sum.longsPerKey())
                .apply(MapElements.via(new ToString()))
                .apply("WriteToFile", TextIO.write().to("output.txt")
                        .withWindowedWrites()
                        .withNumShards(1)
                );

        pipeline.run().waitUntilFinish();
    }

    static class ToString extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return Long.toString(input.getValue());
        }
    }
}
