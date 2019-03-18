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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

/* "Inspired" by org.apache.beam.sdk.transforms.ImpulseTest */
public class ImpulseTest extends AbstractRunnerTest {

    @Test
    @Ignore //todo: impulse data sources not yet implemented
    public void testImpulse() {
        PCollection<Integer> result =
                pipeline.apply(Impulse.create())
                        .apply(
                                FlatMapElements.into(TypeDescriptors.integers())
                                        .via(impulse -> Arrays.asList(1, 2, 3)));
        PAssert.that(result).containsInAnyOrder(1, 2, 3);
        pipeline.run().waitUntilFinish();
    }

}
