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

package com.hazelcast.jet.beam.groupby;

import com.hazelcast.jet.beam.AbstractRunnerTest;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.hamcrest.Matcher;
import org.junit.Assert;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class AbstractGroupByKeyTest extends AbstractRunnerTest {

    static SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> containsKvs(
            KV<String, Collection<Integer>>... kvs) {
        return new ContainsKVs(ImmutableList.copyOf(kvs));
    }

    static KV<String, Collection<Integer>> kv(String key, Integer... values) {
        return KV.of(key, ImmutableList.copyOf(values));
    }

    private static class ContainsKVs implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> {
        private final List<KV<String, Collection<Integer>>> expectedKvs;

        private ContainsKVs(List<KV<String, Collection<Integer>>> expectedKvs) {
            this.expectedKvs = expectedKvs;
        }

        @Override
        public Void apply(Iterable<KV<String, Iterable<Integer>>> input) {
            List<Matcher<? super KV<String, Iterable<Integer>>>> matchers = new ArrayList<>();
            for (KV<String, Collection<Integer>> expected : expectedKvs) {
                Integer[] values = expected.getValue().toArray(new Integer[0]);
                matchers.add(KvMatcher.isKv(equalTo(expected.getKey()), containsInAnyOrder(values)));
            }
            assertThat(input, containsInAnyOrder(matchers.toArray(new Matcher[0])));
            return null;
        }
    }

    /**
     * This is a bogus key class that returns random hash values from {@link #hashCode()} and always
     * returns {@code false} for {@link #equals(Object)}. The results of the test are correct if the
     * runner correctly hashes and sorts on the encoded bytes.
     */
    static class BadEqualityKey {
        long key;

        public BadEqualityKey() {}

        public BadEqualityKey(long key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public int hashCode() {
            return ThreadLocalRandom.current().nextInt();
        }
    }

    /** Deterministic {@link Coder} for {@link BadEqualityKey}. */
    static class DeterministicKeyCoder extends AtomicCoder<BadEqualityKey> {

        public static DeterministicKeyCoder of() {
            return INSTANCE;
        }

        /////////////////////////////////////////////////////////////////////////////

        private static final DeterministicKeyCoder INSTANCE = new DeterministicKeyCoder();

        private DeterministicKeyCoder() {}

        @Override
        public void encode(BadEqualityKey value, OutputStream outStream) throws IOException {
            new DataOutputStream(outStream).writeLong(value.key);
        }

        @Override
        public BadEqualityKey decode(InputStream inStream) throws IOException {
            return new BadEqualityKey(new DataInputStream(inStream).readLong());
        }

        @Override
        public void verifyDeterministic() {}
    }

    static class AssignRandomKey extends DoFn<KV<BadEqualityKey, Long>, KV<Long, KV<BadEqualityKey, Long>>> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output(KV.of(ThreadLocalRandom.current().nextLong(), c.element()));
        }
    }

    static class CountFn implements SerializableFunction<Iterable<Long>, Long> {
        @Override
        public Long apply(Iterable<Long> input) {
            long result = 0L;
            for (Long in : input) {
                result += in;
            }
            return result;
        }
    }

    static class AssertThatCountPerKeyCorrect implements SerializableFunction<Iterable<KV<BadEqualityKey, Long>>, Void> {
        private final int numValues;

        AssertThatCountPerKeyCorrect(int numValues) {
            this.numValues = numValues;
        }

        @Override
        public Void apply(Iterable<KV<BadEqualityKey, Long>> input) {
            for (KV<BadEqualityKey, Long> val : input) {
                Assert.assertEquals(numValues, (long) val.getValue());
            }
            return null;
        }
    }

    static class AssertThatAllKeysExist implements SerializableFunction<Iterable<BadEqualityKey>, Void> {
        private final int numKeys;

        AssertThatAllKeysExist(int numKeys) {
            this.numKeys = numKeys;
        }

        private static <T> Iterable<Object> asStructural(
                final Iterable<T> iterable, final Coder<T> coder) {

            return StreamSupport.stream(iterable.spliterator(), false)
                    .map(
                            input -> {
                                try {
                                    return coder.structuralValue(input);
                                } catch (Exception e) {
                                    Assert.fail("Could not structural values.");
                                    throw new RuntimeException(); // to satisfy the compiler...
                                }
                            })
                    .collect(Collectors.toList());
        }

        @Override
        public Void apply(Iterable<BadEqualityKey> input) {
            final DeterministicKeyCoder keyCoder = DeterministicKeyCoder.of();

            List<BadEqualityKey> expectedList = new ArrayList<>();
            for (int key = 0; key < numKeys; key++) {
                expectedList.add(new BadEqualityKey(key));
            }

            Iterable<Object> structuralInput = asStructural(input, keyCoder);
            Iterable<Object> structuralExpected = asStructural(expectedList, keyCoder);

            for (Object expected : structuralExpected) {
                assertThat(structuralInput, hasItem(expected));
            }

            return null;
        }
    }
}
