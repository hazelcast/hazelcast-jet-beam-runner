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

package com.hazelcast.jet.beam.combine;

import com.hazelcast.jet.beam.AbstractRunnerTest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class AbstractCombineTest extends AbstractRunnerTest implements Serializable {

    static final List<KV<String, Integer>> EMPTY_TABLE = Collections.emptyList();

    static PCollection<KV<String, Integer>> createInput(
            Pipeline p, List<KV<String, Integer>> table) {
        return p.apply(
                Create.of(table).withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
    }

    static class Summer implements Serializable {
        public int sum(Iterable<Integer> integers) {
            int sum = 0;
            for (int i : integers) {
                sum += i;
            }
            return sum;
        }
    }

    protected static class MeanInts extends Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, Double> {
        private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
        private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

        static class CountSum
                implements Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, Double> {
            long count;
            double sum;

            CountSum(long count, double sum) {
                this.count = count;
                this.sum = sum;
            }

            @Override
            public void addInput(Integer element) {
                count++;
                sum += element.doubleValue();
            }

            @Override
            public void mergeAccumulator(CountSum accumulator) {
                count += accumulator.count;
                sum += accumulator.sum;
            }

            @Override
            public Double extractOutput() {
                return count == 0 ? 0.0 : sum / count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(count, sum);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (!(obj instanceof CountSum)) {
                    return false;
                }
                CountSum other = (CountSum) obj;
                return this.count == other.count && (Math.abs(this.sum - other.sum) < 0.1);
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
            }
        }

        @Override
        public CountSum createAccumulator() {
            return new CountSum(0, 0.0);
        }

        @Override
        public Coder<CountSum> getAccumulatorCoder(
                CoderRegistry registry, Coder<Integer> inputCoder) {
            return new CountSumCoder();
        }

        /** A {@link Coder} for {@link CountSum}. */
        private static class CountSumCoder extends AtomicCoder<CountSum> {
            @Override
            public void encode(CountSum value, OutputStream outStream) throws IOException {
                LONG_CODER.encode(value.count, outStream);
                DOUBLE_CODER.encode(value.sum, outStream);
            }

            @Override
            public CountSum decode(InputStream inStream) throws IOException {
                long count = LONG_CODER.decode(inStream);
                double sum = DOUBLE_CODER.decode(inStream);
                return new CountSum(count, sum);
            }

            @Override
            public void verifyDeterministic() {}

            @Override
            public boolean isRegisterByteSizeObserverCheap(CountSum value) {
                return true;
            }

            @Override
            public void registerByteSizeObserver(CountSum value, ElementByteSizeObserver observer)
                    throws Exception {
                LONG_CODER.registerByteSizeObserver(value.count, observer);
                DOUBLE_CODER.registerByteSizeObserver(value.sum, observer);
            }
        }
    }
}
