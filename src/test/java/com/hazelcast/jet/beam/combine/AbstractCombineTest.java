package com.hazelcast.jet.beam.combine;

import com.hazelcast.jet.beam.AbstractRunnerTest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

abstract class AbstractCombineTest extends AbstractRunnerTest {

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
}
