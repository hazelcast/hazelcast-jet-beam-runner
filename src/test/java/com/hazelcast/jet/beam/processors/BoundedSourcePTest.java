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

package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.core.test.TestSupport;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertTrue;

public class BoundedSourcePTest {

    @Test
    public void testEmpty() {
        TestSupport
                .verifyProcessor(() -> new BoundedSourceP<>(emptyList(), null, null, null))
                .disableSnapshots()
                .expectOutput(emptyList());
    }

    @Test
    public void testSimple() {
        List<BoundedSource<Long>> shards =
                asList(new MockBoundedSource(0, 100), new MockBoundedSource(100, 200));

        TestSupport
                .verifyProcessor(() -> new BoundedSourceP<>(shards, null, null, null))
                .disableSnapshots()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(IntStream
                        .range(0, 200)
                        .mapToObj(val -> WindowedValue.timestampedValueInGlobalWindow(val, BoundedWindow.TIMESTAMP_MIN_VALUE))
                        .collect(Collectors.toList()));

        for (BoundedSource<Long> shard : shards) {
            ((MockBoundedSource) shard).assertReaderClosed();
        }
    }

    private static class MockBoundedSource extends BoundedSource<Long> {
        private final MockBoundedReader reader;
        private boolean readerTaken;

        MockBoundedSource(long from, long to) {
            if (from > to) {
                throw new IllegalArgumentException();
            }
            reader = new MockBoundedReader(from, to);
        }

        @Override
        public List<? extends BoundedSource<Long>> split(long desiredBundleSizeBytes, PipelineOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BoundedReader<Long> createReader(PipelineOptions options) {
            assert !readerTaken : "reader already taken";
            readerTaken = true;
            return reader;
        }

        void assertReaderClosed() {
            assertTrue("reader not closed", reader.closed);
        }
    }

    private static class MockBoundedReader extends BoundedReader<Long> {
        private final long from;
        private final long to;

        private long current;
        private boolean closed;

        MockBoundedReader(long from, long to) {
            this.from = from;
            this.to = to;
            current = from - 1;
        }

        @Override
        public boolean start() {
            if (current >= from) {
                throw new IllegalStateException("already started");
            }
            return ++current < to;
        }

        @Override
        public boolean advance() {
            if (current < from) {
                throw new IllegalStateException("not started");
            }
            return ++current < to;
        }

        @Override
        public Long getCurrent() throws NoSuchElementException {
            if (current < from) {
                throw new IllegalStateException("not started");
            }
            if (current >= to) {
                throw new IllegalStateException("already exhausted");
            }
            return current;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public BoundedSource<Long> getCurrentSource() {
            throw new UnsupportedOperationException();
        }
    }
}
