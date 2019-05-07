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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.NoSuchElementException;

public class CurrentTimeSource extends UnboundedSource<Long, CurrentTimeSource.CounterMark> {
    private static final long serialVersionUID = 1L;

    @Override
    public java.util.List<? extends UnboundedSource<Long, CurrentTimeSource.CounterMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Long> createReader(PipelineOptions options, @Nullable CurrentTimeSource.CounterMark counterMark) {
        return new ValuesReader(this);
    }

    @Nullable
    @Override
    public Coder<CurrentTimeSource.CounterMark> getCheckpointMarkCoder() {
        return DelegateCoder.of(VarIntCoder.of(), input -> input.current, CounterMark::new);
    }

    @Override
    public Coder<Long> getOutputCoder() {
        return VarLongCoder.of();
    }

    public static class CounterMark implements UnboundedSource.CheckpointMark {
        int current;

        public CounterMark(int current) {
            this.current = current;
        }

        @Override
        public void finalizeCheckpoint() {
        }
    }

    private static class ValuesReader extends UnboundedReader<Long> {

        private final UnboundedSource<Long, CounterMark> source;
        private Long current;

        public ValuesReader(UnboundedSource<Long, CounterMark> source) {
            this.source = source;
        }

        @Override
        public boolean start() throws IOException {
            return advance();
        }

        @Override
        public boolean advance() {
            current = System.currentTimeMillis();
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            } //todo: bad...
            return true;
        }

        @Override
        public Long getCurrent() throws NoSuchElementException {
            return current;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return Instant.now();
        }

        @Override
        public void close() {
        }

        @Override
        public Instant getWatermark() {
            return Instant.now();
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return new CounterMark(0);
        }

        @Override
        public UnboundedSource<Long, ?> getCurrentSource() {
            return source;
        }
    }
}