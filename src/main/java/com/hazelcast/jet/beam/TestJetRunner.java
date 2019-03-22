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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.JetInstance;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.function.Function;

public class TestJetRunner extends PipelineRunner<PipelineResult> {

    public static Function<ClientConfig, JetInstance> JET_CLIENT_SUPPLIER = null;

    public static TestJetRunner fromOptions(PipelineOptions options) {
        return new TestJetRunner(options);
    }

    private final JetRunner delegate;

    private TestJetRunner(PipelineOptions options) {
        this.delegate = JetRunner.fromOptions(options, JET_CLIENT_SUPPLIER);
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        return delegate.run(pipeline);
    }
}
