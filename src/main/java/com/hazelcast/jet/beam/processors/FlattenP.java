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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.SupplierEx;

public class FlattenP extends AbstractProcessor {

    private final ResettableSingletonTraverser<Object> traverser = new ResettableSingletonTraverser<>();
    private final FlatMapper<Object, Object> flatMapper;
    private final String ownerId; //do not remove, useful for debugging

    FlattenP(String ownerId) {
        this.ownerId = ownerId;
        this.flatMapper = flatMapper(
                item -> {
                    traverser.accept(item);
                    return traverser;
                }
        );
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        return flatMapper.tryProcess(item);
    }

    public static SupplierEx<Processor> supplier(String ownerId) {
        return () -> new FlattenP(ownerId);
    }
}
