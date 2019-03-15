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

import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;

import java.util.Collection;
import java.util.Collections;

public class CreateViewProcessor extends AbstractProcessor {

    private final ResettableSingletonTraverser<SideInputValue> traverser = new ResettableSingletonTraverser<>();
    private final FlatMapper<Object, SideInputValue> flatMapper;
    private final String ownerId; //do not remove, useful for debugging

    public CreateViewProcessor(PCollectionView view, String ownerId) {
        flatMapper = flatMapper(
                item -> {
                    WindowedValue windowedValue = (WindowedValue) item;

                    Iterable<?> iterableValue = Collections.singletonList(windowedValue.getValue());
                    Instant timestamp = windowedValue.getTimestamp();
                    Collection<? extends BoundedWindow> windows = windowedValue.getWindows();
                    PaneInfo pane = windowedValue.getPane();

                    WindowedValue<Iterable<?>> sideInputValue = WindowedValue.of(iterableValue, timestamp, windows, pane);

                    traverser.accept(new SideInputValue(view, sideInputValue));
                    return traverser;
                }
        );
        this.ownerId = ownerId;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        return flatMapper.tryProcess(item);
    }
}
