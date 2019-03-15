package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Collections;

public class CreateViewProcessor extends AbstractProcessor {

    private final ResettableSingletonTraverser<SideInputValue> traverser = new ResettableSingletonTraverser<>();
    private final FlatMapper<Object, SideInputValue> flatMapper;

    public CreateViewProcessor(PCollectionView view) {
        flatMapper = flatMapper(item -> {
            traverser.accept(new SideInputValue(view, WindowedValue.valueInGlobalWindow(
                    Collections.singleton(((WindowedValue) item).getValue()))));
            return traverser;
        });
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        return flatMapper.tryProcess(item);
    }
}
