package com.hazelcast.jet.beam.processors;

import com.hazelcast.jet.beam.SideInputValue;
import com.hazelcast.jet.core.AbstractProcessor;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class CreateViewProcessor extends AbstractProcessor implements Serializable {

    private final PCollectionView view;

    public CreateViewProcessor(PCollectionView view) {
        this.view = view;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) throws Exception {
        WindowedValue windowedValue = (WindowedValue) item;
        SideInputValue sideInputValue = new SideInputValue(view, WindowedValue.valueInGlobalWindow(Collections.singleton(windowedValue.getValue())));

        while (!tryEmit(sideInputValue)) { //todo: how to do better, ie. without garbage?
            TimeUnit.MILLISECONDS.sleep(10);
        }
        return true;
    }
}
