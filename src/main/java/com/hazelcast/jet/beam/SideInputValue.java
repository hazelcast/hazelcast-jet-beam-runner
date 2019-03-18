package com.hazelcast.jet.beam;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

public class SideInputValue {

    private final PCollectionView<?> view;
    private final WindowedValue<Iterable<?>> windowedValue;

    public SideInputValue(PCollectionView view, WindowedValue<Iterable<?>> windowedValue) {
        this.view = view;
        this.windowedValue = windowedValue;
    }

    public PCollectionView<?> getView() {
        return view;
    }

    public WindowedValue<Iterable<?>> getWindowedValue() {
        return windowedValue;
    }

    @Override
    public String toString() {
        return "SideInputValue (" + windowedValue + ")" ;
    }
}
