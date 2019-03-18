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
