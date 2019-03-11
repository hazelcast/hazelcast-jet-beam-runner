package com.hazelcast.jet.beam;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

@SuppressWarnings("unchecked")
public class JetTranslationContext {

    private final SerializablePipelineOptions options;
    private final DAGBuilder dagBuilder = new DAGBuilder();

    JetTranslationContext(JetRunnerOptions options) {
        this.options = new SerializablePipelineOptions(options);
    }

    SerializablePipelineOptions getOptions() {
        return options;
    }

    DAGBuilder getDagBuilder() {
        return dagBuilder;
    }

    <T> WindowedValue.FullWindowedValueCoder<T> getTypeInfo(PCollection<T> collection) {
        return getTypeInfo(collection.getCoder(), collection.getWindowingStrategy());
    }

    <T> WindowedValue.FullWindowedValueCoder<T> getTypeInfo(Coder<T> coder, WindowingStrategy<?, ?> windowingStrategy) {
        return WindowedValue.getFullCoder(coder, windowingStrategy.getWindowFn().windowCoder());
    }
}
