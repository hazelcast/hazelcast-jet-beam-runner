package com.hazelcast.jet.beam.pardo;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.junit.Ignore;
import org.junit.Test;

/* "Inspired" by org.apache.beam.sdk.transforms.ParDoTest.TimerCoderInferenceTests */
@SuppressWarnings("ALL")
public class TimerCoderInferenceParDoTest extends AbstractParDoTest {

    //todo: enable tests after state & timers are implemented

    @Test
    @Ignore
    public void testValueStateCoderInference() {
        final String stateId = "foo";
        MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();
        pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

        DoFn<KV<String, Integer>, MyInteger> fn =
                new DoFn<KV<String, Integer>, MyInteger>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<MyInteger>> intState = StateSpecs.value();

                    @ProcessElement
                    public void processElement(
                            ProcessContext c,
                            @StateId(stateId) ValueState<MyInteger> state,
                            OutputReceiver<MyInteger> r) {
                        MyInteger currentValue = MoreObjects.firstNonNull(state.read(), new MyInteger(0));
                        r.output(currentValue);
                        state.write(new MyInteger(currentValue.getValue() + 1));
                    }
                };

        PCollection<MyInteger> output =
                pipeline
                        .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
                        .apply(ParDo.of(fn))
                        .setCoder(myIntegerCoder);

        PAssert.that(output).containsInAnyOrder(new MyInteger(0), new MyInteger(1), new MyInteger(2));
        pipeline.run();
    }

    @Test
    @Ignore
    public void testValueStateCoderInferenceFailure() {
        final String stateId = "foo";
        MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();

        DoFn<KV<String, Integer>, MyInteger> fn =
                new DoFn<KV<String, Integer>, MyInteger>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<MyInteger>> intState = StateSpecs.value();

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<MyInteger> state, OutputReceiver<MyInteger> r) {
                        MyInteger currentValue = MoreObjects.firstNonNull(state.read(), new MyInteger(0));
                        r.output(currentValue);
                        state.write(new MyInteger(currentValue.getValue() + 1));
                    }
                };

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Unable to infer a coder for ValueState and no Coder was specified.");

        pipeline
                .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
                .apply(ParDo.of(fn))
                .setCoder(myIntegerCoder);

        pipeline.run();
    }

    @Test
    @Ignore
    public void testValueStateCoderInferenceFromInputCoder() {
        final String stateId = "foo";
        MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();

        DoFn<KV<String, MyInteger>, MyInteger> fn =
                new DoFn<KV<String, MyInteger>, MyInteger>() {

                    @StateId(stateId)
                    private final StateSpec<ValueState<MyInteger>> intState = StateSpecs.value();

                    @ProcessElement
                    public void processElement(
                            @StateId(stateId) ValueState<MyInteger> state, OutputReceiver<MyInteger> r) {
                        MyInteger currentValue = MoreObjects.firstNonNull(state.read(), new MyInteger(0));
                        r.output(currentValue);
                        state.write(new MyInteger(currentValue.getValue() + 1));
                    }
                };

        pipeline
                .apply(
                        Create.of(
                                KV.of("hello", new MyInteger(42)),
                                KV.of("hello", new MyInteger(97)),
                                KV.of("hello", new MyInteger(84)))
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), myIntegerCoder)))
                .apply(ParDo.of(fn))
                .setCoder(myIntegerCoder);

        pipeline.run();
    }

}
