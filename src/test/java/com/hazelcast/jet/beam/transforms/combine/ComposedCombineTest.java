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

package com.hazelcast.jet.beam.transforms.combine;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThat;

/* "Inspired" by org.apache.beam.sdk.transforms.CombineFnsTest */
@SuppressWarnings("ALL")
public class ComposedCombineTest extends AbstractCombineTest {

    @Test
    public void testComposedCombine() {
        pipeline.getCoderRegistry().registerCoderForClass(UserString.class, UserStringCoder.of());

        PCollection<KV<String, KV<Integer, UserString>>> perKeyInput =
                pipeline.apply(
                        Create.timestamped(
                                Arrays.asList(
                                        KV.of("a", KV.of(1, UserString.of("1"))),
                                        KV.of("a", KV.of(1, UserString.of("1"))),
                                        KV.of("a", KV.of(4, UserString.of("4"))),
                                        KV.of("b", KV.of(1, UserString.of("1"))),
                                        KV.of("b", KV.of(13, UserString.of("13")))),
                                Arrays.asList(0L, 4L, 7L, 10L, 16L))
                                .withCoder(
                                        KvCoder.of(
                                                StringUtf8Coder.of(),
                                                KvCoder.of(BigEndianIntegerCoder.of(), UserStringCoder.of()))));

        TupleTag<Integer> maxIntTag = new TupleTag<>();
        TupleTag<UserString> concatStringTag = new TupleTag<>();
        PCollection<KV<String, KV<Integer, String>>> combineGlobally =
                perKeyInput
                        .apply(Values.create())
                        .apply(
                                Combine.globally(
                                        CombineFns.compose()
                                                .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                                                .with(new GetUserStringFunction(), new ConcatString(), concatStringTag)))
                        .apply(WithKeys.of("global"))
                        .apply(
                                "ExtractGloballyResult",
                                ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));

        PCollection<KV<String, KV<Integer, String>>> combinePerKey =
                perKeyInput
                        .apply(
                                Combine.perKey(
                                        CombineFns.compose()
                                                .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                                                .with(new GetUserStringFunction(), new ConcatString(), concatStringTag)))
                        .apply(
                                "ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
        PAssert.that(combineGlobally).containsInAnyOrder(KV.of("global", KV.of(13, "111134")));
        PAssert.that(combinePerKey)
                .containsInAnyOrder(KV.of("a", KV.of(4, "114")), KV.of("b", KV.of(13, "113")));
        pipeline.run();
    }

    @Test
    public void testComposedCombineWithContext() {
        pipeline.getCoderRegistry().registerCoderForClass(UserString.class, UserStringCoder.of());

        PCollectionView<String> view = pipeline.apply(Create.of("I")).apply(View.asSingleton());

        PCollection<KV<String, KV<Integer, UserString>>> perKeyInput =
                pipeline.apply(
                        Create.timestamped(
                                Arrays.asList(
                                        KV.of("a", KV.of(1, UserString.of("1"))),
                                        KV.of("a", KV.of(1, UserString.of("1"))),
                                        KV.of("a", KV.of(4, UserString.of("4"))),
                                        KV.of("b", KV.of(1, UserString.of("1"))),
                                        KV.of("b", KV.of(13, UserString.of("13")))),
                                Arrays.asList(0L, 4L, 7L, 10L, 16L))
                                .withCoder(
                                        KvCoder.of(
                                                StringUtf8Coder.of(),
                                                KvCoder.of(BigEndianIntegerCoder.of(), UserStringCoder.of()))));

        TupleTag<Integer> maxIntTag = new TupleTag<>();
        TupleTag<UserString> concatStringTag = new TupleTag<>();
        PCollection<KV<String, KV<Integer, String>>> combineGlobally =
                perKeyInput
                        .apply(Values.create())
                        .apply(
                                Combine.globally(
                                        CombineFns.compose()
                                                .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                                                .with(
                                                        new GetUserStringFunction(),
                                                        new ConcatStringWithContext(view),
                                                        concatStringTag))
                                        .withoutDefaults()
                                        .withSideInputs(ImmutableList.of(view)))
                        .apply(WithKeys.of("global"))
                        .apply(
                                "ExtractGloballyResult",
                                ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));

        PCollection<KV<String, KV<Integer, String>>> combinePerKey =
                perKeyInput
                        .apply(
                                Combine.<String, KV<Integer, UserString>, CombineFns.CoCombineResult>perKey(
                                        CombineFns.compose()
                                                .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                                                .with(
                                                        new GetUserStringFunction(),
                                                        new ConcatStringWithContext(view),
                                                        concatStringTag))
                                        .withSideInputs(ImmutableList.of(view)))
                        .apply(
                                "ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
        PAssert.that(combineGlobally).containsInAnyOrder(KV.of("global", KV.of(13, "111134I")));
        PAssert.that(combinePerKey)
                .containsInAnyOrder(KV.of("a", KV.of(4, "114I")), KV.of("b", KV.of(13, "113I")));
        pipeline.run();
    }

    @Test
    public void testComposedCombineNullValues() {
        pipeline.getCoderRegistry()
                .registerCoderForClass(UserString.class, NullableCoder.of(UserStringCoder.of()));
        pipeline.getCoderRegistry()
                .registerCoderForClass(String.class, NullableCoder.of(StringUtf8Coder.of()));

        PCollection<KV<String, KV<Integer, UserString>>> perKeyInput =
                pipeline.apply(
                        Create.timestamped(
                                Arrays.asList(
                                        KV.of("a", KV.of(1, UserString.of("1"))),
                                        KV.of("a", KV.of(1, UserString.of("1"))),
                                        KV.of("a", KV.of(4, UserString.of("4"))),
                                        KV.of("b", KV.of(1, UserString.of("1"))),
                                        KV.of("b", KV.of(13, UserString.of("13")))),
                                Arrays.asList(0L, 4L, 7L, 10L, 16L))
                                .withCoder(
                                        KvCoder.of(
                                                NullableCoder.of(StringUtf8Coder.of()),
                                                KvCoder.of(
                                                        BigEndianIntegerCoder.of(), NullableCoder.of(UserStringCoder.of())))));

        TupleTag<Integer> maxIntTag = new TupleTag<>();
        TupleTag<UserString> concatStringTag = new TupleTag<>();

        PCollection<KV<String, KV<Integer, String>>> combinePerKey =
                perKeyInput
                        .apply(
                                Combine.perKey(
                                        CombineFns.compose()
                                                .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                                                .with(
                                                        new GetUserStringFunction(), new OutputNullString(), concatStringTag)))
                        .apply(
                                "ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
        PAssert.that(combinePerKey)
                .containsInAnyOrder(
                        KV.of("a", KV.of(4, null)), KV.of("b", KV.of(13, null)));
        pipeline.run();
    }

    private static class UserString implements Serializable {
        private String strValue;

        static UserString of(String strValue) {
            UserString ret = new UserString();
            ret.strValue = strValue;
            return ret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UserString that = (UserString) o;
            return Objects.equal(strValue, that.strValue);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(strValue);
        }
    }

    private static class UserStringCoder extends AtomicCoder<UserString> {
        public static UserStringCoder of() {
            return INSTANCE;
        }

        private static final UserStringCoder INSTANCE = new UserStringCoder();

        @Override
        public void encode(UserString value, OutputStream outStream)
                throws CoderException, IOException {
            encode(value, outStream, Context.NESTED);
        }

        @Override
        public void encode(UserString value, OutputStream outStream, Coder.Context context)
                throws CoderException, IOException {
            StringUtf8Coder.of().encode(value.strValue, outStream, context);
        }

        @Override
        public UserString decode(InputStream inStream) throws CoderException, IOException {
            return decode(inStream, Context.NESTED);
        }

        @Override
        public UserString decode(InputStream inStream, Context context)
                throws CoderException, IOException {
            return UserString.of(StringUtf8Coder.of().decode(inStream, context));
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}
    }

    private static class GetIntegerFunction extends SimpleFunction<KV<Integer, UserString>, Integer> {
        @Override
        public Integer apply(KV<Integer, UserString> input) {
            return input.getKey();
        }
    }

    private static class GetUserStringFunction
            extends SimpleFunction<KV<Integer, UserString>, UserString> {
        @Override
        public UserString apply(KV<Integer, UserString> input) {
            return input.getValue();
        }
    }

    private static class ConcatString extends Combine.BinaryCombineFn<UserString> {
        @Override
        public UserString apply(UserString left, UserString right) {
            String retStr = left.strValue + right.strValue;
            char[] chars = retStr.toCharArray();
            Arrays.sort(chars);
            return UserString.of(new String(chars));
        }
    }

    private static class OutputNullString extends Combine.BinaryCombineFn<UserString> {
        @Override
        public UserString apply(UserString left, UserString right) {
            return null;
        }
    }

    private static class ConcatStringWithContext extends CombineWithContext.CombineFnWithContext<UserString, UserString, UserString> {
        private final PCollectionView<String> view;

        private ConcatStringWithContext(PCollectionView<String> view) {
            this.view = view;
        }

        @Override
        public UserString createAccumulator(CombineWithContext.Context c) {
            return UserString.of(c.sideInput(view));
        }

        @Override
        public UserString addInput(
                UserString accumulator, UserString input, CombineWithContext.Context c) {
            assertThat(accumulator.strValue, Matchers.startsWith(c.sideInput(view)));
            accumulator.strValue += input.strValue;
            return accumulator;
        }

        @Override
        public UserString mergeAccumulators(
                Iterable<UserString> accumulators, CombineWithContext.Context c) {
            String keyPrefix = c.sideInput(view);
            String all = keyPrefix;
            for (UserString accumulator : accumulators) {
                assertThat(accumulator.strValue, Matchers.startsWith(keyPrefix));
                all += accumulator.strValue.substring(keyPrefix.length());
                accumulator.strValue = "cleared in mergeAccumulators";
            }
            return UserString.of(all);
        }

        @Override
        public UserString extractOutput(UserString accumulator, CombineWithContext.Context c) {
            assertThat(accumulator.strValue, Matchers.startsWith(c.sideInput(view)));
            char[] chars = accumulator.strValue.toCharArray();
            Arrays.sort(chars);
            return UserString.of(new String(chars));
        }
    }

    private static class ExtractResultDoFn extends DoFn<KV<String, CombineFns.CoCombineResult>, KV<String, KV<Integer, String>>> {

        private final TupleTag<Integer> maxIntTag;
        private final TupleTag<UserString> concatStringTag;

        ExtractResultDoFn(TupleTag<Integer> maxIntTag, TupleTag<UserString> concatStringTag) {
            this.maxIntTag = maxIntTag;
            this.concatStringTag = concatStringTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            UserString userString = c.element().getValue().get(concatStringTag);
            KV<Integer, String> value =
                    KV.of(
                            c.element().getValue().get(maxIntTag),
                            userString == null ? null : userString.strValue);
            c.output(KV.of(c.element().getKey(), value));
        }
    }

}
