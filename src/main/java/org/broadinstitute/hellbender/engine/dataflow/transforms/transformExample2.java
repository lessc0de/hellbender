package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;

public class transformExample2 extends PTransform<PCollection<Integer>, PCollection<String>> {
    @Override
    public PCollection<String> apply(PCollection<Integer> input) {
            return input.apply(ParDo.of(new DoFn<Integer, String>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {
                    Integer i = c.element();
                    c.output(i.toString());
                }
            }).named("transformExample2"));
    }
}

