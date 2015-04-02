package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.DataFlowReadFn;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

/**
 * Count the number of bases in a PCollection<MutableGATKRead>
 */
public final class CountBasesDataflowTransform extends PTransformSAM<Long> {
    private static final long serialVersionUID = 1l;

    @Override
    public PCollection<Long> apply(final PCollection<MutableGATKRead> reads) {

        return reads.apply(ParDo.of(new DataFlowReadFn<Long>(getHeader()) {
            private static final long serialVersionUID = 1l;

            @Override
            protected void apply(final MutableGATKRead read) {
                final long bases = read.getBases().length;
                output(bases);
            }
        }))
        .apply(Sum.longsGlobally());
    }

}
