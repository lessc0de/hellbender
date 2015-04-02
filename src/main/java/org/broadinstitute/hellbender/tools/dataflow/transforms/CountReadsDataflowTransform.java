package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

/**
 * Count the number of Reads in a PCollection<MutableGATKRead>
 */
public final class CountReadsDataflowTransform extends PTransformSAM<Long> {

    private static final long serialVersionUID = 1l;

    @Override
    public PCollection<Long> apply(final PCollection<MutableGATKRead> input) {
        return input.apply(Count.globally());
    }
}
