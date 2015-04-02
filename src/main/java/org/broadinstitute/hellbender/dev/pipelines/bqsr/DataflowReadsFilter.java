package org.broadinstitute.hellbender.dev.pipelines.bqsr;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

import java.io.Serializable;

/**
 * A simple filter for reads on dataflow
 */
public final class DataflowReadsFilter extends PTransform<PCollection<MutableGATKRead>, PCollection<MutableGATKRead>> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final ReadFilter readFilter;
    private final SAMFileHeader header;

    /**
     * ReadsFilter will use the given header to interpret each read, and then let the Read through
     * if the passed filter accepts it.
     */
    public DataflowReadsFilter( final ReadFilter filterToApply, final SAMFileHeader header ) {
        if (null==filterToApply) {
            throw new IllegalArgumentException("Missing argument: filterToApply");
        }
        if (null==header) {
            throw new IllegalArgumentException("Missing argument: header");
        }
        this.readFilter = filterToApply;
        this.header = header;
    }

    /**
     * Filter out reads we don't want.
     */
    @Override
    public PCollection<MutableGATKRead> apply(PCollection<MutableGATKRead> in) {
        return in.apply(ParDo
                .named(getName())
                .of(new DoFn<MutableGATKRead, MutableGATKRead>() {
                    @Override
                    public void processElement(DoFn.ProcessContext c) throws Exception {
                        GATKRead read = (GATKRead)c.element();
                        if (readFilter.test(read)) {
                            c.output(read);
                        }
                    }
                }));
    }


}
