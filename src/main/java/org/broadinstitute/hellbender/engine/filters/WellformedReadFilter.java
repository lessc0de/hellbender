package org.broadinstitute.hellbender.engine.filters;

import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.utils.read.GATKRead;

public class WellformedReadFilter implements ReadFilter {

    private final ReadFilter wellFormedFilter;

    public WellformedReadFilter( final SAMFileHeader header ) {
        final AlignmentAgreesWithHeaderReadFilter alignmentAgreesWithHeader = new AlignmentAgreesWithHeaderReadFilter(header);

        wellFormedFilter = ReadFilterLibrary.VALID_ALIGNMENT_START
                             .and(ReadFilterLibrary.VALID_ALIGNMENT_END)
                             .and(alignmentAgreesWithHeader)
                             .and(ReadFilterLibrary.HAS_READ_GROUP)
                             .and(ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS)
                             .and(ReadFilterLibrary.SEQ_IS_STORED)
                             .and(ReadFilterLibrary.CIGAR_IS_SUPPORTED);
    }


    @Override
    public boolean test( GATKRead read ) {
        return wellFormedFilter.test(read);
    }
}
