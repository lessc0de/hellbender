package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.util.Locatable;

public interface MutableGATKRead extends GATKRead {
    void setName( final String name );

    void setPosition( final String contig, final int start );

    void setPosition( final Locatable locatable );

    void setMatePosition( final String contig, final int start );

    void setMatePosition( final Locatable locatable );

    void setFragmentLength( final int fragmentLength );

    void setMappingQuality( final int mappingQuality );

    void setBases( final byte[] bases );

    void setBaseQualities( final byte[] baseQualities );

    void setCigar( final Cigar cigar );

    void setCigar( final String cigarString );

    void setReadGroup( final String readGroupID );

    void setNumberOfReadsInFragment( final int numberOfReads );

    void setReadNumber( final int readNumber);

    void setIsPaired( final boolean isPaired );

    void setIsProperlyPaired( final boolean isProperlyPaired );

    void setIsUnmapped();

    void setMateIsUnmapped();

    void setIsReverseStrand( final boolean isReverseStrand );

    void setMateIsReverseStrand( final boolean mateIsReverseStrand );

    void setIsFirstOfPair( final boolean isFirstOfPair );

    void setIsSecondOfPair( final boolean isSecondOfPair );

    void setIsNonPrimaryAlignment( final boolean isNonPrimaryAlignment );

    void setIsSupplementaryAlignment( final boolean isSupplementaryAlignment );

    void setFailsVendorQualityCheck( final boolean failsVendorQualityCheck );

    void setIsDuplicate( final boolean isDuplicate );

    void setAttribute( final String attributeName, final Integer attributeValue );

    void setAttribute( final String attributeName, final String attributeValue );

    void setAttribute( final String attributeName, final byte[] attributeValue );

    void clearAttribute( final String attributeName );

    void clearAttributes();

    @Override
    MutableGATKRead copy();
}
