package org.broadinstitute.hellbender.utils.read;

import com.google.api.services.genomics.model.Read;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.Locatable;

import java.util.UUID;

public interface GATKRead extends Locatable {
    UUID getUUID();

    String getName();

    int getLength();

    int getUnclippedStart();

    int getUnclippedEnd();

    String getMateContig();

    int getMateStart();

    int getFragmentLength();

    int getMappingQuality();

    byte[] getBases();

    byte[] getBaseQualities();

    Cigar getCigar();

    String getReadGroup();

    int getNumberOfReadsInFragment();

    int getReadNumber();

    boolean isPaired();

    boolean isProperlyPaired();

    boolean isUnmapped();

    boolean mateIsUnmapped();

    boolean isReverseStrand();

    boolean mateIsReverseStrand();

    boolean isFirstOfPair();

    boolean isSecondOfPair();

    boolean isNonPrimaryAlignment();

    boolean isSupplementaryAlignment();

    boolean failsVendorQualityCheck();

    boolean isDuplicate();

    boolean hasAttribute( final String attributeName );

    Integer getAttributeAsInteger( final String attributeName );

    String getAttributeAsString( final String attributeName );

    byte[] getAttributeAsByteArray( final String attributeName );

    GATKRead copy();

    SAMRecord convertToSAMRecord( final SAMFileHeader header );

    Read convertToGoogleGenomicsRead();
}

