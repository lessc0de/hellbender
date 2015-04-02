package org.broadinstitute.hellbender.utils.read;


import com.google.api.services.genomics.model.Read;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import htsjdk.samtools.*;
import htsjdk.samtools.util.Locatable;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.io.Serializable;
import java.util.UUID;

public final class SAMRecordToGATKReadAdapter implements MutableGATKRead, Serializable {
    private static final long serialVersionUID = 1L;

    private final SAMRecord samRecord;
    private UUID uuid;

    public SAMRecordToGATKReadAdapter( final SAMRecord samRecord ) {
        this.samRecord = samRecord;
        this.uuid = UUID.randomUUID();
    }

    @Override
    public UUID getUUID() {
        return uuid;
    }

    public void setUUID( final UUID uuid ) {
        this.uuid = uuid;
    }

    @Override
    public String getContig() {
        if ( isUnmapped() ) {
            return null;
        }

        // Guaranteed not to be null or SAMRecord.NO_ALIGNMENT_REFERENCE_NAME due to the isUnmapped() check above
        return samRecord.getReferenceName();
    }

    @Override
    public int getStart() {
        if ( isUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        // Guaranteed not to be SAMRecord.NO_ALIGNMENT_START due to the isUnmapped() check above
        return samRecord.getAlignmentStart();
    }

    @Override
    public int getEnd() {
        if ( isUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        // Guaranteed not to be SAMRecord.NO_ALIGNMENT_START due to the isUnmapped() check above
        return samRecord.getAlignmentEnd();
    }

    @Override
    public String getName() {
        return samRecord.getReadName();
    }

    @Override
    public int getLength() {
        return samRecord.getReadLength();
    }

    @Override
    public int getUnclippedStart() {
        if ( isUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        return samRecord.getUnclippedStart();
    }

    @Override
    public int getUnclippedEnd() {
        if ( isUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        return samRecord.getUnclippedEnd();
    }

    @Override
    public String getMateContig() {
        if ( mateIsUnmapped() ) {
            return null;
        }

        return samRecord.getMateReferenceName();
    }

    @Override
    public int getMateStart() {
        if ( mateIsUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        return samRecord.getMateAlignmentStart();
    }

    @Override
    public int getFragmentLength() {
        // Will be 0 if unknown
        return samRecord.getInferredInsertSize();
    }

    @Override
    public int getMappingQuality() {
        return samRecord.getMappingQuality();
    }

    @Override
    public byte[] getBases() {
        return samRecord.getReadBases() != null ? samRecord.getReadBases() : new byte[0];
    }

    @Override
    public byte[] getBaseQualities() {
        return samRecord.getBaseQualities() != null ? samRecord.getBaseQualities() : new byte[0];
    }

    @Override
    public Cigar getCigar() {
        return samRecord.getCigar() != null ? samRecord.getCigar() : new Cigar();
    }

    @Override
    public String getReadGroup() {
        // May return null
        return (String)samRecord.getAttribute(SAMTagUtil.getSingleton().RG);
    }

    @Override
    public int getNumberOfReadsInFragment() {
        return isPaired() ? 2 : 1;
    }

    @Override
    public int getReadNumber() {
        if ( isPaired() ) {
            return isFirstOfPair() ? 1 : isSecondOfPair() ? 2 : 0;
        }
        return 1;
    }

    @Override
    public boolean isPaired() {
        return samRecord.getReadPairedFlag();
    }

    @Override
    public boolean isProperlyPaired() {
        return isPaired() && samRecord.getProperPairFlag();
    }

    @Override
    public boolean isUnmapped() {
        return samRecord.getReadUnmappedFlag() ||
               samRecord.getReferenceIndex() == null || samRecord.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX ||
               samRecord.getReferenceName() == null || samRecord.getReferenceName().equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) ||
               samRecord.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START;
    }

    @Override
    public boolean mateIsUnmapped() {
        if ( ! isPaired() ) {
            throw new IllegalStateException("Cannot get mate information for an unpaired read");
        }

        return samRecord.getMateUnmappedFlag() ||
               samRecord.getMateReferenceName() == null || samRecord.getMateReferenceName().equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) ||
               samRecord.getMateAlignmentStart() == SAMRecord.NO_ALIGNMENT_START;
    }

    @Override
    public boolean isReverseStrand() {
        return samRecord.getReadNegativeStrandFlag();
    }

    @Override
    public boolean mateIsReverseStrand() {
        if ( ! isPaired() ) {
            throw new IllegalStateException("Cannot get mate information for an unpaired read");
        }

        return samRecord.getMateNegativeStrandFlag();
    }

    @Override
    public boolean isFirstOfPair() {
        return samRecord.getFirstOfPairFlag();
    }

    @Override
    public boolean isSecondOfPair() {
        return samRecord.getSecondOfPairFlag();
    }

    @Override
    public boolean isNonPrimaryAlignment() {
        return samRecord.getNotPrimaryAlignmentFlag();
    }

    @Override
    public boolean isSupplementaryAlignment() {
        return samRecord.getSupplementaryAlignmentFlag();
    }

    @Override
    public boolean failsVendorQualityCheck() {
        return samRecord.getReadFailsVendorQualityCheckFlag();
    }

    @Override
    public boolean isDuplicate() {
        return samRecord.getDuplicateReadFlag();
    }

    @Override
    public boolean hasAttribute( final String attributeName ) {
        return samRecord.getAttribute(attributeName) != null;
    }

    @Override
    public Integer getAttributeAsInteger( final String attributeName ) {
        try {
            return samRecord.getIntegerAttribute(attributeName);
        }
        catch ( RuntimeException e ) {
            throw new GATKException.ReadAttributeTypeMismatch(attributeName, "integer", e);
        }
    }

    @Override
    public String getAttributeAsString( final String attributeName ) {
        try {
            return samRecord.getStringAttribute(attributeName);
        }
        catch ( SAMException e ) {
            throw new GATKException.ReadAttributeTypeMismatch(attributeName, "String", e);
        }
    }

    @Override
    public byte[] getAttributeAsByteArray( final String attributeName ) {
        try {
            return samRecord.getByteArrayAttribute(attributeName);
        }
        catch ( SAMException e ) {
            throw new GATKException.ReadAttributeTypeMismatch(attributeName, "byte array", e);
        }
    }

    @Override
    public MutableGATKRead copy() {
        // Produces a shallow but "safe to use" copy. TODO: perform a deep copy here
        return new SAMRecordToGATKReadAdapter(ReadUtils.cloneSAMRecord(samRecord));
    }

    @Override
    public SAMRecord convertToSAMRecord( final SAMFileHeader header ) {
        final SAMRecord copy = ReadUtils.cloneSAMRecord(samRecord);

        if ( header != null ) {
            copy.setHeader(header);
        }

        return copy;
    }

    @Override
    public Read convertToGoogleGenomicsRead() {
        // TODO: this converter is imperfect/lossy and should either be patched or replaced
        return ReadConverter.makeRead(samRecord);
    }

    @Override
    public void setName( final String name ) {
        samRecord.setReadName(name);
    }

    @Override
    public void setPosition( final String contig, final int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        samRecord.setReferenceName(contig);
        samRecord.setAlignmentStart(start);
        samRecord.setReadUnmappedFlag(false);
    }

    @Override
    public void setPosition( final Locatable locatable ) {
        if ( locatable == null ) {
            throw new IllegalArgumentException("Cannot set read position to null");
        }

        setPosition(locatable.getContig(), locatable.getStart());
    }

    @Override
    public void setMatePosition( final String contig, final int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        samRecord.setMateReferenceName(contig);
        samRecord.setMateAlignmentStart(start);
        samRecord.setMateUnmappedFlag(false);
    }

    @Override
    public void setMatePosition( final Locatable locatable ) {
        if ( locatable == null ) {
            throw new IllegalArgumentException("Cannot set mate position to null");
        }

        setMatePosition(locatable.getContig(), locatable.getStart());
    }

    @Override
    public void setFragmentLength( final int fragmentLength ) {
        // May be negative if mate maps to lower position than read
        samRecord.setInferredInsertSize(fragmentLength);
    }

    @Override
    public void setMappingQuality( final int mappingQuality ) {
        if ( mappingQuality < 0 || mappingQuality > 255 ) {
            throw new IllegalArgumentException("mapping quality must be >= 0 and <= 255");
        }

        samRecord.setMappingQuality(mappingQuality);
    }

    @Override
    public void setBases( final byte[] bases ) {
        samRecord.setReadBases(bases);
    }

    @Override
    public void setBaseQualities( final byte[] baseQualities ) {
        samRecord.setBaseQualities(baseQualities);
    }

    @Override
    public void setCigar( final Cigar cigar ) {
        samRecord.setCigar(cigar);
    }

    @Override
    public void setCigar( final String cigarString ) {
        samRecord.setCigarString(cigarString);
    }

    @Override
    public void setReadGroup( final String readGroupID ) {
        samRecord.setAttribute(SAMTag.RG.name(), readGroupID);
    }

    @Override
    public void setNumberOfReadsInFragment( final int numberOfReads ) {
        if ( numberOfReads < 1 ) {
            throw new IllegalArgumentException("number of reads in fragment must be >= 1");
        }
        else if ( numberOfReads == 1 ) {
            setIsPaired(false);
        }
        else if ( numberOfReads == 2 ) {
            setIsPaired(true);
        }
        else {
            throw new IllegalArgumentException("The underlying read type (SAMRecord) does not support fragments with > 2 reads");
        }
    }

    @Override
    public void setReadNumber( final int readNumber) {
        if ( readNumber < 1 ) {
            throw new IllegalArgumentException("read number must be >= 1");
        }
        else if ( readNumber == 1 ) {
            setIsFirstOfPair(true);
            setIsSecondOfPair(false);
        }
        else if ( readNumber == 2 ) {
            setIsFirstOfPair(false);
            setIsSecondOfPair(true);
        }
        else {
            throw new IllegalArgumentException("The underlying read type (SAMRecord) does not support fragments with > 2 reads");
        }
    }

    @Override
    public void setIsPaired( final boolean isPaired ) {
        samRecord.setReadPairedFlag(isPaired);
    }

    @Override
    public void setIsProperlyPaired( final boolean isProperlyPaired ) {
        if ( isProperlyPaired ) {
            setIsPaired(true);
        }

        samRecord.setProperPairFlag(isProperlyPaired);
    }

    @Override
    public void setIsUnmapped() {
        samRecord.setReadUnmappedFlag(true);
    }

    @Override
    public void setMateIsUnmapped() {
        samRecord.setMateUnmappedFlag(true);
    }

    @Override
    public void setIsReverseStrand( final boolean isReverseStrand ) {
        samRecord.setReadNegativeStrandFlag(isReverseStrand);
    }

    @Override
    public void setMateIsReverseStrand( final boolean mateIsReverseStrand ) {
        samRecord.setMateNegativeStrandFlag(mateIsReverseStrand);
    }

    @Override
    public void setIsFirstOfPair( final boolean isFirstOfPair ) {
        setIsPaired(true);
        samRecord.setFirstOfPairFlag(isFirstOfPair);
        samRecord.setSecondOfPairFlag(! isFirstOfPair);
    }

    @Override
    public void setIsSecondOfPair( final boolean isSecondOfPair ) {
        setIsPaired(true);
        samRecord.setSecondOfPairFlag(isSecondOfPair);
        samRecord.setFirstOfPairFlag(! isSecondOfPair);
    }

    @Override
    public void setIsNonPrimaryAlignment( final boolean isNonPrimaryAlignment ) {
        samRecord.setNotPrimaryAlignmentFlag(isNonPrimaryAlignment);
    }

    @Override
    public void setIsSupplementaryAlignment( final boolean isSupplementaryAlignment ) {
        samRecord.setSupplementaryAlignmentFlag(isSupplementaryAlignment);
    }

    @Override
    public void setFailsVendorQualityCheck( final boolean failsVendorQualityCheck ) {
        samRecord.setReadFailsVendorQualityCheckFlag(failsVendorQualityCheck);
    }

    @Override
    public void setIsDuplicate( final boolean isDuplicate ) {
        samRecord.setDuplicateReadFlag(isDuplicate);
    }

    @Override
    public void setAttribute( final String attributeName, final Integer attributeValue ) {
        samRecord.setAttribute(attributeName, attributeValue);
    }

    @Override
    public void setAttribute( final String attributeName, final String attributeValue ) {
        samRecord.setAttribute(attributeName, attributeValue);
    }

    @Override
    public void setAttribute( final String attributeName, final byte[] attributeValue ) {
        samRecord.setAttribute(attributeName, attributeValue);
    }

    @Override
    public void clearAttribute( final String attributeName ) {
        samRecord.setAttribute(attributeName, null);
    }

    @Override
    public void clearAttributes() {
        samRecord.clearAttributes();
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        SAMRecordToGATKReadAdapter that = (SAMRecordToGATKReadAdapter) o;

        if ( samRecord != null ? !samRecord.equals(that.samRecord) : that.samRecord != null ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return samRecord != null ? samRecord.hashCode() : 0;
    }
}
