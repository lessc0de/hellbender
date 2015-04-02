package org.broadinstitute.hellbender.utils.read;


import com.google.api.services.genomics.model.LinearAlignment;
import com.google.api.services.genomics.model.Position;
import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.gatk.common.GenomicsConverter;
import htsjdk.samtools.*;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.StringUtil;
import org.broadinstitute.hellbender.engine.dataflow.coders.UUIDCoder;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.util.*;

public final class GoogleGenomicsReadToGATKReadAdapter implements MutableGATKRead {

    private final Read genomicsRead;
    private UUID uuid;

    public GoogleGenomicsReadToGATKReadAdapter( final Read genomicsRead ) {
        this.genomicsRead = genomicsRead;
        this.uuid = UUID.randomUUID();
    }

    public static final DelegateCoder<GATKRead, KV<UUID, Read>> CODER =
        DelegateCoder.of(

            KvCoder.of(UUIDCoder.CODER, GenericJsonCoder.of(Read.class)),
                    new DelegateCoder.CodingFunction<GATKRead, KV<UUID, Read>>() {
                        @Override
                        public KV<UUID, Read> apply( GATKRead gatkRead ) throws Exception {
                            return KV.of(gatkRead.getUUID(), gatkRead.convertToGoogleGenomicsRead());
                        }
                    },
                    new DelegateCoder.CodingFunction<KV<UUID, Read>, GATKRead>() {
                        @Override
                        public GATKRead apply( KV<UUID, Read> read ) throws Exception {
                            final GoogleGenomicsReadToGATKReadAdapter adapter = new GoogleGenomicsReadToGATKReadAdapter(read.getValue());
                            adapter.setUUID(read.getKey());
                            return adapter;
                        }
                    }
            );

    private static <T> T assertFieldValueNotNull( final T fieldValue, final String fieldName ) {
        if ( fieldValue == null ) {
            throw new GATKException.MissingReadField(fieldName);
        }
        return fieldValue;
    }

    private void assertHasAlignment() {
        assertFieldValueNotNull(genomicsRead.getAlignment(), "alignment");
    }

    private void assertHasPosition() {
        assertHasAlignment();
        assertFieldValueNotNull(genomicsRead.getAlignment().getPosition(), "position");
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

        // Guaranteed non-null due to isUnmapped() check above.
        return genomicsRead.getAlignment().getPosition().getReferenceName();
    }

    @Override
    public int getStart() {
        if ( isUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        // Guaranteed non-null due to isUnmapped() check above.
        // Convert from 0-based to 1-based start position
        return genomicsRead.getAlignment().getPosition().getPosition().intValue() + 1;
    }

    @Override
    public int getEnd() {
        if ( isUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        // Guaranteed non-null due to isUnmapped() check above.
        // Position in genomicsRead is 0-based, so add getCigar().getReferenceLength() to it,
        // not getCigar().getReferenceLength() - 1, in order to convert to a 1-based end position.
        return genomicsRead.getAlignment().getPosition().getPosition().intValue() + getCigar().getReferenceLength();
    }

    @Override
    public String getName() {
        return genomicsRead.getFragmentName();
    }

    @Override
    public int getLength() {
        return getBases().length;
    }

    @Override
    public int getUnclippedStart() {
        final int start = getStart();
        return start == ReadConstants.UNSET_POSITION ? ReadConstants.UNSET_POSITION :
                                                       SAMUtils.getUnclippedStart(start, getCigar());
    }

    @Override
    public int getUnclippedEnd() {
        final int end = getEnd();
        return end == ReadConstants.UNSET_POSITION ? ReadConstants.UNSET_POSITION :
                                                     SAMUtils.getUnclippedEnd(getEnd(), getCigar());
    }

    @Override
    public String getMateContig() {
        if ( mateIsUnmapped() ) {
            return null;
        }

        // Guaranteed non-null due to mateIsUnmapped() check above.
        return genomicsRead.getNextMatePosition().getReferenceName();
    }

    @Override
    public int getMateStart() {
        if ( mateIsUnmapped() ) {
            return ReadConstants.UNSET_POSITION;
        }

        // Guaranteed non-null due to mateIsUnmapped() check above.
        // Convert from 0-based to 1-based position.
        return genomicsRead.getNextMatePosition().getPosition().intValue() + 1;
    }

    @Override
    public int getFragmentLength() {
        return genomicsRead.getFragmentLength() != null ? genomicsRead.getFragmentLength() : 0;
    }

    @Override
    public int getMappingQuality() {
        if ( genomicsRead.getAlignment() == null || genomicsRead.getAlignment().getMappingQuality() == null ) {
            return ReadConstants.NO_MAPPING_QUALITY;
        }

        return genomicsRead.getAlignment().getMappingQuality();
    }

    @Override
    public byte[] getBases() {
        final String basesString = genomicsRead.getAlignedSequence();
        if ( basesString == null || basesString.isEmpty() || basesString.equals(SAMRecord.NULL_SEQUENCE_STRING) ) {
            return new byte[0];
        }

        return StringUtil.stringToBytes(basesString);
    }

    @Override
    public byte[] getBaseQualities() {
        final List<Integer> baseQualities = genomicsRead.getAlignedQuality();
        if ( baseQualities == null || baseQualities.isEmpty() ) {
            return new byte[0];
        }

        byte[] convertedBaseQualities = new byte[baseQualities.size()];
        for ( int i = 0; i < baseQualities.size(); ++i ) {
            if ( baseQualities.get(i) < 0 || baseQualities.get(i) > Byte.MAX_VALUE ) {
                throw new GATKException("Base quality score " + baseQualities.get(i) + " is invalid and/or not convertible to byte");
            }
            convertedBaseQualities[i] = baseQualities.get(i).byteValue();
        }

        return convertedBaseQualities;
    }

    @Override
    public Cigar getCigar() {
        if ( genomicsRead.getAlignment() == null || genomicsRead.getAlignment().getCigar() == null ) {
            return new Cigar();
        }

        return CigarConverter.convertCigarUnitListToSAMCigar(genomicsRead.getAlignment().getCigar());
    }

    @Override
    public String getReadGroup() {
        // May be null
        return genomicsRead.getReadGroupId();
    }

    @Override
    public int getNumberOfReadsInFragment() {
        return genomicsRead.getNumberReads() != null ? genomicsRead.getNumberReads() : 0;
    }

    @Override
    public int getReadNumber() {
        // Convert from 0-based to 1-based numbering
        return genomicsRead.getReadNumber() != null ? genomicsRead.getReadNumber() + 1 : 0;
    }

    @Override
    public boolean isPaired() {
        assertFieldValueNotNull(genomicsRead.getNumberReads(), "number of reads");
        return genomicsRead.getNumberReads() == 2;
    }

    @Override
    public boolean isProperlyPaired() {
        assertFieldValueNotNull(genomicsRead.getProperPlacement(), "proper placement");
        return isPaired() && genomicsRead.getProperPlacement();
    }

    @Override
    public boolean isUnmapped() {
        return genomicsRead.getAlignment() == null ||
               positionIsUnmapped(genomicsRead.getAlignment().getPosition());
    }

    @Override
    public boolean mateIsUnmapped() {
        if ( ! isPaired() ) {
            throw new IllegalStateException("Cannot get mate information for an unpaired read");
        }

        return positionIsUnmapped(genomicsRead.getNextMatePosition());
    }

    private boolean positionIsUnmapped( final Position position ) {
        return position == null ||
               position.getReferenceName() == null || position.getReferenceName().equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) ||
               position.getPosition() == null || position.getPosition() < 0;
    }

    @Override
    public boolean isReverseStrand() {
        assertHasPosition();
        return assertFieldValueNotNull(genomicsRead.getAlignment().getPosition().getReverseStrand(), "strand");
    }

    @Override
    public boolean mateIsReverseStrand() {
        if ( ! isPaired() ) {
            throw new IllegalStateException("Cannot get mate information for an unpaired read");
        }

        final Position matePosition = assertFieldValueNotNull(genomicsRead.getNextMatePosition(), "mate position");
        return assertFieldValueNotNull(matePosition.getReverseStrand(), "mate strand");
    }

    @Override
    public boolean isFirstOfPair() {
        final int readNumber = assertFieldValueNotNull(genomicsRead.getReadNumber(), "read number");
        return isPaired() && readNumber == 0;
    }

    @Override
    public boolean isSecondOfPair() {
        final int readNumber = assertFieldValueNotNull(genomicsRead.getReadNumber(), "read number");
        return isPaired() && readNumber == 1;
    }

    @Override
    public boolean isNonPrimaryAlignment() {
        return assertFieldValueNotNull(genomicsRead.getSecondaryAlignment(), "secondary alignment");
    }

    @Override
    public boolean isSupplementaryAlignment() {
        return assertFieldValueNotNull(genomicsRead.getSupplementaryAlignment(), "supplementary alignment");
    }

    @Override
    public boolean failsVendorQualityCheck() {
        return assertFieldValueNotNull(genomicsRead.getFailedVendorQualityChecks(), "failed vendor quality checks");
    }

    @Override
    public boolean isDuplicate() {
        return assertFieldValueNotNull(genomicsRead.getDuplicateFragment(), "duplicate fragment");
    }

    @Override
    public boolean hasAttribute( final String attributeName ) {
        return genomicsRead.getInfo() != null && genomicsRead.getInfo().containsKey(attributeName);
    }

    private String getRawAttributeValue( final String attributeName, final String targetType ) {
        if ( genomicsRead.getInfo() == null ) {
            return null;
        }

        final List<String> rawValue = genomicsRead.getInfo().get(attributeName);
        if ( rawValue == null || rawValue.isEmpty() || rawValue.get(0) == null ) {
            return null;
        }

        // We don't support decoding attribute values represented as multiple Strings
        if ( rawValue.size() > 1 ) {
            throw new GATKException.ReadAttributeTypeMismatch(attributeName, targetType);
        }

        return rawValue.get(0);
    }

    @Override
    public Integer getAttributeAsInteger( final String attributeName ) {
        try {
            // Assume that integer attributes are encoded as a single String in the first position of the List of values for an attribute
            final String rawValue = getRawAttributeValue(attributeName, "integer");
            return rawValue != null ? Integer.parseInt(rawValue) : null;
        }
        catch ( NumberFormatException e ) {
            throw new GATKException.ReadAttributeTypeMismatch(attributeName, "integer", e);
        }
    }

    @Override
    public String getAttributeAsString( final String attributeName ) {
        // Assume that String attributes are encoded as a single String in the first position of the List of values for an attribute
        return getRawAttributeValue(attributeName, "String");
    }

    @Override
    public byte[] getAttributeAsByteArray( final String attributeName ) {
        // Assume that byte array attributes are encoded as a single String in the first position of the List of values for an attribute,
        // and that the bytes of this String are directly convertible to byte array.
        final String rawValue = getRawAttributeValue(attributeName, "byte array");
        return rawValue != null ? rawValue.getBytes() : null;
    }

    @Override
    public MutableGATKRead copy() {
        // clone() actually makes a deep copy of all fields here (via GenericData.clone())
        return new GoogleGenomicsReadToGATKReadAdapter(genomicsRead.clone());
    }

    @Override
    public SAMRecord convertToSAMRecord( final SAMFileHeader header ) {
        // TODO: this converter is imperfect and should either be patched or replaced completely.
        return GenomicsConverter.makeSAMRecord(genomicsRead, header);
    }

    @Override
    public Read convertToGoogleGenomicsRead() {
        return genomicsRead.clone();
    }

    @Override
    public void setName( final String name ) {
        genomicsRead.setFragmentName(name);
    }

    private void makeAlignmentIfNecessary() {
        if ( genomicsRead.getAlignment() == null ) {
            genomicsRead.setAlignment(new LinearAlignment());
        }
    }

    private void makePositionIfNecessary() {
        makeAlignmentIfNecessary();

        if ( genomicsRead.getAlignment().getPosition() == null ) {
            genomicsRead.getAlignment().setPosition(new Position());
        }
    }

    @Override
    public void setPosition( final String contig, final int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        makePositionIfNecessary();

        genomicsRead.getAlignment().getPosition().setReferenceName(contig);
        // Convert from a 1-based to a 0-based position
        genomicsRead.getAlignment().getPosition().setPosition((long)start - 1);
    }

    @Override
    public void setPosition( final Locatable locatable ) {
        if ( locatable == null ) {
            throw new IllegalArgumentException("Cannot set read position to null");
        }

        setPosition(locatable.getContig(), locatable.getStart());
    }

    private void makeMatePositionIfNecessary() {
        if ( genomicsRead.getNextMatePosition() == null ) {
            genomicsRead.setNextMatePosition(new Position());
        }
    }

    @Override
    public void setMatePosition( final String contig, final int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        makeMatePositionIfNecessary();

        genomicsRead.getNextMatePosition().setReferenceName(contig);
        // Convert from a 1-based to a 0-based position
        genomicsRead.getNextMatePosition().setPosition((long)start - 1);
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
        genomicsRead.setFragmentLength(fragmentLength);
    }

    @Override
    public void setMappingQuality( final int mappingQuality ) {
        if ( mappingQuality < 0 || mappingQuality > 255 ) {
            throw new IllegalArgumentException("mapping quality must be >= 0 and <= 255");
        }

        makeAlignmentIfNecessary();
        genomicsRead.getAlignment().setMappingQuality(mappingQuality);
    }

    @Override
    public void setBases( final byte[] bases ) {
        genomicsRead.setAlignedSequence(bases != null ? StringUtil.bytesToString(bases) : null);
    }

    @Override
    public void setBaseQualities( final byte[] baseQualities ) {
        if ( baseQualities == null ) {
            genomicsRead.setAlignedQuality(null);
            return;
        }

        final List<Integer> convertedBaseQualities = new ArrayList<>(baseQualities.length);
        for ( byte b : baseQualities ) {
            if ( b < 0 ) {
                throw new GATKException("Base quality score " + b + " is invalid");
            }

            convertedBaseQualities.add((int)b);
        }

        genomicsRead.setAlignedQuality(convertedBaseQualities.isEmpty() ? null : convertedBaseQualities);
    }

    @Override
    public void setCigar( final Cigar cigar ) {
        makeAlignmentIfNecessary();
        genomicsRead.getAlignment().setCigar(cigar != null ? CigarConverter.convertSAMCigarToCigarUnitList(cigar) : null);
    }

    @Override
    public void setCigar( final String cigarString ) {
        makeAlignmentIfNecessary();
        genomicsRead.getAlignment().setCigar(cigarString != null ? CigarConverter.convertSAMCigarToCigarUnitList(TextCigarCodec.decode(cigarString)) : null);
    }

    @Override
    public void setReadGroup( final String readGroupID ) {
        genomicsRead.setReadGroupId(readGroupID);
    }

    @Override
    public void setNumberOfReadsInFragment( final int numberOfReads ) {
        if ( numberOfReads < 1 ) {
            throw new IllegalArgumentException("number of reads in fragment must be >= 1");
        }

        genomicsRead.setNumberReads(numberOfReads);
    }

    @Override
    public void setReadNumber( final int readNumber) {
        if ( readNumber < 1 ) {
            throw new IllegalArgumentException("read number must be >= 1");
        }

        // Convert from 1-based to 0-based numbering
        genomicsRead.setReadNumber(readNumber - 1);
    }

    @Override
    public void setIsPaired( final boolean isPaired ) {
        if ( isPaired ) {
            genomicsRead.setNumberReads(2);
        }
        else {
            genomicsRead.setNumberReads(1);
        }
    }

    @Override
    public void setIsProperlyPaired( final boolean isProperlyPaired ) {
        if ( isProperlyPaired ) {
            setIsPaired(true);
        }

        genomicsRead.setProperPlacement(isProperlyPaired);
    }

    @Override
    public void setIsUnmapped() {
        genomicsRead.setAlignment(null);
    }

    @Override
    public void setMateIsUnmapped() {
        genomicsRead.setNextMatePosition(null);
    }

    @Override
    public void setIsReverseStrand( final boolean isReverseStrand ) {
        makePositionIfNecessary();
        genomicsRead.getAlignment().getPosition().setReverseStrand(isReverseStrand);
    }

    @Override
    public void setMateIsReverseStrand( final boolean mateIsReverseStrand ) {
        makeMatePositionIfNecessary();
        genomicsRead.getNextMatePosition().setReverseStrand(mateIsReverseStrand);
    }

    @Override
    public void setIsFirstOfPair( final boolean isFirstOfPair ) {
        setIsPaired(true);
        setReadNumber(isFirstOfPair ? 1 : 2);
    }

    @Override
    public void setIsSecondOfPair( final boolean isSecondOfPair ) {
        setIsPaired(true);
        setReadNumber(isSecondOfPair ? 2 : 1);
    }

    @Override
    public void setIsNonPrimaryAlignment( final boolean isNonPrimaryAlignment ) {
        genomicsRead.setSecondaryAlignment(isNonPrimaryAlignment);
    }

    @Override
    public void setIsSupplementaryAlignment( final boolean isSupplementaryAlignment ) {
        genomicsRead.setSupplementaryAlignment(isSupplementaryAlignment);
    }

    @Override
    public void setFailsVendorQualityCheck( final boolean failsVendorQualityCheck ) {
        genomicsRead.setFailedVendorQualityChecks(failsVendorQualityCheck);
    }

    @Override
    public void setIsDuplicate( final boolean isDuplicate ) {
        genomicsRead.setDuplicateFragment(isDuplicate);
    }

    @Override
    public void setAttribute( final String attributeName, final Integer attributeValue ) {
        makeInfoMapIfNecessary();
        if ( attributeValue == null ) {
            clearAttribute(attributeName);
            return;
        }

        final List<String> encodedValue = Arrays.asList(attributeValue.toString());
        genomicsRead.getInfo().put(attributeName, encodedValue);
    }

    @Override
    public void setAttribute( final String attributeName, final String attributeValue ) {
        makeInfoMapIfNecessary();
        if ( attributeValue == null ) {
            clearAttribute(attributeName);
            return;
        }

        final List<String> encodedValue = Arrays.asList(attributeValue);
        genomicsRead.getInfo().put(attributeName, encodedValue);
    }

    @Override
    public void setAttribute( final String attributeName, final byte[] attributeValue ) {
        makeInfoMapIfNecessary();
        if ( attributeValue == null ) {
            clearAttribute(attributeName);
            return;
        }

        final List<String> encodedValue = Arrays.asList(new String(attributeValue));
        genomicsRead.getInfo().put(attributeName, encodedValue);
    }

    private void makeInfoMapIfNecessary() {
        if ( genomicsRead.getInfo() == null ) {
            genomicsRead.setInfo(new LinkedHashMap<>());
        }
    }

    @Override
    public void clearAttribute( final String attributeName ) {
        if ( genomicsRead.getInfo() != null ) {
            genomicsRead.getInfo().remove(attributeName);
        }
    }

    @Override
    public void clearAttributes() {
        genomicsRead.setInfo(null);
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        GoogleGenomicsReadToGATKReadAdapter that = (GoogleGenomicsReadToGATKReadAdapter) o;

        if ( genomicsRead != null ? !genomicsRead.equals(that.genomicsRead) : that.genomicsRead != null ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return genomicsRead != null ? genomicsRead.hashCode() : 0;
    }
}
