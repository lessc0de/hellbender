package org.broadinstitute.hellbender.tools.recalibration.covariates;

import htsjdk.samtools.SAMRecord;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.recalibration.ReadCovariates;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationArgumentCollection;
import org.broadinstitute.hellbender.utils.NGSPlatform;
import org.broadinstitute.hellbender.utils.SequencerFlowClass;

import java.io.Serializable;

/**
 * The Cycle covariate.
 * For Solexa the cycle is simply the position in the read (counting backwards if it is a negative strand read)
 * For 454 the cycle is the TACG flow cycle, that is, each flow grabs all the TACG's in order in a single cycle
 * For example, for the read: AAACCCCGAAATTTTTACTG
 * the cycle would be 11111111222333333344
 * For SOLiD the cycle is a more complicated mixture of ligation cycle and primer round
 */

public final class CycleCovariate implements Covariate {

    private final int MAXIMUM_CYCLE_VALUE;
    public static final int CUSHION_FOR_INDELS = 4;
    private final String default_platform;   //can be null

    public CycleCovariate(final RecalibrationArgumentCollection RAC){
        this.MAXIMUM_CYCLE_VALUE = RAC.MAXIMUM_CYCLE_VALUE;

        if (RAC.DEFAULT_PLATFORM != null && !NGSPlatform.isKnown(RAC.DEFAULT_PLATFORM)) {
            throw new UserException.CommandLineException("The requested default platform (" + RAC.DEFAULT_PLATFORM + ") is not a recognized platform.");
        }

        default_platform = RAC.DEFAULT_PLATFORM;
    }

    // Used to pick out the covariate's value from attributes of the read
    @Override
    public void recordValues(final SAMRecord read, final ReadCovariates values) {
        final NGSPlatform ngsPlatform = default_platform == null ? NGSPlatform.fromRead(read) : NGSPlatform.fromReadGroupPL(default_platform);

        // Discrete cycle platforms
        if (ngsPlatform.getSequencerType() == SequencerFlowClass.DISCRETE) {
            final int readLength = read.getReadLength();
            for (int i = 0; i < readLength; i++) {
                final int substitutionKey = cycleKey(i, read, false, MAXIMUM_CYCLE_VALUE);
                final int indelKey        = cycleKey(i, read, true, MAXIMUM_CYCLE_VALUE);
                values.addCovariate(substitutionKey, indelKey, indelKey, i);
            }
        }

        // Flow cycle platforms
        else if (ngsPlatform.getSequencerType() == SequencerFlowClass.FLOW) {
            throw new UserException("The platform (" + read.getReadGroup().getPlatform()
                    + ") associated with read group " + read.getReadGroup()
                    + " is not a supported platform.");
        }
        // Unknown platforms
        else {
            throw new UserException("The platform (" + read.getReadGroup().getPlatform()
                    + ") associated with read group " + read.getReadGroup()
                    + " is not a recognized platform. Allowable options are " + NGSPlatform.knownPlatformsString());
        }
    }

    @Override
    public String formatKey(final int key){
            return String.format("%d", cycleFromKey(key));
    }

    @Override
    public int keyFromValue(final Object value) {
        return (value instanceof String) ? keyFromCycle(Integer.parseInt((String) value), MAXIMUM_CYCLE_VALUE) : keyFromCycle((Integer) value, MAXIMUM_CYCLE_VALUE);
    }

    @Override
    public int maximumKeyValue() {
        return (MAXIMUM_CYCLE_VALUE << 1) + 1;
    }

    /**
     * Computes the encoded value of CycleCovariate's key for the given position at the read.
     * Uses keyFromCycle to do the encoding.
     * @param baseNumber index of the base to compute the key for
     * @param read the read
     * @param indel is this an indel key or a substitution key?
     * @param maxCycle max value of the base to compute the key for
     *                 (this method throws UserException if the computed absolute value of the cycle number is higher than this value).
     */
    public static int cycleKey(final int baseNumber, final SAMRecord read, final boolean indel, final int maxCycle) {
        final boolean isNegStrand = read.getReadNegativeStrandFlag();
        final boolean isSecondInPair = read.getReadPairedFlag() && read.getSecondOfPairFlag();
        final int readLength = read.getReadLength();

        final int readOrderFactor = isSecondInPair ? -1 : 1;
        final int increment;
        int cycle;
        if (isNegStrand) {
            cycle = readLength * readOrderFactor;
            increment = -1 * readOrderFactor;
        } else {
            cycle = readOrderFactor;
            increment = readOrderFactor;
        }

        cycle += baseNumber * increment;

        if (!indel) {
            return CycleCovariate.keyFromCycle(cycle, maxCycle);
        }
        final int maxCycleForIndels = readLength - CycleCovariate.CUSHION_FOR_INDELS - 1;
        if (baseNumber < CycleCovariate.CUSHION_FOR_INDELS || baseNumber > maxCycleForIndels) {
            return -1;
        } else {
            return CycleCovariate.keyFromCycle(cycle, maxCycle);
        }
    }

    /**
     * Decodes the cycle number from the key.
     */
    public static int cycleFromKey(final int key) {
        int cycle = key >> 1; // shift so we can remove the "sign" bit
        if ( (key & 1) != 0 ) { // is the last bit set?
            cycle *= -1; // then the cycle is negative
        }
        return cycle;
    }

    /**
     * Encodes the cycle number as a key.
     */
    public static int keyFromCycle(final int cycle, final int maxCycle) {
        // no negative values because values must fit into the first few bits of the long
        int result = Math.abs(cycle);
        if ( result > maxCycle ) {
            throw new UserException("The maximum allowed value for the cycle is " + maxCycle + ", but a larger cycle (" + result + ") was detected.  Please use the --maximum_cycle_value argument to increase this value (at the expense of requiring more memory to run)");
        }

        result <<= 1; // shift so we can add the "sign" bit
        if ( cycle < 0 ) {
            result++; // negative cycles get the lower-most bit set
        }
        return result;
    }
}