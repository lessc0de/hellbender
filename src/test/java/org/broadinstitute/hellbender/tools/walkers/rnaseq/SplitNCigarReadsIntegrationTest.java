package org.broadinstitute.hellbender.tools.walkers.rnaseq;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.GenomeLocParser;
import org.broadinstitute.hellbender.utils.test.ReadClipperTestUtils;
import org.broadinstitute.hellbender.utils.fasta.CachingIndexedFastaSequenceFile;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * Tests all possible (and valid) cigar strings that might contain any cigar elements. It uses a code that were written to test the ReadClipper walker.
 * For valid cigar sting in length 8 there are few thousands options, with N in every possible option and with more than one N (for example 1M1N1M1N1M1N2M).
 * The cigarElements array is used to provide all the possible cigar element that might be included.
 */
public final class SplitNCigarReadsIntegrationTest extends CommandLineProgramTest {
    final static CigarElement[] cigarElements = {
            new CigarElement(1, CigarOperator.HARD_CLIP),
            new CigarElement(1, CigarOperator.SOFT_CLIP),
            new CigarElement(1, CigarOperator.INSERTION),
            new CigarElement(1, CigarOperator.DELETION),
            new CigarElement(1, CigarOperator.MATCH_OR_MISMATCH),
            new CigarElement(1, CigarOperator.SKIPPED_REGION)
    };

    private static File REFERENCE_FASTA = new File(BaseTest.exampleReference);

    private IndexedFastaSequenceFile referenceReader;
    private GenomeLocParser genomeLocParser;

    @BeforeClass
    public void setup() throws FileNotFoundException {
        referenceReader = new CachingIndexedFastaSequenceFile(REFERENCE_FASTA);
        genomeLocParser = new GenomeLocParser(referenceReader.getSequenceDictionary());
    }

    @Override
    public String getTestedClassName() {
        return SplitNCigarReads.class.getSimpleName();
    }

    private final class TestManager extends OverhangFixingManager {
        public TestManager() {
            super(null, genomeLocParser, referenceReader, 10000, 1, 40, false);
        }
    }

    @Test
    public void splitReadAtN() {
        final int maxCigarElements = 9;
        final List<Cigar> cigarList = ReadClipperTestUtils.generateCigarList(maxCigarElements, cigarElements);

        // For Debugging use those lines (instead of above cigarList) to create specific read:
        //------------------------------------------------------------------------------------
        // final SAMRecord tmpRead = SAMRecord.createRandomRead(6);
        // tmpRead.setCigarString("1M1N1M");

        // final List<Cigar> cigarList = new ArrayList<>();
        // cigarList.add(tmpRead.getCigar());

        for(Cigar cigar: cigarList){

            final int numOfSplits = numOfNElements(cigar.getCigarElements());

            if(numOfSplits != 0 && isCigarDoesNotHaveEmptyRegionsBetweenNs(cigar)){

                final TestManager manager = new TestManager();
                SAMRecord read = ReadClipperTestUtils.makeReadFromCigar(cigar);
                SplitNCigarReads.splitNCigarRead(read, manager);
                List<OverhangFixingManager.SplitRead> splitReads = manager.getReadsInQueueForTesting();
                final int expectedReads = numOfSplits+1;
                Assert.assertEquals(splitReads.size(),expectedReads,"wrong number of reads after split read with cigar: "+cigar+" at Ns [expected]: "+expectedReads+" [actual value]: "+splitReads.size());
                final List<Integer> readLengths = consecutiveNonNElements(read.getCigar().getCigarElements());
                int index = 0;
                int offsetFromStart = 0;
                for(final OverhangFixingManager.SplitRead splitRead: splitReads){
                    int expectedLength = readLengths.get(index);
                    Assert.assertTrue(splitRead.read.getReadLength() == expectedLength,
                            "the "+index+" (starting with 0) split read has a wrong length.\n" +
                                    "cigar of original read: "+cigar+"\n"+
                                    "expected length: "+expectedLength+"\n"+
                                    "actual length: "+splitRead.read.getReadLength()+"\n");
                    assertBases(splitRead.read.getReadBases(), read.getReadBases(), offsetFromStart);
                    index++;
                    offsetFromStart += expectedLength;
                }
            }
        }
    }

    private int numOfNElements(final List<CigarElement> cigarElements){
        int numOfNElements = 0;
        for (CigarElement element: cigarElements){
            if (element.getOperator() == CigarOperator.SKIPPED_REGION)
                numOfNElements++;
        }
        return numOfNElements;
    }

    private static boolean isCigarDoesNotHaveEmptyRegionsBetweenNs(final Cigar cigar) {
        boolean sawM = false;
        boolean sawS = false;

        for (CigarElement cigarElement : cigar.getCigarElements()) {
            if (cigarElement.getOperator().equals(CigarOperator.SKIPPED_REGION)) {
                if(!sawM && !sawS)
                    return false;
                sawM = false;
                sawS = false;
            }
            if (cigarElement.getOperator().equals(CigarOperator.MATCH_OR_MISMATCH))
                sawM = true;
            if (cigarElement.getOperator().equals(CigarOperator.SOFT_CLIP))
                sawS = true;

        }
        if(!sawS && !sawM)
            return false;
        return true;
    }

    private List<Integer> consecutiveNonNElements(final List<CigarElement> cigarElements){
        final LinkedList<Integer> results = new LinkedList<>();
        int consecutiveLength = 0;
        for(CigarElement element: cigarElements){
            final CigarOperator op = element.getOperator();
            if(op.equals(CigarOperator.MATCH_OR_MISMATCH) || op.equals(CigarOperator.SOFT_CLIP) || op.equals(CigarOperator.INSERTION)){
                consecutiveLength += element.getLength();
            }
            else if(op.equals(CigarOperator.SKIPPED_REGION))
            {
                if(consecutiveLength != 0){
                    results.addLast(consecutiveLength);
                    consecutiveLength = 0;
                }
            }
        }
        if(consecutiveLength != 0)
            results.addLast(consecutiveLength);
        return results;
    }

    private void assertBases(final byte[] actualBase, final byte[] expectedBase, final int startIndex) {
        for (int i = 0; i < actualBase.length; i++) {
            Assert.assertEquals(actualBase[i], expectedBase[startIndex + i],"unmatched bases between: "+ Arrays.toString(actualBase)+"\nand:\n"+Arrays.toString(expectedBase)+"\nat position: "+i);
        }
    }

}
