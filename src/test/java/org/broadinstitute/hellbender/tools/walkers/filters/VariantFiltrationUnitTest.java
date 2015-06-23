package org.broadinstitute.hellbender.tools.walkers.filters;

import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VariantFiltrationUnitTest extends BaseTest {

    private String chr1 = null;
    private GenomeLoc genomeLoc = null;
    private String vcFilter = "testFilter";

    @BeforeTest
    public void before() {
        // Create GenomeLoc
        IndexedFastaSequenceFile fasta = CachingIndexedFastaSequenceFile.checkAndCreate(new File(privateTestDir + "iupacFASTA.fasta"));
        GenomeLocParser genomeLocParser = new GenomeLocParser(fasta);
        chr1 = fasta.getSequenceDictionary().getSequence(0).getSequenceName();
        genomeLoc = genomeLocParser.createGenomeLoc(chr1, 5, 10);
    }

    @DataProvider(name = "VariantMaskData")
    public Object[][] DoesMaskCoverVariantTestData() {

        final String maskName = "testMask";

        List<Object[]> tests = Arrays.asList(new Object[]{chr1, 0, 0, maskName, 10, true, true},
                new Object[]{"chr2", 0, 0, maskName, 10, true, false},
                new Object[]{chr1, 0, 0, null, 10, true, true},
                new Object[]{chr1, 0, 0, maskName, 10, true, true},
                new Object[]{chr1, 0, 0, vcFilter, 10, true, false},
                new Object[]{chr1, 0, 0, maskName, 1, true, false},
                new Object[]{chr1, 15, 15, maskName, 10, false, true},
                new Object[]{chr1, 15, 15, maskName, 1, false, false}
        );
        return tests.toArray(new Object[][]{});
    }

    /**
     * Test doesMaskCoverVariant() logic
     *
     * @param contig chromosome or contig name
     * @param start  variant context start
     * @param stop variant context stop
     * @param maskName mask or filter name
     * @param maskExtension bases beyond the mask
     * @param vcBeforeLoc if true, variant context is before the genome location; if false, the converse is true.
     * @param expectedValue  return the expected return value from doesMaskCoverVariant()
     */
    @Test(dataProvider = "VariantMaskData")
    public void TestDoesMaskCoverVariant(final String contig, final int start, final int stop, final String maskName, final int maskExtension,
                                         final boolean vcBeforeLoc, final boolean expectedValue) {

        // Build VariantContext
        final byte[] allele1 = Utils.dupBytes((byte) 'A', 1);
        final byte[] allele2 = Utils.dupBytes((byte) 'T', 2);

        final List<Allele> alleles = new ArrayList<Allele>(2);
        final Allele ref = Allele.create(allele1, true);
        final Allele alt = Allele.create(allele2, false);
        alleles.add(ref);
        alleles.add(alt);

        final VariantContext vc = new VariantContextBuilder("test", contig, start, stop, alleles).filter(vcFilter).make();

        boolean coversVariant = VariantFiltration.doesMaskCoverVariant(vc, genomeLoc, maskName, maskExtension, vcBeforeLoc);
        Assert.assertEquals(coversVariant, expectedValue);
    }
}
