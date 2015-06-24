//package org.broadinstitute.hellbender.tools.walkers.filters;
//
//import org.testng.annotations.Test;
//
//import java.util.Arrays;
//
//public class VariantFiltrationIntegrationTest extends WalkerTest {
//
//    public static String baseTestString() {
//        return "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b36KGReference;
//    }
//
//
//    @Test
//    public void testNoAction() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("a890cd298298e22bc04a2e5a20b71170"));
//        executeTest("test no action", spec);
//    }
//
//    @Test
//    public void testClusteredSnps() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " -window 10 --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("f46b2fe2dbe6a423b5cfb10d74a4966d"));
//        executeTest("test clustered SNPs", spec);
//    }
//
//    @Test
//    public void testMask1() {
//        WalkerTestSpec spec1 = new WalkerTestSpec(
//                baseTestString() + " -maskName foo --mask " + privateTestDir + "vcfexample2.vcf --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("86dbbf62a0623b2dc5e8969c26d8cb28"));
//        executeTest("test mask all", spec1);
//    }
//
//    @Test
//    public void testMask2() {
//        WalkerTestSpec spec2 = new WalkerTestSpec(
//                baseTestString() + " -maskName foo --mask:VCF " + privateTestDir + "vcfMask.vcf --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("2fb33fccda1eafeea7a2f8f9219baa39"));
//        executeTest("test mask some", spec2);
//    }
//
//    @Test
//    public void testMask3() {
//        WalkerTestSpec spec3 = new WalkerTestSpec(
//                baseTestString() + " -maskName foo -maskExtend 10 --mask:VCF " + privateTestDir + "vcfMask.vcf --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("4351e00bd9d821e37cded5a86100c973"));
//        executeTest("test mask extend", spec3);
//    }
//
//    @Test
//    public void testMaskReversed() {
//        WalkerTestSpec spec3 = new WalkerTestSpec(
//                baseTestString() + " -maskName outsideGoodSites -filterNotInMask --mask:BED " + privateTestDir + "goodMask.bed --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("e65d27c13953fc3a77dcad27a4357786"));
//        executeTest("test filter sites not in mask", spec3);
//    }
//
//    @Test
//    public void testIllegalFilterName() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " -filter 'DoC < 20 || FisherStrand > 20.0' -filterName 'foo < foo' --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                UserException.class);
//        executeTest("test illegal filter name", spec);
//    }
//
//    @Test
//    public void testFilter1() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " -filter 'DoC < 20 || FisherStrand > 20.0' -filterName foo --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("2f056b50a41c8e6ba7645ff4c777966d"));
//        executeTest("test filter #1", spec);
//    }
//
//    @Test
//    public void testFilter2() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " -filter 'AlleleBalance < 70.0 && FisherStrand == 1.4' -filterName bar --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("b2a8c1a5d99505be79c03120e9d75f2f"));
//        executeTest("test filter #2", spec);
//    }
//
//    @Test
//    public void testFilterWithSeparateNames() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " --filterName ABF -filter 'AlleleBalance < 0.7' --filterName FSF -filter 'FisherStrand == 1.4' --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("e350d9789bbdf334c1677506590d0798"));
//        executeTest("test filter with separate names #2", spec);
//    }
//
//    @Test
//    public void testInvertFilter() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " --filterName ABF -filter 'AlleleBalance < 0.7' --filterName FSF -filter 'FisherStrand == 1.4' --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000 --invertFilterExpression", 1,
//                Arrays.asList("d478fd6bcf0884133fe2a47adf4cd765"));
//        executeTest("test inversion of selection of filter with separate names #2", spec);
//    }
//
//    @Test
//    public void testInvertJexlFilter() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " --filterName ABF -filter 'AlleleBalance >= 0.7' --filterName FSF -filter 'FisherStrand != 1.4' --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("6fa6cd89bfc8b6b4dfc3da25eb36d08b")); // Differs from testInvertFilter() because their VCF header FILTER description uses the -filter argument. Their filter statuses are identical.
//        executeTest("test inversion of selection of filter via JEXL with separate names #2", spec);
//    }
//
//    @Test
//    public void testGenotypeFilters1() {
//        WalkerTestSpec spec1 = new WalkerTestSpec(
//                baseTestString() + " -G_filter 'GQ == 0.60' -G_filterName foo --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("060e9e7b6faf8b2f7b3291594eb6b39c"));
//        executeTest("test genotype filter #1", spec1);
//    }
//
//    @Test
//    public void testGenotypeFilters2() {
//        WalkerTestSpec spec2 = new WalkerTestSpec(
//                baseTestString() + " -G_filter 'isHomVar == 1' -G_filterName foo --variant " + privateTestDir + "vcfexample2.vcf -L 1:10,020,000-10,021,000", 1,
//                Arrays.asList("00f90028a8c0d56772c47f039816b585"));
//        executeTest("test genotype filter #2", spec2);
//    }
//
//    @Test
//    public void testDeletions() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                baseTestString() + " --filterExpression 'QUAL < 100' --filterName foo --variant:VCF " + privateTestDir + "twoDeletions.vcf", 1,
//                Arrays.asList("8077eb3bab5ff98f12085eb04176fdc9"));
//        executeTest("test deletions", spec);
//    }
//
//    @Test
//    public void testUnfilteredBecomesFilteredAndPass() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//            "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b37KGReference
//                    + " --filterExpression 'FS > 60.0' --filterName SNP_FS -V " + privateTestDir + "unfilteredForFiltering.vcf", 1,
//                Arrays.asList("8ed32a2272bab8043a255362335395ef"));
//        executeTest("testUnfilteredBecomesFilteredAndPass", spec);
//    }
//
//    @Test
//    public void testFilteringDPfromINFO() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b37KGReference
//                        + " --filterExpression 'DP < 8' --filterName lowDP -V " + privateTestDir + "filteringDepthInFormat.vcf", 1,
//                Arrays.asList("a01f7cce53ea556c9741aa60b6124c41"));
//        executeTest("testFilteringDPfromINFO", spec);
//    }
//
//    @Test
//    public void testFilteringDPfromFORMAT() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b37KGReference
//                        + " --genotypeFilterExpression 'DP < 8' --genotypeFilterName lowDP -V " + privateTestDir + "filteringDepthInFormat.vcf", 1,
//                Arrays.asList("e10485c7c33d9211d0c1294fd7858476"));
//        executeTest("testFilteringDPfromFORMAT", spec);
//    }
//
//    @Test
//    public void testInvertGenotypeFilterExpression() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b37KGReference
//                        + " --genotypeFilterExpression 'DP < 8' --genotypeFilterName highDP -V " + privateTestDir + "filteringDepthInFormat.vcf --invertGenotypeFilterExpression", 1,
//                Arrays.asList("d2664870e7145eb73a2295766482c823"));
//        executeTest("testInvertGenotypeFilterExpression", spec);
//    }
//
//    @Test
//    public void testInvertJexlGenotypeFilterExpression() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b37KGReference
//                        + " --genotypeFilterExpression 'DP >= 8' --genotypeFilterName highDP -V " + privateTestDir + "filteringDepthInFormat.vcf", 1,
//                Arrays.asList("8ddd8f3b5ee351c4ab79cb186b1d45ba")); // Differs from testInvertFilter because FILTER description uses the -genotypeFilterExpression argument
//        executeTest("testInvertJexlGenotypeFilterExpression", spec);
//    }
//
//    @Test
//    public void testSetFilteredGtoNocall() {
//        WalkerTestSpec spec = new WalkerTestSpec(
//                "-T VariantFiltration -o %s --no_cmdline_in_header -R " + b37KGReference
//                        + " --genotypeFilterExpression 'DP < 8' --genotypeFilterName lowDP -V " + privateTestDir + "filteringDepthInFormat.vcf --setFilteredGtToNocall", 1,
//                Arrays.asList("9ff801dd726eb4fc562b278ccc6854b1"));
//        executeTest("testSetFilteredGtoNocall", spec);
//    }
//
//    @Test
//    public void testSetVcfFilteredGtoNocall() {
//        String testfile = privateTestDir + "filteredSamples.vcf";
//
//        WalkerTestSpec spec = new WalkerTestSpec(
//                "-T SelectVariants --setFilteredGtToNocall -R " + b37KGReference + " --variant " + testfile + " -o %s --no_cmdline_in_header",
//                1,
//                Arrays.asList("81b99386a64a8f2b857a7ef2bca5856e")
//        );
//
//        spec.disableShadowBCF();
//        executeTest("testSetVcfFilteredGtoNocall--" + testfile, spec);
//    }
//}
