package org.broadinstitute.hellbender.tools.walkers.filters;

import htsjdk.samtools.util.Locatable;
import htsjdk.tribble.Feature;
import htsjdk.variant.variantcontext.*;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.*;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.RequiredVariantInputArgumentCollection;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;

import java.util.*;


/**
 * Filter variant calls based on INFO and FORMAT annotations
 *
 * <p>
 * This tool is designed for hard-filtering variant calls based on certain criteria.
 * Records are hard-filtered by changing the value in the FILTER field to something other than PASS. Filtered records
 * will be preserved in the output unless their removal is requested in the command line. </p>
 *
 * <h3>Input</h3>
 * <p>
 * A variant set to filter.
 * </p>
 *
 * <h3>Output</h3>
 * <p>
 * A filtered VCF.
 * </p>
 *
 * <h3>Usage example</h3>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T VariantFiltration \
 *   -R reference.fasta \
 *   -o output.vcf \
 *   --variant input.vcf \
 *   --filterExpression "AB < 0.2 || MQ0 > 50" \
 *   --filterName "Nov09filters" \
 *   --mask mask.vcf \
 *   --maskName InDel
 * </pre>
 *
 */
public final class VariantFiltration extends VariantWalker {

    @ArgumentCollection
    public RequiredVariantInputArgumentCollection variantCollection = new RequiredVariantInputArgumentCollection();

    /**
     * Any variant which overlaps entries from the provided mask rod will be filtered. If the user wants logic to be reversed,
     * i.e. filter variants that do not overlap with provided mask, then argument -filterNotInMask can be used.
     * Note that it is up to the user to adapt the name of the mask to make it clear that the reverse logic was used
     * (e.g. if masking against Hapmap, use -maskName=hapmap for the normal masking and -maskName=not_hapmap for the reverse masking).
     */
    @Argument(fullName="mask", shortName="mask", doc="Input ROD mask", optional=true)
    public FeatureInput<Feature> mask;

    @Argument(doc="File to which variants should be written")
    public VariantContextWriter writer = null;

    /**
     * VariantFiltration accepts any number of JEXL expressions (so you can have two named filters by using
     * --filterName One --filterExpression "X < 1" --filterName Two --filterExpression "X > 2").
     */
    @Argument(fullName="filterExpression", shortName="filter", doc="One or more expression used with INFO fields to filter", optional=true)
    public ArrayList<String> filterExpressions = new ArrayList<>(); //XXX htsjdk's API is bogus and wants an ArrayList

    /**
     * This name is put in the FILTER field for variants that get filtered.  Note that there must be a 1-to-1 mapping between filter expressions and filter names.
     */
    @Argument(fullName="filterName", shortName="filterName", doc="Names to use for the list of filters", optional=true)
    public ArrayList<String> filterNames = new ArrayList<>(); //XXX htsjdk's API is bogus and wants an ArrayList

    /**
     * Similar to the INFO field based expressions, but used on the FORMAT (genotype) fields instead.
     * VariantFiltration will add the sample-level FT tag to the FORMAT field of filtered samples (this does not affect the record's FILTER tag).
     * One can filter normally based on most fields (e.g. "GQ < 5.0"), but the GT (genotype) field is an exception. We have put in convenience
     * methods so that one can now filter out hets ("isHet == 1"), refs ("isHomRef == 1"), or homs ("isHomVar == 1"). Also available are
     * expressions isCalled, isNoCall, isMixed, and isAvailable, in accordance with the methods of the Genotype object.
     */
    @Argument(fullName="genotypeFilterExpression", shortName="G_filter", doc="One or more expression used with FORMAT (sample/genotype-level) fields to filter (see documentation guide for more info)", optional=true)
    public ArrayList<String> genotypeFilterExpressions = new ArrayList<>();  //XXX htsjdk's API is bogus and wants an ArrayList

    /**
     * Similar to the INFO field based expressions, but used on the FORMAT (genotype) fields instead.
     */
    @Argument(fullName="genotypeFilterName", shortName="G_filterName", doc="Names to use for the list of sample/genotype filters (must be a 1-to-1 mapping); this name is put in the FILTER field for variants that get filtered", optional=true)
    public ArrayList<String> genotypeFilterNames = new ArrayList<>();   //XXX htsjdk's API is bogus and wants an ArrayList

    /**
     * Works together with the --clusterWindowSize argument.
     */
    @Argument(fullName="clusterSize", shortName="cluster", doc="The number of SNPs which make up a cluster", optional=true)
    public Integer clusterSize = 3;

    /**
     * Works together with the --clusterSize argument.  To disable the clustered SNP filter, set this value to less than 1.
     */
    @Argument(fullName="clusterWindowSize", shortName="window", doc="The window size (in bases) in which to evaluate clustered SNPs", optional=true)
    public Integer clusterWindow = 0;

    @Argument(fullName="maskExtension", shortName="maskExtend", doc="How many bases beyond records from a provided 'mask' rod should variants be filtered", optional=true)
    public Integer maskExtension = 0;

    /**
     * When using the -mask argument, the maskName will be annotated in the variant record.
     * Note that when using the -filterNotInMask argument to reverse the masking logic,
     * it is up to the user to adapt the name of the mask to make it clear that the reverse logic was used
     * (e.g. if masking against Hapmap, use -maskName=hapmap for the normal masking and -maskName=not_hapmap for the reverse masking).
     */
    @Argument(fullName="maskName", shortName="maskName", doc="The text to put in the FILTER field if a 'mask' rod is provided and overlaps with a variant call", optional=true)
    public String maskName = "Mask";

    /**
     * By default, if the -mask argument is used, any variant falling in a mask will be filtered.
     * If this argument is used, logic is reversed, and variants falling outside a given mask will be filtered.
     * Use case is, for example, if we have an interval list or BED file with "good" sites.
     * Note that it is up to the user to adapt the name of the mask to make it clear that the reverse logic was used
     * (e.g. if masking against Hapmap, use -maskName=hapmap for the normal masking and -maskName=not_hapmap for the reverse masking).
     */
    @Argument(fullName="filterNotInMask", shortName="filterNotInMask", doc="Filter records NOT in given input mask.", optional=true)
    public boolean filterRecordsNotInMask = false;

    /**
     * By default, if JEXL cannot evaluate your expression for a particular record because one of the annotations is not present, the whole expression evaluates as PASSing.
     * Use this argument to have it evaluate as failing filters instead for these cases.
     */
    @Argument(fullName="missingValuesInExpressionsShouldEvaluateAsFailing", doc="When evaluating the JEXL expressions, missing values should be considered failing the expression", optional=true)
    public Boolean failMissingValues = false;

    /**
     * Invalidate previous filters applied to the VariantContext, applying only the filters here
     */
    @Argument(fullName="invalidatePreviousFilters",doc="Remove previous filters applied to the VCF",optional=true)
    boolean invalidatePrevious = false;

    /**
     * Invert the selection criteria for --filterExpression
     */
    @Argument(fullName="invertFilterExpression", shortName="invfilter", doc="Invert the selection criteria for --filterExpression", optional=true)
    public boolean invertFilterExpression = false;

    /**
     * Invert the selection criteria for --genotypeFilterExpression
     */
    @Argument(fullName="invertGenotypeFilterExpression", shortName="invG_filter", doc="Invert the selection criteria for --genotypeFilterExpression", optional=true)
    public boolean invertGenotypeFilterExpression = false;

    /**
     * If this argument is provided, set filtered genotypes to no-call (./.).
     */
    @Argument(fullName="setFilteredGtToNocall", optional=true, doc="Set filtered genotypes to no-call")
    private boolean setFilteredGenotypesToNocall = false;

    // JEXL expressions for the filters
    private List<VariantContextUtils.JexlVCMatchExp> filterExps;
    private List<VariantContextUtils.JexlVCMatchExp> genotypeFilterExps;

    public static final String CLUSTERED_SNP_FILTER_NAME = "SnpCluster";
    private ClusteredSnps clusteredSNPs = null;
    private Locatable previousMaskPosition = null;

    // the structures necessary to initialize and maintain a windowed context
    private FiltrationContextWindow variantContextWindow;
    private static final int WINDOW_SIZE = 10;  // 10 variants on either end of the current one
    private List<FiltrationContext> windowInitializer = new ArrayList<>();

    private final List<Allele> diploidNoCallAlleles = Arrays.asList(Allele.NO_CALL, Allele.NO_CALL);

    public static final class FiltrationContext {

        private final ReferenceContext ref;
        private VariantContext vc;

        public FiltrationContext(final ReferenceContext ref, final VariantContext vc) {
            this.ref = ref;
            this.vc = vc;
        }

        public ReferenceContext getReferenceContext() { return ref; }

        public VariantContext getVariantContext() { return vc; }

        public void setVariantContext(final VariantContext newVC) { vc = newVC; }
    }

    private static final class ClusteredSnps {
        private int window = 10;
        private int snpThreshold = 3;

        ClusteredSnps(final int snpThreshold, final int window) {
            this.window = window;
            this.snpThreshold = snpThreshold;
            if ( window < 1 || snpThreshold < 1 ) {
                throw new IllegalArgumentException("Window and threshold values need to be positive values");
            }
        }

        public boolean filter(final FiltrationContextWindow contextWindow) {

            final FiltrationContext[] variants = contextWindow.getWindow(snpThreshold-1, snpThreshold-1);
            for (int i = 0; i < snpThreshold; i++) {
                // ignore positions at the beginning or end of the overall interval (where there aren't enough records)
                if ( variants[i] == null || variants[i+snpThreshold-1] == null ) {
                    continue;
                }

                // note: the documentation tells users we'll blow up if ref calls are present.
                //   if we ever get a windowed rod context that isn't a hack, we can actually allow this...
                if ( !variants[i].getVariantContext().isVariant() ) {
                    throw new UserException.BadInput("The clustered SNPs filter does not work in the presence of non-variant records; see the documentation for more details");
                }

                // find the nth variant
                final Locatable left = GATKVariantContextUtils.getLocation(variants[i].getVariantContext());
                Locatable right = null;
                int snpsSeen = 1;

                int currentIndex = i;
                while ( ++currentIndex < variants.length ) {
                    if ( variants[currentIndex] != null && variants[currentIndex].getVariantContext() != null && variants[currentIndex].getVariantContext().isVariant() ) {
                        if ( ++snpsSeen == snpThreshold ) {
                            right = GATKVariantContextUtils.getLocation(variants[currentIndex].getVariantContext());
                            break;
                        }
                    }
                }

                if ( right != null &&
                        left.getContig().equals(right.getContig()) &&
                        Math.abs(right.getStart() - left.getStart()) <= window ) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A window of variants surrounding the current variant being investigated
     */
    private static final class FiltrationContextWindow {

        /**
         * The variants.
         */
        private final LinkedList<FiltrationContext> window = new LinkedList<>();
        private final int currentContext;

        /**
         * Contructor for a variant context.
         * @param firstVariants  the first set of variants, comprising the right half of the window
         */
        FiltrationContextWindow(final List<VariantFiltration.FiltrationContext> firstVariants) {
            final int windowSize = (firstVariants == null ? 1 : 2 * firstVariants.size() + 1);
            currentContext = (firstVariants == null ? 0 : firstVariants.size());
            window.addAll(firstVariants);
            while ( window.size() < windowSize ) {
                window.addFirst(null);
            }
        }

        /**
         * The context currently being examined.
         * @return The current context.
         */
        public VariantFiltration.FiltrationContext getContext() {
            return window.get(currentContext);
        }

        /**
         * The maximum number of elements that can be requested on either end of the current context.
         * @return max.
         */
        public int maxWindowElements() {
            return currentContext;
        }

        /**
         * The window around the context currently being examined.
         * @param elementsToLeft number of earlier contexts to return ()
         * @param elementsToRight number of later contexts to return   ()
         * @return The current context window.
         */
        public VariantFiltration.FiltrationContext[] getWindow(final int elementsToLeft, final int elementsToRight) {
            if ( elementsToLeft > maxWindowElements() || elementsToRight > maxWindowElements() ) {
                throw new GATKException("Too large a window requested");
            }
            if ( elementsToLeft < 0 || elementsToRight < 0 ) {
                throw new GATKException("Window size cannot be negative");
            }

            final VariantFiltration.FiltrationContext[] array = new VariantFiltration.FiltrationContext[elementsToLeft + elementsToRight + 1];
            final ListIterator<VariantFiltration.FiltrationContext> iter = window.listIterator(currentContext - elementsToLeft);
            for (int i = 0; i < elementsToLeft + elementsToRight + 1; i++) {
                array[i] = iter.next();
            }
            return array;
        }

        /**
         * Move the window along to the next context
         * @param context The new rightmost context
         */
        public void moveWindow(final VariantFiltration.FiltrationContext context) {
            window.removeFirst();
            window.addLast(context);
        }
    }

    /**
     * Prepend inverse phrase to description if --invertFilterExpression
     *
     * @param description the description
     * @return the description with inverse prepended if --invert_filter_expression
     */
    private String possiblyInvertFilterExpression( final String description ){
        return invertFilterExpression ? "Inverse of: " + description : description;
    }

    private void initializeVcfWriter() {

        // setup the header fields
        final Set<VCFHeaderLine> hInfo = new HashSet<>();
        hInfo.addAll(getHeaderForVariants().getMetaDataInSortedOrder());

        if ( clusterWindow > 0 ) {
            hInfo.add(new VCFFilterHeaderLine(CLUSTERED_SNP_FILTER_NAME, "SNPs found in clusters"));
        }

        if ( !genotypeFilterExps.isEmpty() ) {
            hInfo.add(VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_FILTER_KEY));
        }

        try {
            for ( final VariantContextUtils.JexlVCMatchExp exp : filterExps ) {
                hInfo.add(new VCFFilterHeaderLine(exp.name, possiblyInvertFilterExpression(exp.exp.toString())));
            }
            for ( final VariantContextUtils.JexlVCMatchExp exp : genotypeFilterExps ) {
                hInfo.add(new VCFFilterHeaderLine(exp.name, possiblyInvertFilterExpression(exp.exp.toString())));
            }

            if ( mask != null ) {
                if (filterRecordsNotInMask) {
                    hInfo.add(new VCFFilterHeaderLine(maskName, "Doesn't overlap a user-input mask"));
                } else {
                    hInfo.add(new VCFFilterHeaderLine(maskName, "Overlaps a user-input mask"));
                }
            }
        } catch (final IllegalArgumentException e) {
            throw new UserException.BadInput(e.getMessage());
        }

        writer.writeHeader(new VCFHeader(hInfo, getHeaderForVariants().getGenotypeSamples()));
    }

    @Override
    public void onTraversalStart() {
        if ( clusterWindow > 0 ) {
            clusteredSNPs = new ClusteredSnps(clusterSize, clusterWindow);
        }

        if ( maskExtension < 0 ) {
            throw new UserException.BadArgumentValue("maskExtension", "negative values are not allowed");
        }

        if (filterRecordsNotInMask && mask == null) {
            throw new UserException.BadArgumentValue("filterNotInMask", "argument not allowed if mask argument is not provided");
        }
        filterExps = VariantContextUtils.initializeMatchExps(filterNames, filterExpressions);
        genotypeFilterExps = VariantContextUtils.initializeMatchExps(genotypeFilterNames, genotypeFilterExpressions);

        VariantContextUtils.engine.get().setSilent(true);

        initializeVcfWriter();
    }

//    /**
//     *
//     * @param tracker  the meta-data tracker
//     * @param ref      the reference base
//     * @param context  the context for the given locus
//     * @return 1 if the locus was successfully processed, 0 if otherwise
//     */
//    public Integer map(RefMetaDataTracker tracker, ReferenceContext ref, AlignmentContext context) {

    @Override
    public void apply(final VariantContext variant, final ReadsContext readsContext, final ReferenceContext ref, final FeatureContext featureContext) {

        // is there a SNP mask present?
        final boolean hasMask = featureContext.getValues(mask).isEmpty() == filterRecordsNotInMask;
        if ( hasMask ) {
            previousMaskPosition = ref.getInterval();  // multi-base masks will get triggered over all bases of the mask
        }

        final VariantContext vc1;
        if (invalidatePrevious) {
            vc1 = (new VariantContextBuilder(variant)).filters(new HashSet<>()).make();
        } else {
            vc1 = variant;
        }

        // filter based on previous mask position
        final VariantContext vc = addMaskIfCoversVariant(vc1, previousMaskPosition, maskName, maskExtension, false);

        final FiltrationContext varContext = new FiltrationContext(ref, vc);

        // if we're still initializing the context, do so
        if ( windowInitializer != null ) {

            // if this is a mask position, filter previous records
            if ( hasMask ) {
                for ( final FiltrationContext prevVC : windowInitializer ) {
                    prevVC.setVariantContext(addMaskIfCoversVariant(prevVC.getVariantContext(), ref.getInterval(), maskName, maskExtension, true));
                }
            }

            windowInitializer.add(varContext);
            if ( windowInitializer.size() == WINDOW_SIZE ) {
                variantContextWindow = new FiltrationContextWindow(windowInitializer);
                windowInitializer = null;
            }
        } else {

            // if this is a mask position, filter previous records
            if ( hasMask ) {
                for ( final FiltrationContext prevVC : variantContextWindow.getWindow(10, 10) ) {
                    if ( prevVC != null ) {
                        prevVC.setVariantContext(addMaskIfCoversVariant(prevVC.getVariantContext(), ref.getInterval(), maskName, maskExtension, true));
                    }
                }
            }

            variantContextWindow.moveWindow(varContext);
            filter();
        }
    }

    /**
     * Helper function to check if a mask covers the variant location.
     *
     * @param vc variant context
     * @param locatation genome location
     * @param maskName name of the mask
     * @param maskExtension bases beyond the mask
     * @param vcBeforeLoc if true, variant context is before the genome location; if false, the converse is true.
     * @return true if the genome location is within the extended mask area, false otherwise
     */
    static boolean doesMaskCoverVariant(final VariantContext vc, final Locatable locatation, final String maskName, final int maskExtension, final boolean vcBeforeLoc) {
        final boolean logic = locatation != null &&                                        // have a location
                locatation.getContig().equals(vc.getContig()) &&                        // it's on the same contig
                (vc.getFilters() == null || !vc.getFilters().contains(maskName));   // the filter hasn't already been applied
        if ( logic ) {
            if (vcBeforeLoc) {
                return locatation.getStart() - vc.getEnd() <= maskExtension;  // it's within the mask area (multi-base VCs that overlap this site will always give a negative distance)
            } else {
                return vc.getStart() - locatation.getEnd() <= maskExtension;
            }
        } else {
            return false;
        }
    }

    /**
     * Add mask to variant context filters if it covers the it's location
     *
     * @param vc VariantContext
     * @param location genome location
     * @param maskName name of the mask
     * @param maskExtension bases beyond the mask
     * @param locStart if true, start at genome location and end at VariantContext. If false, do the opposite.
     * @return VariantContext with the mask added if the VariantContext is within the extended mask area
     */
    private static VariantContext addMaskIfCoversVariant(VariantContext vc, final Locatable location, final String maskName, final int maskExtension, final boolean locStart) {
        if (doesMaskCoverVariant(vc, location, maskName, maskExtension, locStart) ) {
            final Set<String> filters = new LinkedHashSet<>(vc.getFilters());
            filters.add(maskName);
            vc = new VariantContextBuilder(vc).filters(filters).make();
        }

        return vc;
    }

    private void filter() {
        // get the current context
        final FiltrationContext context = variantContextWindow.getContext();
        if ( context == null )
            return;

        final VariantContext vc = context.getVariantContext();
        final VariantContextBuilder builder = new VariantContextBuilder(vc);

        // make new Genotypes based on filters
        if ( !genotypeFilterExps.isEmpty() || setFilteredGenotypesToNocall ) {
            final GenotypesContext genotypes = GenotypesContext.create(vc.getGenotypes().size());

            // for each genotype, check filters then create a new object
            for ( final Genotype g : vc.getGenotypes() ) {
                if ( g.isCalled() ) {
                    final List<String> filters = new ArrayList<>();
                    if ( g.isFiltered() ) {
                        filters.add(g.getFilters());
                    }

                    // Add if expression filters the variant context
                    for ( VariantContextUtils.JexlVCMatchExp exp : genotypeFilterExps ) {
                        if ( Utils.invertLogic(VariantContextUtils.match(vc, g, exp), invertGenotypeFilterExpression) ) {
                            filters.add(exp.name);
                        }
                    }

                    // if sample is filtered and --setFilteredGtToNocall, set genotype to non-call
                    if ( !filters.isEmpty() && setFilteredGenotypesToNocall ) {
                        genotypes.add(new GenotypeBuilder(g).filters(filters).alleles(diploidNoCallAlleles).make());
                    } else {
                        genotypes.add(new GenotypeBuilder(g).filters(filters).make());
                    }
                } else {
                    genotypes.add(g);
                }
            }

            builder.genotypes(genotypes);
        }

        // make a new variant context based on filters
        final Set<String> filters = new LinkedHashSet<>(vc.getFilters());

        // test for clustered SNPs if requested
        if ( clusteredSNPs != null && clusteredSNPs.filter(variantContextWindow) ) {
            filters.add(CLUSTERED_SNP_FILTER_NAME);
        }

        for ( final VariantContextUtils.JexlVCMatchExp exp : filterExps ) {
            try {
                if ( Utils.invertLogic(VariantContextUtils.match(vc, exp), invertFilterExpression) ) {
                    filters.add(exp.name);
                }
            } catch (final Exception e) {
                // do nothing unless specifically asked to; it just means that the expression isn't defined for this context
                if ( failMissingValues  ) {
                    filters.add(exp.name);
                }
            }
        }

        if ( filters.isEmpty() ) {
            builder.passFilters();
        } else {
            builder.filters(filters);
        }

        writer.add(builder.make());
    }

    /**
     * Tell the user the number of loci processed and close out the new variants file.
     */
    @Override
    public Object onTraversalDone() {
        // move the window over so that we can filter the last few variants
        if ( windowInitializer != null ) {
            while ( windowInitializer.size() < WINDOW_SIZE ) {
                windowInitializer.add(null);
            }
            variantContextWindow = new FiltrationContextWindow(windowInitializer);
        }
        for (int i=0; i < WINDOW_SIZE; i++) {
            variantContextWindow.moveWindow(null);
            filter();
        }
        return null;
    }

}
