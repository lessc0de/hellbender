package org.broadinstitute.hellbender.engine;

import org.broadinstitute.hellbender.utils.SimpleInterval;

/**
 * An IntervalWalker is a tool that processes a single interval at a time, with the ability to query
 * optional overlapping sources of reads, reference data, and/or variants/features.
 *
 * The current implementation uses no caching, and so will likely only provide acceptable performance
 * if intervals are spaced sufficiently far apart that few records (reads, etc.) will overlap more than
 * one interval. Performance will likely be poor given a large set of small, nearby intervals, but we
 * will address this issue when porting this traversal to dataflow.
 *
 * IntervalWalker authors must implement the apply() method to process each interval, and may optionally implement
 * onTraversalStart() and/or onTraversalDone(). See the {@link org.broadinstitute.hellbender.tools.examples.ExampleIntervalWalker}
 * tool for an example.
 */
public abstract class IntervalWalker extends GATKTool {

    @Override
    public boolean requiresIntervals() {
        return true;
    }

    /**
     * Customize initialization of the Feature data source for this traversal type to disable query lookahead.
     */
    @Override
    void initializeFeatures() {
        // Disable query lookahead in our FeatureManager for this traversal type. Query lookahead helps
        // when our query intervals are overlapping and gradually increasing in position (as they are
        // with ReadWalkers, typically), but with IntervalWalkers our query intervals are guaranteed
        // to be non-overlapping, since our interval parsing code always merges overlapping intervals.
        features = new FeatureManager(this, 0);
        if ( features.isEmpty() ) {  // No available sources of Features for this tool
            features = null;
        }
    }

    /**
     * Initialize data sources.
     *
     * Marked final so that tool authors don't override it. Tool authors should override onTraversalDone() instead.
     */
    @Override
    protected final void onStartup() {
        // Overridden only to make final so that concrete tool implementations don't override
        super.onStartup();
    }

    @Override
    public void traverse() {
        for ( final SimpleInterval interval : intervalsForTraversal ) {
            apply(interval,
                  new ReadsContext(reads, interval),
                  new ReferenceContext(reference, interval),
                  new FeatureContext(features, interval));
        }
    }

    /**
     * Process an individual interval. Must be implemented by tool authors.
     * In general, tool authors should simply stream their output from apply(), and maintain as little internal state
     * as possible.
     *
     * @param interval Current interval being processed.
     * @param readsContext Reads overlapping the current interval. Will be an empty, but non-null, context object
     *                     if there is no backing source of reads data (in which case all queries on it will return
     *                     an empty array/iterator)
     * @param referenceContext Reference bases spanning the current interval. Will be an empty, but non-null, context object
     *                         if there is no backing source of reference data (in which case all queries on it will return
     *                         an empty array/iterator). Can request extra bases of context around the current interval
     *                         by invoking {@link org.broadinstitute.hellbender.engine.ReferenceContext#setWindow}
     *                         on this object before calling {@link org.broadinstitute.hellbender.engine.ReferenceContext#getBases}
     * @param featureContext Features spanning the current interval. Will be an empty, but non-null, context object
     *                       if there is no backing source of Feature data (in which case all queries on it will return an
     *                       empty List).
     */
    public abstract void apply( SimpleInterval interval, ReadsContext readsContext, ReferenceContext referenceContext, FeatureContext featureContext );

    /**
     * Close data sources.
     *
     * Marked final so that tool authors don't override it. Tool authors should override onTraversalDone() instead.
     */
    @Override
    protected final void onShutdown() {
        // Overridden only to make final so that concrete tool implementations don't override
        super.onShutdown();
    }
}
