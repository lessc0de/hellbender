package org.broadinstitute.hellbender.utils.iterators;

import htsjdk.samtools.util.PeekIterator;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

import java.util.Iterator;
import java.util.NoSuchElementException;


public final class SecondaryOrSupplementaryAlignmentSkippingIterator implements Iterator<MutableGATKRead> {
    private final PeekIterator<MutableGATKRead> iter;

    public SecondaryOrSupplementaryAlignmentSkippingIterator( final Iterator<MutableGATKRead> startingIter ) {
        iter = new PeekIterator<>(startingIter);
        skipAnyNonPrimaryAlignments();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public MutableGATKRead next() {
        if ( ! hasNext() ) {
            throw new NoSuchElementException("No more reads in iterator");
        }

        final MutableGATKRead nextRead = iter.next();
        skipAnyNonPrimaryAlignments();
        return nextRead;
    }

    private void skipAnyNonPrimaryAlignments() {
        while ( iter.hasNext() && (iter.peek().isNonPrimaryAlignment() || iter.peek().isSupplementaryAlignment()) ) {
            iter.next();
        }
    }
}

