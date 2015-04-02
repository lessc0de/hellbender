package org.broadinstitute.hellbender.utils.iterators;

import htsjdk.samtools.SAMRecord;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;

import java.util.Iterator;

/**
 * Wraps a SAMRecord iterator within an iterator of MutableReads.
 */
public final class SAMRecordToReadIterator implements Iterator<MutableGATKRead>, Iterable<MutableGATKRead> {
    private Iterator<SAMRecord> samIterator;

    public SAMRecordToReadIterator( final Iterator<SAMRecord> samIterator ) {
        this.samIterator = samIterator;
    }

    @Override
    public boolean hasNext() {
        return samIterator.hasNext();
    }

    @Override
    public MutableGATKRead next() {
        return new SAMRecordToGATKReadAdapter(samIterator.next());
    }

    @Override
    public Iterator<MutableGATKRead> iterator() {
        return this;
    }
}