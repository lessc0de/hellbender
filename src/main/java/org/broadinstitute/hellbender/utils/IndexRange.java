package org.broadinstitute.hellbender.utils;

import java.util.function.IntConsumer;

/**
 * Represents 0-based integer index range.
 *
 * <p>
 * It represents an integer index range as the pair values:
 * <dl>
 *     <dt>{@link #from}</dt>
 *     <dd>- index of the first element in range (i.e. inclusive).</dd>
 *     <dt>{@link #to}</dt>
 *     <dd>- index of the element following the last element in range (i.e. exclusive).</dd>
 * </dl>
 * </p>
 *
 * <p>
 *     This class is intended to specify a valid index range in arrays or ordered collections.
 * </p>
 *
 * <p>
 *     All instances are constraint so that neither <code>from</code> nor <code>to</code> can
 *     be negative nor <code>from</code> can be larger than <code>to</code>.
 * </p>
 *
 * <p>
 *     You can use {@link #isValidLength(int) isValidFor(length)} to verify that a range instance represents a valid
 *     range for an 0-based indexed object with {@code length} elements.
 * </p>
 */
public final class IndexRange {

    /**
     * First index in the range.
     * <p>
     *     It won't ever be negative nor greater than {@link #to}.
     * </p>
     */
    public final int from;

    /**
     * Index following the last index included in the range.
     *
     * <p>
     *     It won't ever be negative nor less than {@link #from}.
     * </p>
     */
    public final int to;

    /**
     * Creates a new range given its {@code from} and {@code to} indices.
     *
     * @param fromIndex the {@code from} index value.
     * @param toIndex   the {@code to} index value.
     * @throws IllegalArgumentException if {@code fromIndex} is larger than {@code toIndex} or either is
     *                                  negative.
     */
    public IndexRange(final int fromIndex, final int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("the range size cannot be negative");
        }
        if (fromIndex < 0) {
            throw new IllegalArgumentException("the range cannot contain negative indices");
        }
        from = fromIndex;
        to = toIndex;
    }

    /**
     * Checks whether this range is valid for a collection or array of a given size.
     *
     * <p>
     *     It assume that 0 is the first valid index for target indexed object which is true
     *     for Java Arrays and mainstream collections.
     * </p>
     *
     * <p>
     *     If the input length is less than 0, thus incorrect, this method is guaranteed to return
     *     {@code false}. No exception is thrown.
     * </p>
     *
     *
     * @param length the targeted collection or array length.
     * @return {@code true} if this range is valid for that {@code length}, {@code false} otherwise.
     */
    public boolean isValidLength(final int length) {
        return to <= length;
    }

    /**
     * Returns number indexes expanded by this range.
     *
     * @return 0 or greater.
     */
    public int size() {
        return to - from;
    }

    /**
     * Iterate through all indexes in the range in ascending order to be processed by the
     * provided {@link IntConsumer integer consumer} lambda function.
     *
     * <p>
     *     Exceptions thrown by the execution of the index consumer {@code lambda}
     *     will be propagated to the caller immediately thus stopping early and preventing
     *     further indexes to be processed.
     * </p>
     * @param lambda the index consumer lambda.
     * @throws IllegalArgumentException if {@code lambda} is {@code null}.
     * @throws RuntimeException if thrown by {@code lambda} for some index.
     * @throws Error if thrown by {@code lambda} for some index.
     */
    public void forEach(final IntConsumer lambda) {
        if (lambda == null) {
            throw new IllegalArgumentException("the lambda function cannot be null");
        }
        for (int i = from; i < to; i++) {
            lambda.accept(i);
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof IndexRange)) {
            return false;
        } else {
            final IndexRange otherCasted = (IndexRange) other;
            return otherCasted.from == this.from && otherCasted.to == this.to;
        }
    }

    @Override
    public int hashCode() {
        // Inspired on {@link Arrays#hashCode(Object[])}.
        return (( 31 + Integer.hashCode(from) ) * 31 ) + Integer.hashCode(to);
    }

    @Override
    public String toString() {
        return String.format("%d-%d",from,to);
    }
}
