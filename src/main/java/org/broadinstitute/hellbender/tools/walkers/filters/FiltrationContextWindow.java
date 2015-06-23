package org.broadinstitute.hellbender.tools.walkers.filters;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * A window of variants surrounding the current variant being investigated
 *
 * @author ebanks
 * @version 0.1
 */

public class FiltrationContextWindow {

    /**
     * The variants.
     */
    private LinkedList<FiltrationContext> window = new LinkedList<FiltrationContext>();
    private int currentContext;

    /**
     * Contructor for a variant context.
     * @param firstVariants  the first set of variants, comprising the right half of the window
     */
    public FiltrationContextWindow(List<FiltrationContext> firstVariants) {
        int windowSize = (firstVariants == null ? 1 : 2 * firstVariants.size() + 1);
        currentContext = (firstVariants == null ? 0 : firstVariants.size());
        window.addAll(firstVariants);
        while ( window.size() < windowSize )
            window.addFirst(null);
    }

    /**
     * The context currently being examined.
     * @return The current context.
     */
    public FiltrationContext getContext() {
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
    public FiltrationContext[] getWindow(int elementsToLeft, int elementsToRight) {
        if ( elementsToLeft > maxWindowElements() || elementsToRight > maxWindowElements() )
            throw new ReviewedGATKException("Too large a window requested");
        if ( elementsToLeft < 0 || elementsToRight < 0 )
            throw new ReviewedGATKException("Window size cannot be negative");

        FiltrationContext[] array = new FiltrationContext[elementsToLeft + elementsToRight + 1];
        ListIterator<FiltrationContext> iter = window.listIterator(currentContext - elementsToLeft);
        for (int i = 0; i < elementsToLeft + elementsToRight + 1; i++)
            array[i] = iter.next();
        return array;
    }

    /**
     * Move the window along to the next context
     * @param context The new rightmost context
     */
    public void moveWindow(FiltrationContext context) {
        window.removeFirst();
        window.addLast(context);
    }
}
