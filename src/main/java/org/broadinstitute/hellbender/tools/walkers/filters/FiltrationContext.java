package org.broadinstitute.hellbender.tools.walkers.filters;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;

public class FiltrationContext {

    private ReferenceContext ref;
    private VariantContext vc;

    public FiltrationContext(ReferenceContext ref, VariantContext vc) {
        this.ref = ref;
        this.vc = vc;
    }

    public ReferenceContext getReferenceContext() { return ref; }

    public VariantContext getVariantContext() { return vc; }

    public void setVariantContext(VariantContext newVC) { vc = newVC; }
}