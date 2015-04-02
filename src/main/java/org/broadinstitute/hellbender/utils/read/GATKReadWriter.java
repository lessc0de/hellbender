package org.broadinstitute.hellbender.utils.read;

import java.io.Closeable;

public interface GATKReadWriter extends Closeable {
    void addRead(final GATKRead read);
}
