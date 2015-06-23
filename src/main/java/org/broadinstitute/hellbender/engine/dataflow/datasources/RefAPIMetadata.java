package org.broadinstitute.hellbender.engine.dataflow.datasources;

import java.io.Serializable;
import java.util.Map;

public class RefAPIMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    public RefAPIMetadata(String referenceName, Map<String, String> referenceNameToIdTable) {
        this.referenceName = referenceName;
        this.referenceNameToIdTable = referenceNameToIdTable;
    }

    private String referenceName;
    private Map<String, String> referenceNameToIdTable;

    public String getReferenceName() {
        return referenceName;
    }

    public Map<String, String> getReferenceNameToIdTable() {
        return referenceNameToIdTable;
    }
}
