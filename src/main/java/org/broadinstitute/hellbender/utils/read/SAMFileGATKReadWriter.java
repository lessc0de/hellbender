package org.broadinstitute.hellbender.utils.read;

import htsjdk.samtools.SAMFileWriter;

public class SAMFileGATKReadWriter implements GATKReadWriter {

    private SAMFileWriter samWriter;

    public SAMFileGATKReadWriter( final SAMFileWriter samWriter ) {
        this.samWriter = samWriter;
    }

    @Override
    public void addRead( GATKRead read ) {
        samWriter.addAlignment(read.convertToSAMRecord(samWriter.getFileHeader()));
    }

    @Override
    public void close() {
        samWriter.close();
    }
}
