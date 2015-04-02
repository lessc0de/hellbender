package org.broadinstitute.hellbender.engine.dataflow.coders;


import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GATKReadCoder<T extends GATKRead> extends CustomCoder<T> {

    @Override
    public void encode( T value, OutputStream outStream, Context context ) throws IOException {
        final Boolean isGoogleRead = value.getClass() == GoogleGenomicsReadToGATKReadAdapter.class;
        SerializableCoder.of(Boolean.class).encode(isGoogleRead, outStream, context);

        if ( isGoogleRead ) {
            GoogleGenomicsReadToGATKReadAdapter.CODER.encode(value, outStream, context);
        }
        else {
            SerializableCoder.of(SAMRecordToGATKReadAdapter.class).encode(((SAMRecordToGATKReadAdapter)value), outStream, context);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T decode( InputStream inStream, Context context ) throws IOException {
        final Boolean isGoogleRead = SerializableCoder.of(Boolean.class).decode(inStream, context);

        if ( isGoogleRead ) {
            return (T)GoogleGenomicsReadToGATKReadAdapter.CODER.decode(inStream, context);
        }
        else {
            return (T)SerializableCoder.of(SAMRecordToGATKReadAdapter.class).decode(inStream, context);
        }
    }
}
