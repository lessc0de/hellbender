package org.broadinstitute.hellbender.utils.dataflow;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import org.broadinstitute.hellbender.exceptions.UserException;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;

/**
 * Utilities for dealing with google buckets
 */
public final class BucketUtils {
    public static final String GCS_PREFIX = "gs://";
    public static final String HDFS_PREFIX = "hdfs://";

    private BucketUtils(){} //private so that no one will instantiate this class

    public static boolean isCloudStorageUrl(String path) {
        return path.startsWith(GCS_PREFIX);
    }

    /**
     * Returns true if the given path is a HDFS (Hadoop filesystem) URL.
     */
    public static boolean isHadoopUrl(String path) {
        return path.startsWith(HDFS_PREFIX);
    }

    /**
     * Open a file for reading regardless of whether it's on GCS or local disk.
     *
     * @param path the GCS or local path to read from. If GCS, it must start with "gs://".
     * @param popts the pipeline's options, with authentication information.
     * @return an InputStream that reads from the specified file.

     */
    public static InputStream openFile(String path, PipelineOptions popts) {
        try {
            if (null==path) throw new NullPointerException("path was null, shouldn't be.");
            if (null==popts) throw new NullPointerException("popts was null, shouldn't be.");
            if (BucketUtils.isCloudStorageUrl(path)) {
                GcsPath gcsPath = GcsPath.fromUri(path);
                if (null==gcsPath) throw new NullPointerException("parsing path yielded null, shouldn't have.");
                return Channels.newInputStream(new GcsUtil.GcsUtilFactory().create(popts).open(gcsPath));
            } else {
                return new FileInputStream(path);
            }
        } catch (Exception x) {
            throw new UserException.CouldNotReadInputFile(path, x);
        }
    }

    /**
     * Open a binary file for writing regardless of whether it's on GCS or local disk.
     * For writing to GCS it'll use the application/octet-stream MIME type.
     *
     * @param path the GCS or local path to write to. If GCS, it must start with "gs://".
     * @param popts the pipeline's options, with authentication information.
     * @return an OutputStream that writes to the specified file.
     */
    public static OutputStream createFile(String path, PipelineOptions popts) {
        try {
            if (isCloudStorageUrl(path)) {
                return Channels.newOutputStream(new GcsUtil.GcsUtilFactory().create(popts).create(GcsPath.fromUri(path), "application/octet-stream"));
            } else {
                return new FileOutputStream(path);
            }
        } catch (Exception x) {
            throw new UserException.CouldNotCreateOutputFile(path, x);
        }
    }

    /**
     * Copies a file. Can be used to copy e.g. from GCS to local.
     *
     * @param sourcePath the path to read from. If GCS, it must start with "gs://".
     * @param popts the pipeline's options, with authentication information.
     * @param destPath the path to copy to. If GCS, it must start with "gs://".
     * @throws IOException
     */
    public static void copyFile(String sourcePath, PipelineOptions popts, String destPath) throws IOException {
        try (
            InputStream in = openFile(sourcePath, popts);
            OutputStream fout = createFile(destPath, popts)) {
            final byte[] buf = new byte[1024 * 1024];
            int count;
            while ((count = in.read(buf)) > 0) {
                fout.write(buf, 0, count);
            }
        }
    }

    /**
     * Deletes a file, local or on GCS.
     *
     * @param pathToDelete the path to delete. If GCS, it must start with "gs://".
     * @param popts the pipeline's options, with authentication information.
     */
    public static void deleteFile(String pathToDelete, PipelineOptions popts) throws IOException, GeneralSecurityException {
        if (!BucketUtils.isCloudStorageUrl(pathToDelete)) {
            boolean ok = new File(pathToDelete).delete();
            if (!ok) throw new IOException("Unable to delete '"+pathToDelete+"'");
        }
        GcsPath path = GcsPath.fromUri(pathToDelete);
        GcsOptions gcsOptions = (GcsOptions)popts.as(GcsOptions.class);
        Storage storage = Transport.newStorageClient(gcsOptions).build();
        storage.objects().delete(path.getBucket(), path.getObject()).execute();
    }
}
