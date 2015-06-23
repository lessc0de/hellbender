package org.broadinstitute.hellbender.dev.pipelines.bqsr;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.POutput;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.recalibration.QuantizationInfo;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationReport;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationTables;
import org.broadinstitute.hellbender.tools.recalibration.covariates.StandardCovariateList;
import org.broadinstitute.hellbender.utils.dataflow.BucketUtils;
import org.broadinstitute.hellbender.utils.test.BaseTest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;

/**
 * Load a BaseRecalOutput from GCS, at the worker.
 */
public final class BaseRecalOutputSource implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Recalibration report on GCS -> PCollection of a single BaseRecalOutput
     *
     * @param pipeline the pipeline, with authentication information.
     * @param GCSFileName the path to the recalibration report. Must start with "gs://"
     * @return
     */
    static public PCollection<BaseRecalOutput> of(final Pipeline pipeline, String GCSFileName) {
        return pipeline.apply(Create.of(GCSFileName).withName("calibration report name"))
                .apply(ParDo.of(new DoFn<String, BaseRecalOutput>() {
                    @Override
                    public void processElement(ProcessContext c) {
                        final String fname = c.element();
                        File dest = BaseTest.createTempFile("temp-BaseRecal-",".gz");
                        try {
                            BucketUtils.copyFile(fname, c.getPipelineOptions(), dest.getPath());
                        } catch (IOException x) {
                            throw new GATKException("Unable to download recalibration table from '"+fname+"'.", x);
                        }
                        c.output(new BaseRecalOutput(dest));
                    }

        }).named("ingest calibration report"));
    }

}
