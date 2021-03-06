package org.broadinstitute.hellbender.engine.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.common.base.Strings;
import org.broadinstitute.hellbender.CommandLineProgramTest;


/**
 * A creator of test pipelines for use in tests that can be configured to run using any
 * dataflow runner.
 */
public class GATKTestPipeline {

    static {
        System.setProperty("spark.driver.allowMultipleContexts", "true");
        System.setProperty("spark.ui.enabled", "false");
    }

    /**
     * Creates and returns a new test pipeline.
     *
     * <p>If the <code>dataflowRunner</code> system property is set to the unqualified
     * class name of a <code>PipelineRunner</code> subclass then a new instance of that
     * class will be used as the runner. Otherwise the method delegates to
     * {@link com.google.cloud.dataflow.sdk.testing.TestPipeline#create}, which creates
     * either a local runner or a cloud runner.
     */
    public static Pipeline create() {
        String dataflowRunnerProperty = System.getProperty(CommandLineProgramTest.PROPERTY_DATAFLOW_RUNNER);
        if (!Strings.isNullOrEmpty(dataflowRunnerProperty)) {
            PipelineOptions options = PipelineOptionsFactory.fromArgs(
                new String[] { "--runner=" + dataflowRunnerProperty }).create();
            return Pipeline.create(options);
        } else {
            return com.google.cloud.dataflow.sdk.testing.TestPipeline.create();
        }
    }

}
