package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.common.collect.Lists;
import htsjdk.samtools.SAMRecord;
import org.broadinstitute.hellbender.engine.dataflow.GATKTestPipeline;
import org.broadinstitute.hellbender.utils.dataflow.DataflowUtils;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

public final class CountBasesTransformUnitTest {

    @DataProvider(name = "reads")
    public Object[][] reads(){

        return new Object[][]{
                {Lists.newArrayList(ArtificialReadUtils.createRandomRead(100)), 100,},
                {Lists.newArrayList(ArtificialReadUtils.createRandomRead(50), ArtificialReadUtils.createRandomRead(100), ArtificialReadUtils.createRandomRead(1000)), 1150}
        };
    }

    @Test(dataProvider = "reads", groups= {"dataflow"})
    public void countBasesTest(List<MutableGATKRead> reads, long expected){
        Pipeline p = GATKTestPipeline.create();
        DataflowUtils.registerGATKCoders(p);
        PCollection<MutableGATKRead> preads = p.apply(Create.of(reads));
        PTransformSAM<Long> transform = new CountBasesDataflowTransform();
        transform.setHeader(ArtificialReadUtils.createArtificialSamHeader());
        PCollection<Long> presult = preads.apply(transform);

        DataflowAssert.thatSingleton(presult).isEqualTo(expected);

        p.run();

    }

}