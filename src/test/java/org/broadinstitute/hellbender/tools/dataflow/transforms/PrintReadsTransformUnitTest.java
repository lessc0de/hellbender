package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import htsjdk.samtools.SAMFileHeader;
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
import java.util.Set;
import java.util.stream.Collectors;

public final class PrintReadsTransformUnitTest {

    @DataProvider(name = "reads")
    public Object[][] reads(){
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader();
        return new Object[][]{
                {Lists.newArrayList(ArtificialReadUtils.createRandomRead(30)), header},
                {Lists.newArrayList(ArtificialReadUtils.createRandomRead(50), ArtificialReadUtils.createRandomRead(100), ArtificialReadUtils.createRandomRead(1000)), header}
        };
    }

    @Test(dataProvider = "reads", groups= {"dataflow"})
    public void printReadsTransformTest(List<MutableGATKRead> reads, SAMFileHeader header){
        Set<String> expected = reads.stream().
                 map(r -> r.convertToSAMRecord(header))
                .map(SAMRecord::getSAMString).collect(Collectors.toSet());

        Pipeline p = GATKTestPipeline.create();
        DataflowUtils.registerGATKCoders(p);
        PCollection<MutableGATKRead> preads = p.apply(Create.of(reads));
        PTransformSAM<String> transform = new PrintReadsDataflowTransform();
        transform.setHeader(ArtificialReadUtils.createArtificialSamHeader());
        PCollection<String> presult = preads.apply(transform);

        DataflowAssert.that(presult);

        p.run();

    }

}