package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;
import org.broadinstitute.hellbender.engine.dataflow.DataflowTestData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.FakeReferenceSource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceAPISource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public final class RefBasesFromAPITest {

    @DataProvider(name = "bases")
    public Object[][] bases(){
        DataflowTestData testData = new DataflowTestData();

        List<Read> inputs = testData.getReads();

        List<KV<ReferenceShard, Iterable<Read>>> kvs = testData.getKvRefShardiReads();

        List<SimpleInterval> intervals = testData.getAllIntervals();

        List<KV<ReferenceBases, Iterable<Read>>> expected = testData.getKvRefBasesiReads();

        return new Object[][]{
                {inputs, kvs, expected, intervals},
        };
    }

    public <T> PCollection<T> PCollectionCreateAndVerify(Pipeline p, Iterable<T> list) {
        Iterable<T> copy = Lists.newArrayList(list.iterator());
        Assert.assertEquals(list, copy);
        PCollection<T> pCollection = p.apply(Create.of(list));
        DataflowAssert.that(pCollection).containsInAnyOrder(copy);
        return pCollection;
    }
    
    @Test(dataProvider = "bases")
    public void refBasesTest(List<Read> reads, List<KV<ReferenceShard, Iterable<Read>>> kvs, List<KV<ReferenceBases, Iterable<Read>>> expected,
                             List<SimpleInterval> intervals) {
        Pipeline p = TestPipeline.create();

        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);
        p.getCoderRegistry().registerCoder(Read.class, GoogleGenomicsReadToReadAdapter.CODER);
        p.getCoderRegistry().registerCoder(GoogleGenomicsReadToReadAdapter.class, GoogleGenomicsReadToReadAdapter.CODER);

        ReferenceAPISource mockSource = mock(ReferenceAPISource.class, withSettings().serializable());
        for (SimpleInterval interval : intervals) {
            when(mockSource.getReferenceBases(interval)).thenReturn(FakeReferenceSource.bases(interval));

        }

        List<Read> reads2 = Lists.newArrayList(reads.iterator());
        Assert.assertEquals(reads, reads2);
        PCollection<Read> pReads = p.apply(Create.of(reads));
        DataflowAssert.that(pReads).containsInAnyOrder(reads2);

        // We have to use GroupReads because if we try to create a pCollection that contains an ArrayList, we get a
        // coder error.
        PCollection<KV<ReferenceShard, Iterable<Read>>> pKvs = pReads.apply(new GroupReadsForRef());
        DataflowAssert.that(pKvs).containsInAnyOrder(kvs);

        PCollection<KV<ReferenceBases, Iterable<Read>>> kvpCollection = RefBasesFromAPI.GetBases(pKvs, mockSource);
        DataflowAssert.that(kvpCollection).containsInAnyOrder(expected);

        p.run();
    }
}
