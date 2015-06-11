package org.broadinstitute.hellbender.engine.dataflow;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import com.google.common.collect.Lists;
import org.broadinstitute.hellbender.engine.dataflow.datasources.FakeReferenceSource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ArtificialSAMUtils;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToReadAdapter;
import org.broadinstitute.hellbender.utils.read.Read;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;

import java.util.List;
import java.util.UUID;

public class DataflowTestData {
    private final List<KV<Integer, Integer>> readStartLength;

    private final List<Read> reads;
    private final List<KV<ReferenceShard, Iterable<Read>>> kvRefShardiReads;
    private final List<SimpleInterval> readIntervals;
    private final SimpleInterval spannedReadInterval;
    private final List<SimpleInterval> allIntervals;
    private final List<KV<ReferenceBases, Iterable<Read>>> kvRefBasesiReads;
    public DataflowTestData() {
        this.readStartLength = Lists.newArrayList(KV.of(100, 10), KV.of(105, 10), KV.of(299999, 10));

        // TODO Make reads construction more general.
        this.reads = Lists.newArrayList(
                makeRead(readStartLength.get(0), 1),
                makeRead(readStartLength.get(1), 2),
                makeRead(readStartLength.get(2), 3));

        this.kvRefShardiReads =  Lists.newArrayList(
                KV.of(new ReferenceShard(0, "1"), Lists.newArrayList(reads.get(1), reads.get(0))),
                KV.of(new ReferenceShard(2, "1"), Lists.newArrayList(reads.get(2))));

        this.readIntervals = Lists.newArrayList(
                makeInterval(readStartLength.get(0)),
                makeInterval(readStartLength.get(1)),
                makeInterval(readStartLength.get(2)));

        // The first two reads are mapped onto the same reference shard. The ReferenceBases returned should
        // be from the start of the first read [rStartLength.get(0).getKey()] to the end
        // the second [rStartLength.get(1).getKey() + rStartLength.get(1).getValue()-1].
        this.spannedReadInterval =
                new SimpleInterval("1", readStartLength.get(0).getKey(), readStartLength.get(1).getKey() + readStartLength.get(1).getValue()-1);

        this.allIntervals = Lists.newArrayList(readIntervals.iterator());
        allIntervals.add(spannedReadInterval);

        this.kvRefBasesiReads = Lists.newArrayList(
                KV.of(FakeReferenceSource.bases(spannedReadInterval), Lists.newArrayList(reads.get(1), reads.get(0))),
                KV.of(FakeReferenceSource.bases(readIntervals.get(2)), Lists.newArrayList(reads.get(2))));

    }

    public Read makeRead(KV<Integer, Integer> startLength, int i) {
        return makeRead(startLength.getKey(), startLength.getValue(), i);
    }

    public Read makeRead(int start, int length, int i) {
        return new GoogleGenomicsReadToReadAdapter(ReadConverter.makeRead(ArtificialSAMUtils.createRandomRead(start, length)), new UUID(i, i));
    }

    private SimpleInterval makeInterval(KV<Integer, Integer> startLength) {
        return new SimpleInterval("1", startLength.getKey(), startLength.getKey() + startLength.getValue() - 1);
    }

    public final List<KV<Integer, Integer>> getReadStartLength() {
        return readStartLength;
    }

    public List<KV<ReferenceShard, Iterable<Read>>> getKvRefShardiReads() {
        return kvRefShardiReads;
    }

    public List<SimpleInterval> getReadIntervals() {
        return readIntervals;
    }

    public List<SimpleInterval> getAllIntervals() {
        return allIntervals;
    }

    public List<KV<ReferenceBases, Iterable<Read>>> getKvRefBasesiReads() {
        return kvRefBasesiReads;
    }

    public List<Read> getReads() {
        return reads;
    }
}
