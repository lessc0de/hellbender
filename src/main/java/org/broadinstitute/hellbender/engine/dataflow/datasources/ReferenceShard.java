package org.broadinstitute.hellbender.engine.dataflow.datasources;

import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.values.KV;
import htsjdk.samtools.util.Locatable;

import java.io.Serializable;

public class ReferenceShard implements Serializable {
    public static final DelegateCoder<ReferenceShard, KV<Integer, String>> CODER =
            DelegateCoder.of(
                    KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()),
                    new DelegateCoder.CodingFunction<ReferenceShard, KV<Integer, String>>() {
                        @Override
                        public KV<Integer, String> apply(ReferenceShard ref) throws Exception {
                            return KV.of(ref.getShardNumber(), ref.getContig());
                        }
                    },
                    new DelegateCoder.CodingFunction<KV<Integer, String>, ReferenceShard>() {
                        @Override
                        public ReferenceShard apply(KV<Integer, String> kv) throws Exception {
                            return new ReferenceShard(kv.getKey(), kv.getValue());
                        }
                    }
            );
    private final int shardNumber;
    private final String contig;

    public static int REFERENCE_SHARD_SIZE = 100000;

    public ReferenceShard(int shardNumber, String contig) {
        this.shardNumber = shardNumber;
        if (contig == null) {
            throw new IllegalArgumentException("contig cannot be null");
        }
        this.contig = contig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReferenceShard that = (ReferenceShard) o;

        if (getShardNumber() != that.getShardNumber()) return false;
        return getContig().equals(that.getContig());

    }

    @Override
    public int hashCode() {
        int result = getShardNumber();
        result = 31 * result + getContig().hashCode();
        return result;
    }

    public String getContig() {
        return contig;
    }

    public int getShardNumber() {

        return shardNumber;
    }

    @Override
    public String toString() {
        return "ReferenceShard{" +
                "shardNumber=" + shardNumber +
                ", contig='" + contig + '\'' +
                '}';
    }

    static public ReferenceShard getShardNumberFromInterval(final Locatable location) {
        return new ReferenceShard(location.getStart()/REFERENCE_SHARD_SIZE, location.getContig());
    }

}
