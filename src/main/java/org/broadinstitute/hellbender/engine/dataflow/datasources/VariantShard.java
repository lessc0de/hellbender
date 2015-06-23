package org.broadinstitute.hellbender.engine.dataflow.datasources;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import htsjdk.samtools.util.Locatable;

import java.util.ArrayList;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public final class VariantShard {
    private int shardNumber;
    private String contig;
    static public final int VARIANT_SHARDSIZE = 100000;

    private VariantShard() {} // Required for AvroCoder.
    public VariantShard(int shardNumber, String contig) {
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

        VariantShard that = (VariantShard) o;

        return getShardNumber() == that.getShardNumber() && getContig().equals(that.getContig());

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

    /*
      * May have some bugs...
      */
    static public List<VariantShard> getVariantShardsFromInterval(final Locatable location) {
        List<VariantShard> intervalList = new ArrayList<>();
        // Get all of the shard numbers that span the start and end of the interval.
        int startShard = location.getStart()/ VARIANT_SHARDSIZE;
        int endShard = location.getEnd()/ VARIANT_SHARDSIZE;
        for (int i = startShard; i <= endShard; ++i) {
            intervalList.add(new VariantShard(i, location.getContig()));
        }
        return intervalList;
    }

    @Override
    public String toString() {
        return "VariantShard{" +
                "shardNumber=" + shardNumber +
                ", contig='" + contig + '\'' +
                '}';
    }
}
