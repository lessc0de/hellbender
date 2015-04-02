package org.broadinstitute.hellbender.transformers;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Classes which perform transformations from MutableRead -> MutableRead should implement this interface by overriding {@link #apply(org.broadinstitute.hellbender.utils.read.MutableGATKRead)
 */
@FunctionalInterface
public interface ReadTransformer extends UnaryOperator<MutableGATKRead>, SerializableFunction<MutableGATKRead, MutableGATKRead>{
    //HACK: These methods are a hack to get to get the type system to accept compositions of ReadTransformers.

    @SuppressWarnings("overloads")
    default ReadTransformer andThen(ReadTransformer after) {
        Objects.requireNonNull(after);
        return (MutableGATKRead r) -> after.apply(apply(r));
    }

    @SuppressWarnings("overloads")
    default ReadTransformer compose(ReadTransformer before) {
        Objects.requireNonNull(before);
        return (MutableGATKRead r) -> apply(before.apply(r));
    }

    static ReadTransformer identity(){
        return read -> read;
    }
}
