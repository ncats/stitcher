package ncats.stitcher.domain;

import org.neo4j.graphdb.Transaction;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Created by katzelda on 2/22/17.
 */
public class TransactionStream{

    public static <T> Stream<T> wrap(Transaction tx, Stream<T> stream){
        Objects.requireNonNull(tx);
        return stream.onClose(() -> tx.close());
    }

    public static <T> Stream<T> create(Transaction tx, Supplier<Stream<T>> streamSupplier){
        return wrap(tx, streamSupplier.get());
    }
}
