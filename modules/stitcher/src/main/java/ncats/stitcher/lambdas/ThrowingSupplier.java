package ncats.stitcher.lambdas;

import java.util.function.Supplier;

public interface ThrowingSupplier<T, E extends Throwable> {

    T get() throws E;
}
