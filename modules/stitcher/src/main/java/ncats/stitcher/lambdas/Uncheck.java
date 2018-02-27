package ncats.stitcher.lambdas;

import ncats.stitcher.Sneak;

import java.util.function.Function;
import java.util.function.Supplier;

public class Uncheck {
    public static <T,R, E extends Throwable> Function<T,R> throwingFunction(ThrowingFunction<T,R,E> f){
        return t -> {
            try {
                return f.apply(t);
            } catch (Throwable e) {
                Sneak.sneakyThrow(e);
            }
            return null; // can't happen but makes compiler happy
        };
    }

    public static <T, E extends Throwable> Supplier<T> throwingSupplier(ThrowingSupplier<T,E> supplier){
        return ()->{
            try {
                return supplier.get();
            }catch(Throwable e){
                Sneak.sneakyThrow(e);
            }
            return null;
        };
    }
}
