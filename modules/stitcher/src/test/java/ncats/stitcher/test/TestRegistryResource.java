package ncats.stitcher.test;

import ncats.stitcher.EntityRegistry;
import ncats.stitcher.lambdas.ThrowingFunction;
import ncats.stitcher.lambdas.ThrowingSupplier;
import ncats.stitcher.lambdas.Uncheck;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestRegistryResource<R extends EntityRegistry> extends ExternalResource{

    private R registry;

    private final Supplier<R> registrySupplier;


    public static <E extends Throwable, R extends EntityRegistry> TestRegistryResource<R> createFromTempFolder(TemporaryFolder tmpDir, ThrowingFunction<File, R, E> registrySupplier){
        return new TestRegistryResource<R>(tmpDir, registrySupplier);
    }

    public <E extends Throwable> TestRegistryResource(TemporaryFolder tmpDir, ThrowingFunction<File, R, E> registrySupplier){
        this.registrySupplier = ()->{
            try {
                return Uncheck.throwingFunction(registrySupplier).apply(tmpDir.newFolder());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
    public <E extends Throwable> TestRegistryResource(ThrowingSupplier<R, E> registrySupplier) {
        this.registrySupplier = Uncheck.throwingSupplier(registrySupplier);
    }
    public TestRegistryResource(Supplier<R> registrySupplier) {
        this.registrySupplier = registrySupplier;
    }

    @Override
    protected void before() throws Throwable {
        registry = registrySupplier.get();
    }

    @Override
    protected void after() {
       registry.shutdown();
    }

    public R getRegistry(){
        return registry;
    }
}
