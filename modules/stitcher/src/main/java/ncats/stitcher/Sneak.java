package ncats.stitcher;

public class Sneak {
    public static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}
