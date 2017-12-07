package ncats.stitcher;

public interface StitchValueVisitor {
    void visit (StitchKey key, Object value, Integer count);
}
