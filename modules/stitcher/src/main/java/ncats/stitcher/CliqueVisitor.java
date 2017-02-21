package ncats.stitcher;

/**
 * Visitor interface for clique enumeration; return false to stop
 * the enumeration. A clique has a minimum size of 3.
 */
public interface CliqueVisitor {
    boolean clique (Clique clique);
}
