package ncats.stitcher;

import java.util.Map;

/**
 * Visitor interface for entity traversal; return false in next to stop the
 * traversal.
 */
public interface EntityVisitor {
    boolean visit (Entity.Traversal traversal, Entity.Triple triple);
}
