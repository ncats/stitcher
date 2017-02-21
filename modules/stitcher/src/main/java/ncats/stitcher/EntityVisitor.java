package ncats.stitcher;

/**
 * Visitor interface for entity traversal; return false in next to stop the
 * traversal.
 */
public interface EntityVisitor {
    boolean next (StitchKey key, Object value);
    // path[path.length-1] == entity
    boolean visit (Entity[] path, Entity entity);
}
