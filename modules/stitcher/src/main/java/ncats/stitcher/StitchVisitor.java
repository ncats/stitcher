package ncats.stitcher;

import java.util.Map;

public interface StitchVisitor {
    void visit (Entity source, Entity target, Map<StitchKey, Object> values);
}
