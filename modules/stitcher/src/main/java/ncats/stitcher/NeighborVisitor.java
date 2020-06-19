package ncats.stitcher;
import java.util.Map;

public interface NeighborVisitor {
    /*
     * id - unique edge id
     * other - other entity connected to this entity
     * key - stitch key 
     * reverse - true if source (this) <- other (target)
     * props - properties associated with relationship between neighbors
     * return true to continue, otherwise stop
     */
    boolean visit (long id, Entity other, StitchKey key,
                   boolean reverse, Map<String, Object> props);
}
