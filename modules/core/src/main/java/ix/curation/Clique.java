package ix.curation;

import java.util.Map;
import java.util.Set;

public interface Clique {
    /*
     * the clique size (number of nodes/entities)
     */
    int size ();

    /*
     * return the set of node ids
     */
    Set<Long> nodes ();
    
    /*
     * return all stitching values that formed this clique. note that if
     * there were one stitching value that participates in all of the 
     * stitching, then this value is the sole designation stitching value
     * for the corresponding stitch key. the value returned can be 
     * singleton or an array of values; use getClass().isArray() to
     * determine which.
     */
    Map<StitchKey, Object> values ();

    /*
     * return all entities for this clique. by virtual of being a clique,
     * all entities returned are pairwise connected to each other.
     */
    Entity[] entities ();
    
    /*
     * return entities that are overlap between this and the given clique.
     * if there are no overlaps, then null is returned.
     */
    Entity[] entities (Clique clique);
    
    /*
     * determine if this clique has any common nodes to the given clique
     */
    boolean overlaps (Clique clique);
}
