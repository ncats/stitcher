package ix.curation;

import java.util.Map;

/*
 * by virtual of being a clique, all entities returned are 
 * pairwise connected to each other.
 */
public interface Clique extends Component {
    /*
     * return all stitching values that formed this clique. note that if
     * there were one stitching value that participates in all of the 
     * stitching, then this value is the sole designation stitching value
     * for the corresponding stitch key. the value returned can be 
     * singleton or an array of values; use getClass().isArray() to
     * determine which.
     */
    Map<StitchKey, Object> values ();
}
