package ncats.stitcher;

import java.util.Map;
import java.util.Arrays;
import java.lang.reflect.Array;

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
    default int weight () {
        Map<StitchKey, Object> values = values ();
        int wt = 0;
        for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
            Object val = me.getValue();
            if (val != null) {
                wt += me.getKey().priority *
                    (val.getClass().isArray() ? Array.getLength(val) : 1);
            }
        }
        return wt*size();
    }

    default boolean subordinate (StitchKey... _keys) {
        StitchKey[] keys = values().keySet().toArray(new StitchKey[0]);
        Arrays.sort(keys, (a, b) -> b.priority - a.priority);
        Arrays.sort(_keys, (a, b) -> b.priority - a.priority);
        return keys[0].priority >= _keys[0].priority;
    }
}
