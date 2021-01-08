package ncats.stitcher;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;
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

    /*
     * return all members of this clique that are maximal support, where
     * it's defined as those entities for which the values associated with
     * the stitch keys are maximal in terms of cardinality. for example,
     * if this clique is span by N_Name with values {value1, value2, value3}
     * and one of the entities in the clique has N_Name as {value1, value2,
     * value3, value4}, then this entity isn't maximal support.
     */
    default Set<Entity> maximalSupport () {
        return maximalSupport (0.5);
    }
    
    default Set<Entity> maximalSupport (double threshold) {
        Set<Entity> maxsup = new HashSet<>();
        Set<Entity> remove = new HashSet<>();
        Map<StitchKey, Object> values = values ();
        for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
            // should we be poluting this with specific strategy here?
            switch (me.getKey()) {
            case H_LyChI_L1:
            case H_LyChI_L2:
            case H_LyChI_L3:
            case H_LyChI_L4:
            case H_LyChI_L5:
                { Predicate notSalt = v -> !v.toString().endsWith("-S");
                    Object[] cvals = Util.filter(me.getValue(), notSalt);
                    for (Entity e : entities ()) {
                        Object[] evals =
                            Util.filter(e.get(me.getKey()), notSalt);
                        if (cvals.length == evals.length)
                            maxsup.add(e);
                        else
                            remove.add(e);
                    }
                }
                break;
            default:
                { int clen = Util.toArray(me.getValue()).length;
                    for (Entity e : entities ()) {
                        Object eval = e.get(me.getKey());
                        // clen  <= eval.length
                        double ratio = (double)clen/Util.toArray(eval).length;
                        if (ratio > threshold) {
                            maxsup.add(e);
                        }
                        else {
                            remove.add(e);
                        }
                    }
                }
            }
        }
        maxsup.removeAll(remove);
        
        return maxsup;
    }

    /*
     * return all members of this clique that have stronger stitches to others
     * than the values(). members that are in others are not returned.
     */
    default Set<Entity> defectors (Set<Entity> others) {
        Map<StitchKey, Object> values = values ();
        Comparator<StitchKey> cmp = (a, b) -> b.priority - a.priority;
        StitchKey[] keys = values.keySet().toArray(new StitchKey[0]);
        Arrays.sort(keys, cmp);
        Object[] vals = Util.toArray(values.get(keys[0]));
        
        Set<Entity> defectors = new TreeSet<>();
        for (Entity e : entities ()) {
            if (!others.contains(e)) {
                for (Entity xe : others) {
                    Map<StitchKey, Object> xv = e.keys(xe);
                    StitchKey[] xk = xv.keySet().toArray(new StitchKey[0]);
                    Arrays.sort(xk, cmp);
                    int dif = xk[0].priority - keys[0].priority;
                    if (dif == 0) {
                        dif = xv.size() - keys.length;
                        if (dif == 0) {
                            Object x = xv.get(keys[0]);
                            dif = Util.toArray(x).length - vals.length;
                        }
                    }
                    System.err.println("%%%% "+Util.toString(values)+" vs "
                                       +Util.toString(xv)+" ===> "+dif+" "
                                       +e.getId()+" "+xe.getId());
                    if (dif > 0) {
                        defectors.add(e);
                        break;
                    }
                }
            }
        }
        return defectors;
    }
        
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

    // infinium on the stitch key
    default boolean inf (StitchKey... _keys) {
        StitchKey[] keys = values().keySet().toArray(new StitchKey[0]);
        Arrays.sort(keys, (a, b) -> b.priority - a.priority);
        Arrays.sort(_keys, (a, b) -> b.priority - a.priority);
        return keys[0].priority >= _keys[0].priority;
    }
}
