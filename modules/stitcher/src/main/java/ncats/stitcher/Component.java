package ncats.stitcher;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A (connected) component is a set of linked nodes within a graph
 */
public interface Component extends Iterable<Entity> {
    /*
     * a unique id associated with this component
     */
    String getId (); 
    
    /*
     * the component size (number of nodes/entities)
     */
    int size ();

    /*
     * return the set of node ids
     */
    Set<Long> nodeSet ();

    default boolean isEmpty () {
        return nodeSet().isEmpty();
    }
    
    default boolean contains (Entity e) {
        return nodeSet().contains(e.getId());
    }
    
    default boolean contains (long id) {
        return nodeSet().contains(id);
    }

    default Map<String, Integer> labels () {
        Map<String, Integer> labels = new TreeMap<>();
        for (Entity e : this) {
            for (String l : e.labels()) {
                Integer c = labels.get(l);
                labels.put(l, c==null ? 1:(c+1));
            }
        }
        return labels;
    }
    
    /*
     * unique set of values that span the given stitch key
     */
    default Map<Object, Integer> values (StitchKey key) {
        throw new UnsupportedOperationException
            ("values(key) is not supported for this implementation");
    }
    
    default long[] nodes () {
        return Util.toPrimitive(nodeSet().toArray(new Long[0]));
    }

    default long[] nodes (StitchKey key, Object value) {
        throw new UnsupportedOperationException
            ("nodes(key,value) is not supported for this implementation");
    }

    default void cliques (CliqueVisitor visitor, StitchKey... keys) {
        throw new UnsupportedOperationException
            ("cliques() is not supported for this implementation");
    }

    
    default public void cliques (CliqueVisitor visitor,
                         StitchKey key, Object value) {
        cliques (visitor, key, value, Stitchable.ANY);
    }

    default void cliques (CliqueVisitor visitor, StitchKey key, 
                          Object value, int selection) {
        throw new UnsupportedOperationException
            ("cliques() is not supported for this implementation");
    }

    default Map<Object, Integer> stats (StitchKey key) {
        throw new UnsupportedOperationException
            ("stats() is not supported for this implementation");       
    }

    default void stitches (BiConsumer<Entity, Entity> consumer,
                           StitchKey... keys) {
        throw new UnsupportedOperationException
            ("stitches() is not supported for this implementation");
    }

    default void stitches (StitchVisitor visitor, StitchKey... keys) {
        throw new UnsupportedOperationException
            ("stitches() is not supported for this implementation");
    }

    default double similarity (Component other) {
        Set<Long> ns = other.nodeSet();
        int ov = 0;
        for (Long n : nodeSet ()) {
            if (ns.contains(n))
                ++ov;
        }
        return (double)ov/(size() + ns.size() - ov);
    }

    /*
     * root entity of this component
     */
    default Entity root () { return null; }
    
    /*
     * return all entities for this component.
     */
    Entity[] entities ();

    default Entity[] entities (long[] ids) {
        Map<Long, Entity> map = new HashMap<>();
        for (Entity e : entities ())
            map.put(e.getId(), e);
        List<Entity> ents = new ArrayList<>();
        for (int i = 0; i < ids.length; ++i) {
            Entity e = map.get(ids[i]);
            if (e != null)
                ents.add(e);
        }
        return ents.toArray(new Entity[0]);
    }

    /*
     * determine if the given component overlaps with this component
     */
    default boolean overlaps (Component c) {
        Set<Long> nodes = nodeSet ();
        for (Long n : c.nodes())
            if (nodes.contains(n))
                return true;
        return false;
    }

    default void depthFirst (long node, Consumer<long[]> consumer,
                             StitchKey... keys) {
        depthFirst (node, consumer, null, keys);
    }
    
    default void depthFirst (long node, Consumer<long[]> consumer,
                             BiPredicate<Long, StitchValue> predicate,
                             StitchKey... keys) {
        throw new UnsupportedOperationException
            ("depthFirst() is not supported for this implementation!");
    }
    
    /*
     * create a new component that is the xor of the given component
     * and this component
     */    
    default Component xor (Component c) {
        throw new UnsupportedOperationException
            ("xor() is not supported for this implementation");
    }
    
    /*
     * create a new component that is the intersection of the given
     * component and this component
     */
    default Component and (Component c) {
        throw new UnsupportedOperationException
            ("and() is not supported for this implementation");
    }

    /*
     * create a new component that is a union of the given component 
     * and this component
     */
    default <T extends Component> T add (T c) {
        throw new UnsupportedOperationException
            ("add() is not supported for this implementation");
    }

    /*
     * add to this component nodes with specific stitch keys and return
     * a new component; if no nodes with the given stitch keys are matched,
     * then this component is returned.
     */
     default Component add (long[] nodes, StitchKey... keys) {
         return add (nodes, null, keys);
     }
    
     default Component add (long[] nodes,
                            BiPredicate<Long, StitchValue> predicate,
                            StitchKey... keys) {
        throw new UnsupportedOperationException
            ("add() is not supported for this implementation");
    }

    default Component filter (StitchKey key, Object value) {
        throw new UnsupportedOperationException
            ("filter() is not supported for this implementation");
    }
    
    default Stream<Entity> stream () {
        return Stream.of(entities ());
    }
    /*
     * return the score for this component
     */
    default Double score () { return null; }
}
