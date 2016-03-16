package ix.curation;

import java.util.Set;

/**
 * A (connected) component is a set of linked nodes within a graph
 */
public interface Component {
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
    Set<Long> nodes ();

    /*
     * return all entities for this component.
     */
    Entity[] entities ();
}
