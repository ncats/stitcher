package ncats.stitcher;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import java.lang.reflect.Array;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Entity extends CNode {
    static final Logger logger = Logger.getLogger(Entity.class.getName());
    
    public static StitchKey[] KEYS = EnumSet.allOf(StitchKey.class)
        .toArray(new StitchKey[0]);
    public static EntityType[] TYPES = EnumSet.allOf(EntityType.class)
        .toArray(new EntityType[0]);
    
    public static final Entity[] EMPTY_PATH = new Entity[0];
    static protected class BFS {
        final Entity start;

        BFS (Entity start) {
            this.start = start;
        }

        public Entity[] traverse (Entity end, StitchKey... keys) {
            LinkedList<Node> queue = new LinkedList<>();
            Map<Node, Boolean> visited = new HashMap<>();
            Map<Node, Node> parent = new HashMap<>();
            
            queue.offer(start._node);
            parent.put(start._node, null);
            visited.put(start._node, true);
            
            //System.out.println("+ "+start.getId());
            while (!queue.isEmpty()) {
                Node u = queue.poll();
                for (Relationship rel :
                         u.getRelationships(Direction.BOTH, keys)) {
                    Node v = rel.getOtherNode(u);
                    if (v.equals(end._node)) {
                        parent.put(v, u);
                        break;
                    }
                    else if (!visited.containsKey(v) || !visited.get(v)) {
                        visited.put(v, true);
                        parent.put(v, u);
                        //System.out.println("+ "+v.getId());
                        queue.offer(v);
                    }
                }
            }

            List<Entity> path = new ArrayList<>();
            path.add(end);
            for (Node u = end._node, v; (v = parent.get(u)) != start._node; ) {
                u = v;
                path.add(_getEntity (v));
            }
            path.add(start);
            Collections.reverse(path);
            
            return path.toArray(new Entity[0]);
        }
    }

    protected EnumSet<StitchKey> stitches = EnumSet.noneOf(StitchKey.class);
    
    public static Entity getEntity (Node node) {
        // TODO: caching..
        try (Transaction tx = node.getGraphDatabase().beginTx()) {
            Entity e = new Entity (node);
            tx.success();
            return e;
        }
    }

    public static Entity _getEntity (Node node) {
        return new Entity (node);
    }

    public static String nodeIndexName () {
        return Entity.class.getName()+NODE_INDEX;
    }

    public static String relationshipIndexName () {
        return Entity.class.getName()+RELATIONSHIP_INDEX;
    }
    
    protected Entity (Node node) {
        super (node);
        
        if (!node.hasProperty(RANK) && node.hasLabel(AuxNodeType.ENTITY)) {
            // setup node for connected component
            node.setProperty(RANK, 1);
            node.addLabel(AuxNodeType.COMPONENT);
        }
    }

    public Entity parent () {
        try (Transaction tx = gdb.beginTx()) {
            Entity e = new Entity (_parent ());
            tx.success();
            return e;
        }
    }
    
    public Object get (StitchKey key) {
        try (Transaction tx = gdb.beginTx()) {
            Object ret = _get (key);
            tx.success();
            return ret;
        }
    }

    public Object get (String name) {
        try (Transaction tx = gdb.beginTx()) {
            Object ret =  _get (name);
            tx.success();
            return ret;
        }
    }

    public boolean neighbor (Entity entity) {
        try (Transaction tx = gdb.beginTx()) {
            boolean nb = _neighbor (entity);
            tx.success();
            return nb;
        }
    }

    public boolean _neighbor (Entity entity) {
        for (Relationship rel : _node.getRelationships(Direction.BOTH)) {
            if (rel.getOtherNode(_node).equals(entity._node))
                return true;
        }
        return false;
    }

    public Map<StitchKey, Object> keys () {
        try (Transaction tx = gdb.beginTx()) {
            Map<StitchKey,Object> keys = _keys ();
            tx.success();
            return keys;
        }
    }

    public Map<StitchKey, Object> keys (Entity other) {
        try (Transaction tx = gdb.beginTx()) {
            Map<StitchKey,Object> keys = _keys (other);
            tx.success();
            return keys;
        }
    }

    public Map<StitchKey, Object> _keys () {
        return _keys (null);
    }

    public Map<StitchKey, Object> _keys (Entity other) {
        EnumMap<StitchKey, Object> keys = new EnumMap<>(StitchKey.class);
        for (StitchKey key : KEYS) {
            Object value = null;
            for (Relationship rel
                     : _node.getRelationships(key, Direction.BOTH)) {
                if (other == null
                    || other._node.equals(rel.getOtherNode(_node))) {
                    Object v = rel.getProperty(VALUE);
                    if (v == null);
                    else if (value == null) value = v;
                    else value = Util.merge(value, v);
                }
            }
            
            if (value != null)
                keys.put(key, value);
        }
        return keys;
    }

    public Map<String, Object> _properties () {
        return _node.getAllProperties();
    }

    public Map<String, Object> properties () {
        try (Transaction tx = gdb.beginTx()) {
            Map<String,Object> props = _properties ();
            tx.success();
            return props;
        }
    }
    
    public Entity[] neighbors () {
        return neighbors (Entity.KEYS);
    }

    public Entity[] _neighbors () {
        return _neighbors (Entity.KEYS);
    }

    protected Entity[] _neighbors (Direction dir, StitchKey... keys) {
        Set<Entity> neighbors = new TreeSet<Entity>();
        for (Relationship rel : _node.getRelationships(dir, keys)) {
            Node n = rel.getOtherNode(_node);
            neighbors.add(_getEntity (n));
        }
        return neighbors.toArray(new Entity[0]);        
    }
    
    public Entity[] neighbors (StitchKey... keys) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] nb = _neighbors (Direction.BOTH, keys);
            tx.success();
            return nb;
        }
    }

    public Entity[] _neighbors (StitchKey... keys) {
        return _neighbors (Direction.BOTH, keys);
    }

    public Entity[] _inNeighbors (StitchKey... keys) {
        return _neighbors (Direction.INCOMING, keys);
    }
    
    public Entity[] inNeighbors (StitchKey... keys) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] nb = _inNeighbors (keys);
            tx.success();
            return nb;
        }
    }

    public Entity[] _outNeighbors (StitchKey... keys) {
        return _neighbors (Direction.OUTGOING, keys);   
    }

    public Entity[] outNeighbors (StitchKey... keys) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] nb = _neighbors (Direction.OUTGOING, keys);
            tx.success();
            return nb;
        }
    }

    public Entity[] neighbors (StitchKey key, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] nb = _neighbors (key, value);
            tx.success();
            return nb;
        }
    }
    
    public Entity[] _neighbors (StitchKey key, Object value) {
        Set<Entity> neighbors = new TreeSet<Entity>();
        for (Relationship rel : _node.getRelationships(Direction.BOTH, key)) {
            if (rel.hasProperty(VALUE)) {
                Object val = rel.getProperty(VALUE);
                if (Util.delta(val, value) != Util.NO_CHANGE) {
                    neighbors.add(_getEntity (rel.getOtherNode(_node)));
                }
            }
        }
        return neighbors.toArray(new Entity[0]);
    }

    public boolean contains (StitchKey key, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            boolean ret = _contains (key, value);
            tx.success();
            return ret;
        }
    }

    public boolean _contains (StitchKey key, Object value) {
        Object val = _get (key);
        boolean contains = false;
        if (val != null) {
            contains = Util.NO_CHANGE != Util.delta(val, value);
        }
        return contains;
    }
    
    public Object _get (StitchKey key) {
        return _get (key.name());
    }

    public Object _get (String name) {
        return _node.hasProperty(name) ? _node.getProperty(name) : null;
    }

    public Entity removeAll (String type) {
        try (Transaction tx = gdb.beginTx()) {
            _removeAll (type);
            tx.success();
        }
        return this;
    }

    /* removes relationships of type AND the nodes that they point to */
    public Entity _removeAll (String type) {
        _node.getRelationships(RelationshipType.withName(type)).forEach(rel -> {
            Node node = rel.getOtherNode(_node);
            rel.delete();
            node.delete();
        });
        return this;
    }

    public Entity add (Payload payload) {
        try (Transaction tx = gdb.beginTx()) {
            _add (payload);
            tx.success();
        }
        return this;
    }

    public Entity addIfAbsent (String type, Map<String, Object> props, 
                               Map<String, Object> data) {
        try (Transaction tx = gdb.beginTx()) {
            _addIfAbsent (type, props, data);
            tx.success();
        }
        return this;
    }

    /**
     * attach abritrary data to this entity. type is the relationship type.
     * props - properties associated with the edge
     * data - data
     */
    public Entity _addIfAbsent (String type, Map<String, Object> props, 
                                Map<String, Object> data) {
        if (!props.containsKey(ID) || !props.containsKey(SOURCE)) {
            throw new IllegalArgumentException
                ("props must contain "+ID+" and "+SOURCE+" properties!");
        }

        Object id = props.get(ID);
        if (id == null)
            throw new IllegalArgumentException (ID+" property can't be null!");

        String source = (String)props.get(SOURCE);
        if (source == null)
            throw new IllegalArgumentException
                (SOURCE+" property can't be null!");
        
        RelationshipType reltype = RelationshipType.withName(type);
        for (Relationship rel :
                 _node.getRelationships(Direction.INCOMING, reltype)) {
            if (source.equals(rel.getProperty(SOURCE))
                && Util.equals(id, rel.getProperty(ID)))
                return this;
        }
        
        Node node = gdb.createNode(AuxNodeType.DATA);
        node.setProperty(CREATED, System.currentTimeMillis());
        for (Map.Entry<String, Object> me : data.entrySet())
            node.setProperty(me.getKey(), me.getValue());

        Relationship rel = node.createRelationshipTo(_node, reltype);
        rel.setProperty(SOURCE, source);
        rel.setProperty(ID, id);
        for (Map.Entry<String, Object> me : props.entrySet())
            rel.setProperty(me.getKey(), me.getValue());

        return this;
    }

    public Entity _add (Payload payload) {
        DataSource source = payload.getSource();
        
        Object id = payload.getId();
        if (id == null)
            id = "*";

        Relationship payrel = null;
        for (Relationship rel : _node.getRelationships
                 (Direction.BOTH, AuxRelType.PAYLOAD)) {
            String s = (String)rel.getProperty(SOURCE);
            Object i = rel.getProperty(ID);
            if (source.getKey().equals(s) && id.equals(i)) {
                payrel = rel;
                break;
            }
        }

        Node node;
        if (payrel == null) {
            node = gdb.createNode(AuxNodeType.DATA);
            node.setProperty(CREATED, System.currentTimeMillis());
            payrel = node.createRelationshipTo(_node, AuxRelType.PAYLOAD);
            payrel.setProperty(SOURCE, source.getKey());
            payrel.setProperty(ID, id);
        }
        else {
            node = payrel.getOtherNode(_node);
            Index<Node> index = _nodeIndex ();
            for (Iterator<String> it = node.getPropertyKeys().iterator();
                 it.hasNext(); ) {
                String prop = it.next();
                index.remove(_node, prop, node.getProperty(prop));
                node.removeProperty(prop);
            }
            node.setProperty(UPDATED, System.currentTimeMillis());
        }

        Map<String, Object> data = payload.getData();
        if (data != null) {
            Set<String> indexes = payload.getIndexes();
            if (indexes == null)
                indexes = new HashSet<String>();
            
            for (Map.Entry<String, Object> me : data.entrySet()) {
                Object value = me.getValue();
                if (value != null) {
                    if (indexes.contains(me.getKey()))
                        index (me.getKey(), value);
                    node.setProperty(me.getKey(), value);
                }
            }
        }

        return this;
    }

    /*
     * return the last created payload
     */
    public Object payload (String key) {
        return payload().get(key);
    }
    
    public Map<String, Object> payload () {
        try (Transaction tx = gdb.beginTx()) {
            Map<String, Object> py = _payload ();
            tx.success();
            return py;
        }
    }

    public Object payloadId () {
        return get (ID);
    }
    
    public Map<String, Object> _payload () {
        Map<String, Object> payload = new TreeMap<String, Object>();
        
        final List<Node> nodes = new ArrayList<Node>();
        for (Relationship rel : _node.getRelationships
                 (Direction.BOTH, AuxRelType.PAYLOAD)) {
            nodes.add(rel.getOtherNode(_node));
        }

        if (nodes.isEmpty())
            return payload;

        if (nodes.size() > 1) {
            Collections.sort(nodes, new Comparator<Node>() {
                    public int compare (Node n1, Node n2) {
                        Long l1 = (Long)n1.getProperty(CREATED);
                        Long l2 = (Long)n2.getProperty(CREATED);
                        return (int)(l2 - l1);
                    }
                });
        }
        
        Node latest = nodes.iterator().next();
        for (String key : latest.getPropertyKeys()) {
            payload.put(key, latest.getProperty(key));
        }
        return payload;
    }
    
    protected void index (String name, Object value) {
        /*
         * though we can't access this value via a property, we can
         * still search for it, e.g.,
         *  START n=node:`ncats.stitcher.Entity.node_index`(CompoundCAS='16391-75-6') RETURN n
         * where CompoundCAS is name
         *
         * match (n:`jsonDump2015-11-24.txt.gz`)-[r]-(m:`jsonDump2015-11-24.txt.gz`) where type(r)='I_CAS' AND r._value='39421-75-5' return n,r,m limit 25
         */
        Util.index(_nodeIndex (), _node, name, value);
    }

    public Entity set (StitchKey key, Stitchable value) {
        try (Transaction tx = gdb.beginTx()) {
            _set (key, value);
            tx.success();
        }
        return this;
    }

    public Entity _set (StitchKey key, Stitchable value) {
        // first _unstitch this node before we proceed
        _unstitch (key);

        if (value == null || value.getValue() == null) {
            if (_node.hasProperty(key.name()))
                _snapshot (key.name(), null);
        }
        else {
            Object val = value.getValue();
            _snapshot (key.name(), val);
            _stitch (key, val);
            _node.setProperty(key.name(), val);
        }
        
        return this;
    }

    public void delete (StitchKey key) {
        set (key, null);
    }

    @Override
    public void _delete () {
        for (Relationship rel : _node.getRelationships
                 (Direction.BOTH, KEYS)) {
            StitchKey k = StitchKey.valueOf(rel.getType().name());
            for (String index : gdb.index().relationshipIndexNames())
                gdb.index().forRelationships(index).remove(rel);
            rel.delete();
        }
        super._delete();
    }
    
    public static void _delete (Relationship r) {
        GraphDatabaseService gdb = r.getStartNode().getGraphDatabase(); 
        for (String index : gdb.index().relationshipIndexNames())
            gdb.index().forRelationships(index).remove(r);
        r.delete();
    }

    public void _deprecateProperty(StitchKey key, String valStr) {
        _deprecateProperty(_node, key, valStr);
    }

    public static void _deprecateProperty (Node node,
                                           RelationshipType type,
                                           String valStr) {
        Index<Node> index = _nodeIndex (node);

        // update SNAPSHOT
        RelationshipType rt = DynamicRelationshipType.withName(type.name() + ".SNAPSHOT");
        for (Relationship r: node.getRelationships(rt)) {
            boolean deprecate = false;
            Node on = r.getOtherNode(node);
            Object val = null;
            if (on.hasProperty(type.name())) {
                val = on.getProperty(type.name());
                if (val.getClass().isArray()) {
                    int len = Array.getLength(val);
                    for (int i = len - 1; i > -1; i--) {
                        if (valStr.equals(Array.get(val, i))) {
                            // deprecate this value
                            deprecate = true;
                        }
                    }
                } else {
                    if (valStr.equals(val))
                        deprecate = true;
                }
            }
            if (deprecate) {
                index.remove(on, type.name(), val);
                if (val.getClass().isArray()) {
                    ArrayList<String> newVal = new ArrayList<>();
                    int len = Array.getLength(val);
                    for (int i = 0; i < len; ++i)
                        if (!valStr.equals(Array.get(val, i)))
                            newVal.add(Array.get(val, i).toString());
                    if (newVal.size() > 0) {
                        index.add(on, type.name(), newVal.toArray(new String[0]));
                        on.setProperty(type.name(), newVal.toArray(new String[0]));
                    } else {
                        on.removeProperty(type.name());
                    }
                } else {
                    on.removeProperty(type.name());
                }

                // add deprecated annotation
                String typeDep = type.name() + ".deprecated";
                ArrayList<String> newVal = new ArrayList<>();
                newVal.add(valStr);
                if (on.hasProperty(typeDep)) {
                    index.remove(on, typeDep, on.getProperty(typeDep));
                    if (on.getProperty(typeDep).getClass().isArray()) {
                        int len = Array.getLength(on.getProperty(typeDep));
                        for (int i = 0; i < len; i++) {
                            Object v = Array.get(on.getProperty(typeDep), i);
                            if (!valStr.equals(v))
                                newVal.add(v.toString());
                        }
                    } else if (!valStr.equals(on.getProperty(typeDep))) {
                        newVal.add(on.getProperty(typeDep).toString());
                    }
                }
                index.add(on, typeDep, newVal.toArray(new String[0]));
                on.setProperty(typeDep, newVal.toArray(new String[0]));
            }
        }

        Object old = node.getProperty(type.name());
        index.remove(node, type.name(), old);
        ArrayList<Relationship> rs = new ArrayList<>();
        for (Relationship r: node.getRelationships(type)) {
            if (valStr.equals(r.getProperty("_value"))) {
                rs.add(r);
            }
        }
        
        for (Relationship r: rs) {
            _delete (r);
        }
        
        if (old.getClass().isArray()) {
            ArrayList<String> newVal = new ArrayList<>();
            int len = Array.getLength(old);
            for (int i = 0; i < len; ++i)
                if (!valStr.equals(Array.get(old, i)))
                    newVal.add(Array.get(old, i).toString());
            if (newVal.size() > 0) {
                index.add(node, type.name(), newVal.toArray(new String[0]));
                node.setProperty(type.name(), newVal.toArray(new String[0]));
            } else {
                node.removeProperty(type.name());
            }
        }
        else {
            node.removeProperty(type.name());
        }
    }

    protected void _stitch (StitchKey key, Object value) {
        if (value.getClass().isArray()) {
            int size = Array.getLength(value);
            for (int i = 0; i < size; ++i) {
                try {
                    Object v = Array.get(value, i);
                    stitch (_node, key, v);
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE,
                            "Can't retrieve array element "+i, ex);
                }
            }
        }
        else {
            stitch (_node, key, value);
        }
    }

    protected void _unstitch (StitchKey key) {
        if (_node.hasProperty(key.name())) {    
            for (Relationship rel :
                     _node.getRelationships(Direction.BOTH, key)) {
                for (String index : gdb.index().relationshipIndexNames())
                    gdb.index().forRelationships(index).remove(rel);
                rel.delete();
            }

            Object value = _node.getProperty(key.name());
            Index<Node> index = _nodeIndex ();
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i)
                    index.remove(_node, key.name(), Array.get(value, i));
            }
            else {
                index.remove(_node, key.name(), value);
            }
        }
    }

    public Entity add (StitchKey key, Stitchable value) {
        try (Transaction tx = gdb.beginTx()) {
            _add (key, value);
            tx.success();
        }
        return this;
    }

    public Entity _add (StitchKey key, Stitchable value) {
        if (!_node.hasProperty(key.name())) {
            return _set (key, value);
        }

        Object val = value.getValue();
        if (val != null) { // TODO min linking string length = 3?
            // find diffs and update node prop and stitch
            Object old = _node.getProperty(key.name());
            Object delta = Util.delta(val, old);
            if (delta != null) {
                Object newVal = Util.merge(old, val);           
                if (delta == Util.NO_CHANGE)
                    delta = val;
                _snapshot (key.name(), old, newVal);
                _node.setProperty(key.name(), newVal);
                _stitch (key, delta);
            }
        }
        return this;
    }

    // update but don't stitch
    static protected void _update (Node node, StitchKey key, Object value) {
        Index<Node> index = _nodeIndex (node);
        
        Object oldval = node.getProperty(key.name(), null);
        if (oldval == null) {
            node.setProperty(key.name(), value);
        }
        else {
            Object newval = Util.merge(oldval, value);
            node.setProperty(key.name(), newval);
        }
        
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            for (int i = 0; i < len; ++i)
                index.add(node, key.name(), Array.get(value, i));
        }
        else 
            index.add(node, key.name(), value);
    }
    
    public boolean stitch (Entity target, StitchKey key, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            boolean ok = _stitch (target, key, value);
            tx.success();
            return ok;
        }
    }

    /**
     * manually perform the stitch; if either of the nodes is already stitched
     * on the designated key, then the value is append to the existing values.
     */    
    public boolean _stitch (Entity target, StitchKey key, Object value) {
        if (value == null)
            throw new IllegalArgumentException ("Stitch value can't be null!");
        
        for (Relationship rel : _node.getRelationships(key, Direction.BOTH)) {
            Node xn = rel.getOtherNode(_node);
            if (xn.equals(target._node)
                && value.equals(rel.getProperty(VALUE, null)))
                // already exist relationship and value to target node
                return false;
        }
        
        Relationship rel = _node.createRelationshipTo(target._node, key);
        rel.setProperty(CREATED, System.currentTimeMillis());
        rel.setProperty(VALUE, value);
        
        RelationshipIndex relindx = _relationshipIndex (_node);
        relindx.add(rel, key.name(), value);
        union (_node, target._node);

        // now update node properties
        _update (_node, key, value);
        _update (target._node, key, value);
        
        logger.info(_node.getId()
                    +" <-["+key+":\""+value+"\"]-> "+target._node.getId());
        return true;
    }

    public Entity update (StitchKey key, Object oldVal, Object newVal) {
        if (oldVal == null && newVal == null)
            throw new IllegalArgumentException ("oldVal and newVal are null");
        
        try (Transaction tx = gdb.beginTx()) {
            _update (key, oldVal, newVal);
            tx.success();
        }
        return this;
    }

    public Entity _update (StitchKey key, Object oldVal, Object newVal) {
        if (!_node.hasProperty(key.name())) 
            throw new IllegalArgumentException
                ("Entity doesn't have "+key+" property");
        
        Object value = _node.getProperty(key.name());
        if (newVal == null) {
            // remove from value all elements in oldVal
            Object delta = Util.delta(value, oldVal);
            if (delta != null) {
                if (delta != Util.NO_CHANGE) {
                    _unstitch (key);
                    _snapshot (key.name(), value, delta);
                    _node.setProperty(key.name(), delta);
                    _stitch (key, delta);
                }
            }
            else {
                _unstitch (key);
                _node.removeProperty(key.name());
                if (value.equals(oldVal)) {
                    _snapshot (key.name(), value, null);
                }
                else {
                    logger.warning("None of "+oldVal+" found in "
                                   +key+" for node "+_node.getId());
                }
            }
        }
        else if (oldVal == null) {
            _unstitch (key);
            // append?
            Object merged = Util.merge(value, newVal);
            _snapshot (key.name(), value, merged);
            _node.setProperty(key.name(), merged);
            _stitch (key, merged);
        }
        else {
            _unstitch (key);
            // replace oldVal with newVal for this stitch key       
            Object delta = Util.delta(value, oldVal);
            if (delta != null && delta != Util.NO_CHANGE) {
                delta = Util.merge(delta, newVal);
            }
            else
                delta = newVal;

            _snapshot (key.name(), value, delta);
            _node.setProperty(key.name(), delta);
            _stitch (key, delta);
        }
        
        return this;
    }
    
    protected static void stitch (Node node, StitchKey key, Object value) {
        Index<Node> index = _nodeIndex (node);
        IndexHits<Node> hits = index.get(key.name(), value);
        try {
            long size = hits.size();
            if (size > 0) {
                RelationshipIndex relindx = _relationshipIndex (node);
                for (Node n : hits) {
                    // can't have self-link
                    if (!node.equals(n)) {
                        Relationship rel = node.createRelationshipTo(n, key);
                        rel.setProperty(CREATED, System.currentTimeMillis());
                        rel.setProperty(VALUE, value); 
                        relindx.add(rel, key.name(), value);
                        union (n, node);
                        /*   
                             logger.info(node.getId()
                             +" <-["+key+":\""+value+"\"]-> "+n.getId());*/
                    }
                }
                logger.info(key+":\""+value+"\" => "+size);
            }
        }
        finally {
            hits.close();
        }

        index.add(node, key.name(), value);
    }

    public Entity[] pathTo (Entity end, StitchKey key) {
        return pathTo (end, key, null);
    }
    
    public Entity[] pathTo (Entity end, StitchKey key, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] path = _pathTo (end, key, value);
            tx.success();
            return path;
        }
    }

    /*
     * return all nodes between that are stitched between this node 
     * and end
     */
    public Entity[] _pathTo (Entity end, StitchKey key) {
        return _pathTo (end, key, null);
    }

    /*
     * a path is an array of entities with zero or more elements
     */
    public Entity[] _pathTo (Entity end, StitchKey key, Object value) {
        if (equals (end)) {
            return new Entity[]{this};
        }
        
        TraversalDescription traversal =
            gdb.traversalDescription().breadthFirst();
        // this only yields one path if such a path exists
        traversal = traversal.relationships(key, Direction.BOTH);

        Traverser tr = traversal.traverse(_node);
        for (Iterator<Path> it = tr.iterator(); it.hasNext();) {
            Path p = it.next();
            
            List<Entity> path = new ArrayList<Entity> ();
            Entity tail = null;
            for (Iterator<PropertyContainer> el = p.iterator();el.hasNext();) {
                PropertyContainer c = el.next();
                if (c instanceof Node) {
                    Node n = (Node)c;
                    tail = new Entity (n);
                    path.add(tail);
                    if (n.getId() == end.getId())
                        break;
                }
                else if (value != null && c.hasProperty(VALUE)) {
                     // Relationship
                    if (!value.equals(c.getProperty(VALUE))) {
                        tail = null;
                        break;
                    }
                }
            }
            
            if (tail != null && tail.equals(end)) {
                return path.toArray(new Entity[0]);
            }
        }
        
        return EMPTY_PATH;
    }

    public Entity[] _anyPath (Entity end, StitchKey... keys) {
        BFS bfs = new BFS (this);
        return bfs.traverse(end, keys);
    }

    public Entity[] anyPath (Entity end, StitchKey... keys) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] path = _anyPath (end, keys);
            tx.success();
            return path;
        }
    }

    public void walk (EntityVisitor visitor) {
        try (Transaction tx = gdb.beginTx()) {
            _walk (visitor);
            tx.success();
        }
    }

    public void _walk (EntityVisitor visitor) {
        Set<Long> visited = new HashSet<Long>();
        LinkedList<Entity> path = new LinkedList<Entity>();
        depthFirst (visited, path, _node, visitor);
    }

    static protected void depthFirst (Set<Long> visited,
                                      LinkedList<Entity> path, Node node,
                                      EntityVisitor visitor) {
        Entity entity = _getEntity(node);
        path.push(entity);
        if (visitor.visit
            (Util.reverse(path.toArray(new Entity[0])), entity)) {
            visited.add(node.getId());
            for (Relationship rel : node.getRelationships(Direction.BOTH)) {
                Node xnode = rel.getOtherNode(node);
                if (!visited.contains(xnode.getId())) {
                    RelationshipType type = rel.getType();
                    try {
                        StitchKey key = StitchKey.valueOf(type.name());
                        if (rel.hasProperty(VALUE)
                            && visitor.next(key, rel.getProperty(VALUE))) {
                            depthFirst (visited, path, xnode, visitor);
                        }
                    }
                    catch (Exception ex) {
                        // not a stitch key
                    }
                }
            }
        }
        path.pop();
    }

    public Map<String, Object> payload (Entity entity) {
        try (Transaction tx = gdb.beginTx()) {
            Map<String, Object> py =  _payload (entity);
            tx.success();
            return py;
        }
    }

    public Map<String, Object> _payload (Entity entity) {
        for (Relationship rel : _node.getRelationships(Direction.BOTH)) {
            if (rel.getOtherNode(_node).equals(entity._node)) {
                return rel.getAllProperties();
            }
        }
        return null;
    }

    public void union (Entity ent) {
        union (_node, ent._node);
    }

    /*
        Method to return the String value of a given property
        If there are multiple values for the property, a comma
        delimited String of the values is returned.  If the
        Entity does not contain the property, an empty Optional
        is returned.
     */
    public Optional<String> getStringProp(String prop)
    {
        Optional<String> value = Optional.empty();
        if(this.properties().containsKey(prop))
        {
            //System.out.println(e.getId()+" has key "+s);
            Object objVal = this.properties().get(prop);
            if(objVal instanceof  String)
            {
                value = Optional.of((String)objVal);
            }
            else if(objVal instanceof Object[])
            {
                String[] valueList = (String[])objVal;
                StringBuilder sb = new StringBuilder();
                if(valueList[0] instanceof String)
                {
                    for(String val : valueList)
                    {
                        sb.append(val).append(",");
                    }
                    value = Optional.of(sb.deleteCharAt(sb.length() - 1).toString());

                }
            }
        }
        return value;
    }
    /*
        Method returns a Set of Entities which contains the entity that called it
        as well as all of that entity's neighbors.
     */
    public Set<Entity> getCluster() {
        Set<Entity> cluster = new HashSet<>();
        cluster.add(this);
        Collections.addAll(cluster, this.neighbors());
        return cluster;
    }
    /*
        Given a String (property name), this method will return a Set of String values for the property
        from the calling entity's payload.
     */
    public Set<String> getPayloadValues(String s)
    {
        Set<String> values = new HashSet<>();
        Object obj = this.payload().get(s);
        if(obj!=null)
        {
            if(obj instanceof String[])
            {
                String[] array = (String[]) obj;
                for(String str: array)
                {
                    values.add(str);
                }
            }
            else if(obj instanceof String)
            {
                values.add((String)obj);
            }
        }
        return values;
    }
    /*
        Given a String (property name), this method will return a Set of all the corresponding
        values in the payloads of all entities in the calling entity's cluster.
     */
    public Set<Set<String>> getPayloadValuesInCluster(String s)
    {
        Set<Set<String>> values = new HashSet<>();
        Set<Entity> cluster = this.getCluster();
        for(Entity e : cluster) {
            if (!e.getPayloadValues(s).isEmpty()) {
                values.add(e.getPayloadValues(s));
            }
        }
        return values;
    }
    /*
        Returns the entity of the active moeity of the calling entity, if present.  Works under
        the assumption that an Entity can only have one active moeity.
     */
    public Optional<Entity> _getActiveMoiety()
    {
        Optional<Entity> activeMoeity = Optional.empty();
        for (Relationship rel : _node.getRelationships(Direction.BOTH)) {
            if(rel.getType().name().equals("T_ActiveMoiety"))
            {
                activeMoeity=Optional.ofNullable(new Entity(rel.getEndNode()));
                break;
            }
        }
        return activeMoeity;
    }
}
