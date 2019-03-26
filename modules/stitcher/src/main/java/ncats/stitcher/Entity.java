package ncats.stitcher;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

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

    static public class Triple implements Comparable<Triple>, Serializable {
        static private final long serialVersionUID = 12052017l;
        
        final long source, target;
        final Set<StitchKey> flip = EnumSet.noneOf(StitchKey.class);
        final Map<StitchKey, Object> values = new EnumMap<>(StitchKey.class);
        final transient Node sourceNode, targetNode;
        
        Triple (Node source, Node target) {
            this.source = source.getId();
            this.target = target.getId();
            sourceNode = source;
            targetNode = target;
        }
        
        void add (StitchKey key, Object value, boolean flip) {
            if (key.directed && flip) {
                //logger.info("** FLIPPED: "+key+" "+source.getId()+" "+target.getId());
                this.flip.add(key);
            }
            Object v = values.get(key);
            values.put(key, Util.merge(v, value));
        }
        
        void add (StitchKey key, Object value) {
            add (key, value, false);
        }
        
        public int compareTo (Triple t) {
            int d = t.values.size() - values.size();
            if (d == 0) {
                if (source < t.source) d = -1;
                else if (source > t.source) d = 1;
            }
            return d;
        }

        public long source () { return source; }
        public long source (StitchKey key) {
            if (key.directed)
                return flip.contains(key) ? target : source;
            return source;
        }
        public long target () { return target; }
        public long target (StitchKey key) {
            if (key.directed)
                return flip.contains(key) ? source : target;
            return target;
        }
        public Entity getSource () { return Entity.getEntity(sourceNode); }
        public Entity getTarget () { return Entity.getEntity(targetNode); }

        public boolean contains (StitchKey key) {
            return values.containsKey(key);
        }
        public Map<StitchKey, Object> values () { return values; }
        public int size () { return values.size(); }
    }
    
    static public class Traversal {
        final Set<Long> visited = new HashSet<>();
        final LinkedList<Node> stack = new LinkedList<>();
        final GraphDatabaseService gdb;
        final Entity start;
        final Integer rank;
        
        Traversal (Node node) {
            this (Entity.getEntity(node));
        }
        
        Traversal (Entity start) {
            this.start = start;
            this.gdb = start._node.getGraphDatabase();
            try (Transaction tx = gdb.beginTx()) {
                rank = (Integer)start.parent()._node.getProperty(RANK, null);
                tx.success();
            }
            stack.push(start._node);
        }

        public Entity getStart () { return start; }
        public Integer getRank () { return rank; }
        public Entity[] getPath () {
            return stack.stream()
                .map(n -> Entity.getEntity(n)).toArray(Entity[]::new);
        }
        public int getVisitCount () { return visited.size(); }

        public void __traverse (EntityVisitor visitor) {
            while (!stack.isEmpty()) {
                Node n = stack.pop();
                visited.add(n.getId());
                
                Map<Node, Triple> neighbors = new HashMap<>();
                for (Relationship rel :
                         n.getRelationships(Direction.BOTH, Entity.KEYS)) {
                    Node xn = rel.getOtherNode(n);
                    Triple triple = neighbors.get(xn);
                    if (triple == null) {
                        neighbors.put(xn, triple = new Triple (n, xn));
                    }
                    StitchKey key = StitchKey.valueOf(rel.getType().name());
                    triple.add(key, rel.getProperty(VALUE),
                               rel.getStartNode().getId() != triple.source);
                }
                
                List<Map.Entry<Node, Triple>> ordered =
                    new ArrayList<>(neighbors.entrySet());
                Collections.sort(ordered, (a, b) -> {
                        return a.getValue().compareTo(b.getValue());
                    });

                for (Map.Entry<Node, Triple> me : ordered) {
                    Node xn = me.getKey();
                    if (!visited.contains(xn.getId())) {
                        Triple triple = me.getValue();                  
                        if (!visitor.visit(this, triple)) {
                            return; // stop
                        }
                        stack.push(xn);
                    }
                }
            }
        }

        public void _traverse (EntityVisitor visitor, StitchKey... keys) {
            StringBuilder query = new StringBuilder 
                ("match (n:"+AuxNodeType.ENTITY+")-[");
            if (keys != null && keys.length > 0) {
                query.append(":");
                for (int i = 0; i < keys.length; ++i) {
                    if (i > 0) query.append("|"); // OR'ed
                    query.append("`"+keys[i]+"`");
                }
            }
            query.append("]->(m:"+AuxNodeType.ENTITY+") return distinct n,m");

            Node root = getRoot (start._node);
            try (Result result = gdb.execute(query.toString())) {
                while (result.hasNext()) {
                    Map<String, Object> row = result.next();
                    Node n = (Node)row.get("n");
                    Node m = (Node)row.get("m");
                    if (root.equals(getRoot (n)) || root.equals(getRoot (m))) {
                        Triple triple = new Triple (n, m);
                        for (Relationship rel : n.getRelationships
                                 (Direction.BOTH, KEYS)) {
                            Node xn = rel.getOtherNode(n);
                            if (xn.equals(m)) {
                                StitchKey key = 
                                    StitchKey.valueOf(rel.getType().name());
                                triple.add(key, rel.getProperty(VALUE), 
                                           rel.getStartNode().getId() 
                                           != triple.source());
                            }
                        }

                        if (!visitor.visit(this, triple))
                            return;
                    }
                }
            }
        }

        public void traverse (EntityVisitor visitor, StitchKey... keys) {
            try (Transaction tx = gdb.beginTx()) {
                _traverse (visitor, keys);
                tx.success();
            }
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

    public Entity root () {
        try (Transaction tx = gdb.beginTx()) {
            Entity e = new Entity (getRoot (_node));
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

    /*
     * return stitch counts over values spanning all entities
     * in the component for which this entity belongs.
     */
    public void componentStitchValues (StitchValueVisitor visitor, StitchKey... keys) {
        if (keys == null || keys.length == 0)
            keys = KEYS;

        try (Transaction tx = gdb.beginTx()) {
            Node root = getRoot (_node);
            Relationship rel = root.getSingleRelationship
                (AuxRelType.SUMMARY, Direction.INCOMING);
            
            if (rel != null) {
                RelationshipIndex relidx = _relationshipIndex (root);
                Node stats = rel.getOtherNode(root);

                for (Relationship r : stats.getRelationships(keys)) {
                    Object value = r.getProperty(VALUE, null);
                    if (value != null) {
                        StitchKey key = StitchKey.valueOf(r.getType().name());
                        IndexHits<Relationship> hits = 
                            relidx.get(key.name(), value);
                        visitor.visit(key, value, hits.size());
                        hits.close();
                    }
                }
            }
            else {
                logger.warning("Root node "+root.getId()
                               +" has no SUMMARY relationship!");
            }
            
            tx.success();
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

    protected Entity[] _neighbors (Direction dir, RelationshipType... keys) {
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

    public Entity[] neighbors (RelationshipType... keys) {
        try (Transaction tx = gdb.beginTx()) {
            Entity[] nb = _neighbors (Direction.BOTH, keys);
            tx.success();
            return nb;
        }
    }

    public Stitch getStitch(int ver) {
        try (Transaction tx = gdb.beginTx()) {
            Stitch s = null;
            for (Relationship rel : _node.getRelationships(Direction.BOTH, AuxRelType.PAYLOAD)) {
                if (datasource().getKey().equals(rel.getProperty(SOURCE))) {
                    Node n = rel.getOtherNode(_node);
                    for (Relationship sr: n.getRelationships(Direction.BOTH, AuxRelType.STITCH)) {
                        Node sn = sr.getOtherNode(n);
                        if (sn.hasLabel(Label.label("stitch_v"+ver)))
                            s = Stitch.getStitch(sn);
                    }
                }
            }
            tx.success();
            return s;
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

    public double similarity (Entity other, StitchKey... keys) {
        Map<StitchKey, Object> values = keys (other);
        if (values.containsKey(R_exactMatch)
            || values.containsKey(R_equivalentClass))
            return 1.;
        
        if (keys == null || keys.length == 0) {
            keys = KEYS;
        }
        
        int a = 0, b = 0, ov = 0;
        for (StitchKey key : keys) {
            Set set = new HashSet ();
            Object value = other.get(key.name());
            if (value != null) {
                if (value.getClass().isArray()) {
                    int len = Array.getLength(value);
                    for (int i = 0; i < len; ++i)
                        set.add(Array.get(value, i));
                }
                else {
                    set.add(value);
                }
            }
            
            value = get (key.name());
            if (value != null) {
                if (value.getClass().isArray()) {
                    int len = Array.getLength(value);
                    for (int i = 0; i < len; ++i) {
                        Object v = Array.get(value, i);
                        if (set.contains(v))
                            ++ov;
                    }
                    a += len;
                }
                else {
                    if (set.contains(value))
                        ++ov;
                    ++a;
                }
            }
            b += set.size();
        }

        double sim = 0.0;
        if (a+b > 0) {
            sim = (double)ov/(a+b-ov);
        }
        return sim;
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

    /*
     * attach abritrary data to this entity. type is the relationship type.
     * props - properties associated with the edge
     * data - data
     */
    public Entity _addIfAbsent (String type,
                                Map<String, Object> relationshipProps,
                                Map<String, Object> data) {
        if (!relationshipProps.containsKey(ID)
            || !relationshipProps.containsKey(SOURCE)) {
            throw new IllegalArgumentException
                ("props must contain "+ID+" and "+SOURCE+" properties!");
        }

        Object id = relationshipProps.get(ID);
        if (id == null)
            throw new IllegalArgumentException (ID+" property can't be null!");

        String source = (String)relationshipProps.get(SOURCE);
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
        for (Map.Entry<String, Object> me : data.entrySet()) {
            node.setProperty(me.getKey(), me.getValue());
        }
        Relationship rel = node.createRelationshipTo(_node, reltype);
        rel.setProperty(SOURCE, source);
        rel.setProperty(ID, id);
        for (Map.Entry<String, Object> me : relationshipProps.entrySet())
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
                        Long l1 = (Long)n1.getProperty(CREATED, 0l);
                        Long l2 = (Long)n2.getProperty(CREATED, 0l);
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
        if (!value.isBlacklist(key)) {
            // first _unstitch this node before we proceed
            _unstitch (key);
            
            if (value == null || value.getValue() == null) {
                if (_node.hasProperty(key.name()))
                    _snapshot (key.name(), null);
            }
            else {
                Object val = value.getValue();
                _snapshot (key.name(), val);
                Map<String, Object> attrs = null;
                if (value.getName() != null) {
                    attrs = new TreeMap<>();
                    attrs.put(NAME, value.getName());
                }
                _stitch (key, val, attrs);
                _node.setProperty(key.name(), val);
            }
        }
        else
            logger.info("Blacklist: "+key+"="+value.getValue());
        
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
        _stitch (key, value, null);
    }
    
    protected void _stitch (StitchKey key, Object value,
                            Map<String, Object> attrs) {
        if (value.getClass().isArray()) {
            int size = Array.getLength(value);
            for (int i = 0; i < size; ++i) {
                try {
                    Object v = Array.get(value, i);
                    stitch (_node, key, v, attrs);
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE,
                            "Can't retrieve array element "+i, ex);
                }
            }
        }
        else {
            stitch (_node, key, value, attrs);
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

        if (!value.isBlacklist(key)) {
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
                    Map<String, Object> attrs = null;
                    if (value.getName() != null) {
                        attrs = new TreeMap<>();
                        attrs.put(NAME, value.getName());
                    }
                    _stitch (key, delta, attrs);
                }
            }
        }
        else
            logger.info("Blacklist: "+key+"="+value.getValue());
        
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
        return stitch (target, key, value, null);
    }
    
    public boolean stitch (Entity target, StitchKey key, Object value,
                           Map<String, Object> attrs) {
        try (Transaction tx = gdb.beginTx()) {
            boolean ok = _stitch (target, key, value, attrs);
            tx.success();
            return ok;
        }
    }

    static Node getStatsNode (Node node) {
        Node root = getRoot (node);
        Relationship rel = root.getSingleRelationship
            (AuxRelType.SUMMARY, Direction.INCOMING);
        return rel != null ? rel.getOtherNode(root) : null;
    }

    static void mergeStatsNodes (Node to, Node from) {
        RelationshipIndex relidx = _relationshipIndex (to);
        for (Relationship rel : from.getRelationships(Direction.BOTH, KEYS)) {
            Object value = rel.getProperty(VALUE, null);
            if (value != null) {
                IndexHits<Relationship> hits =
                    relidx.get(rel.getType().name(), value, to, to);
                Relationship hit = hits.getSingle();
                if (hit == null) {
                    // create relationship to self
                    hit = to.createRelationshipTo(to, rel.getType());
                    hit.setProperty(VALUE, value);
                    relidx.add(hit, rel.getType().name(), value);
                }
                relidx.remove(rel);
                rel.delete();
                hits.close();
            }
        }
    }

    static void updateStatsNode (Node stats, StitchKey key, Object value) {
        RelationshipIndex relidx = _relationshipIndex (stats);
        IndexHits<Relationship> hits = relidx.get(key.name(), value, stats, stats);
        Relationship hit = hits.getSingle();
        if (hit == null) {
            // create relationship to self
            hit = stats.createRelationshipTo(stats, key);
            hit.setProperty(VALUE, value);
            relidx.add(hit, key.name(), value);
        }
    }

    protected void union (Node node, StitchKey key, Object value) {
        union (_node, node, key, value);
    }

    protected static void union (Node _node, Node node,
                                 StitchKey key, Object value) {
        Node stats1 = getStatsNode (_node);
        Node stats2 = getStatsNode (node);
        Node root = union (_node, node);

        Node stats = null;
        if (stats1 != null && stats2 != null) {
            if (!stats1.equals(stats2)) {
                // merge
                Relationship rel1 = stats1.getSingleRelationship
                    (AuxRelType.SUMMARY, Direction.OUTGOING);
                Relationship rel2 = stats2.getSingleRelationship
                    (AuxRelType.SUMMARY, Direction.OUTGOING);
                if (root.equals(rel1.getOtherNode(stats1))) {
                    mergeStatsNodes (stats1, stats2);
                    rel2.delete();
                    stats2.delete();
                    stats = stats1;
                }
                else if (root.equals(rel2.getOtherNode(stats2))) {
                    mergeStatsNodes (stats2, stats1);
                    rel1.delete();
                    stats1.delete();
                    stats = stats2;
                }
                else {
                    // huh?
                }
            }
            else {
                stats = stats1; // or stats2
            }
        }
        else if (stats1 != null) {
            stats = stats1;
        }
        else if (stats2 != null) {
            stats = stats2;
        }
        else {
            stats = root.getGraphDatabase().createNode(AuxNodeType.STATS);
            stats.setProperty(KIND, "StitchValues");
            stats.createRelationshipTo(root, AuxRelType.SUMMARY);
        }

        if (stats != null) {
            updateStatsNode (stats, key, value);
            Relationship rel = stats.getSingleRelationship
                (AuxRelType.SUMMARY, Direction.OUTGOING);
            if (!root.equals(rel.getOtherNode(stats))) {
                rel.delete();
                stats.createRelationshipTo(root, AuxRelType.SUMMARY);
            }
        }
        else {
            logger.log(Level.SEVERE, 
                       "SOMETHING'S ROTTEN WITH THE SUMMARY STATS!");
        }
    }

    /*
     * manually perform the stitch; if either of the nodes is already stitched
     * on the designated key, then the value is append to the existing values.
     */
    public boolean _stitch (Entity target, StitchKey key, Object value) {
        return _stitch (target, key, value, null);
    }
    
    public boolean _stitch (Entity target, StitchKey key, Object value,
                            Map<String, Object> attrs) {
        if (value == null)
            throw new IllegalArgumentException ("Stitch value can't be null!");
        
        for (Relationship rel : _node.getRelationships(key, Direction.BOTH)) {
            Node xn = rel.getOtherNode(_node);
            if (xn.equals(target._node)
                && value.equals(rel.getProperty(VALUE, null))) {
                
                boolean updated = false;
                if (attrs != null && !attrs.isEmpty()) {
                    // find the relationship and update the attributes
                    for (Map.Entry<String, Object> me : attrs.entrySet()) {
                        if (rel.hasProperty(me.getKey())) {
                            Object old = rel.getProperty(me.getKey());
                            rel.setProperty
                                (me.getKey(), Util.merge(old, me.getValue()));
                        }
                        else {
                            rel.setProperty(me.getKey(), me.getValue());
                        }
                    }
                    updated = true;
                }
                
                return updated;
            }
        }
        
        Relationship rel = _node.createRelationshipTo(target._node, key);
        rel.setProperty(CREATED, System.currentTimeMillis());
        rel.setProperty(VALUE, value);
        if (attrs != null) {
            for (Map.Entry<String, Object> a : attrs.entrySet())
                rel.setProperty(a.getKey(), a.getValue());
        }
        
        RelationshipIndex relindx = _relationshipIndex (_node);
        relindx.add(rel, key.name(), value);
        union (target._node, key, value);
        
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
        if (oldVal != null && !_node.hasProperty(key.name()))
            throw new IllegalArgumentException
                ("Entity doesn't have "+key+" property");
        
        Object value = null;
        try { // if this is an entirely new value, then an exception will be thrown
              // org.neo4j.graphdb.NotFoundException: NODE[...] has no property with propertyKey="...".
              // at org.neo4j.kernel.impl.core.NodeProxy.getProperty(NodeProxy.java:470)
            value = _node.getProperty(key.name());
        } catch (NotFoundException nfe) {}

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
        stitch (node, key, value, null);
    }
    
    protected static void stitch (Node node, StitchKey key,
                                  Object value, Map<String, Object> attrs) {
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
                        if (attrs != null) {
                            for (Map.Entry<String, Object> a
                                     : attrs.entrySet()) {
                                rel.setProperty(a.getKey(), a.getValue());
                            }
                        }
                        relindx.add(rel, key.name(), value);
                        union (n, node, key, value);

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

    public Traversal traverse (EntityVisitor visitor, StitchKey... keys) {
        Traversal tr = new Traversal (_node);
        tr.traverse(visitor, keys);
        return tr;
    }

    public Traversal _traverse (EntityVisitor visitor, StitchKey... keys) {
        Traversal tr = new Traversal(_node);
        tr._traverse(visitor, keys);
        return tr;
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
